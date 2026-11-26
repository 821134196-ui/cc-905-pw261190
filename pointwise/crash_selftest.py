"""硬崩溃 -> 检查点恢复的端到端自检(真实子进程)。

用法:
    python -m pointwise.crash_selftest                # 跑完整场景并打印 JSON
    python -m pointwise.crash_selftest child ...      # 第一个进程: 到点 os._exit(42)
    python -m pointwise.crash_selftest recover ...    # 第二个进程: 从检查点恢复到完成

场景: 子进程跑 24 个受控索引, 第 9 条输出 fsync 后立刻 ``os._exit(42)``
(来不及写检查点); 恢复进程以 outputs.jsonl 台账为事实重放, Sink 幂等去重,
最终独立核对输出不跳项、不重复。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .engine import ExecutionEngine, EngineConfig
from .topology import verify_indices

REPO_ROOT = Path(__file__).resolve().parent.parent


def _wait_terminal(engine: ExecutionEngine, timeout_s: float = 30.0) -> str:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if engine.state in ("completed", "cancelled", "failed"):
            return engine.state
        time.sleep(0.02)
    return "timeout"


def child_main(args: argparse.Namespace) -> int:
    cfg = EngineConfig(
        count=args.count,
        start=0,
        slow_delay_ms=args.slow_delay_ms,
        fast_delay_ms=5,
        inject_interval_ms=4,
        cap_src=4,
        cap_branch=2,
        cap_merged=3,
        checkpoint_every=args.checkpoint_every,
        crash_after_outputs=args.crash_after,
    )
    engine = ExecutionEngine(args.dir)
    engine.config = cfg
    engine.start()
    # 崩溃由 Sink 提交钩子里的 os._exit(42) 完成; 主线程只需保活
    state = _wait_terminal(engine, timeout_s=args.timeout_s)
    sys.stderr.write(f"[child] unexpectedly reached terminal state: {state}\n")
    snap = engine.snapshot()
    sys.stderr.write(json.dumps({"state": state, "verification": snap.get("verification")}) + "\n")
    return 3


def recover_main(args: argparse.Namespace) -> int:
    engine = ExecutionEngine(args.dir)
    try:
        engine.restore()
    except Exception as e:
        print(json.dumps({"ok": False, "stage": "restore", "error": repr(e)}, ensure_ascii=False))
        return 2

    state = _wait_terminal(engine, timeout_s=args.timeout_s)
    # 给 supervisor 一点收尾时间
    for _ in range(50):
        if engine.state == state and engine.state in ("completed", "cancelled", "failed"):
            break
        time.sleep(0.02)

    verification = engine.verify_now()
    cp = engine.store.load_checkpoint()
    records = engine.store.read_outputs()
    planned = cp["indices"] if cp else engine.config.planned_indices()
    independent = verify_indices(planned, records)

    report: Dict[str, Any] = {
        "ok": state == "completed" and independent["ok"],
        "terminal_state": state,
        "restore_info": engine.restore_info,
        "verification": independent,
        "dedup_hits": engine.committer.dedup_hits if engine.committer else None,
        "checkpoint_frontier": (cp or {}).get("frontier") if cp else None,
        "checkpoint_saves": engine.committer.checkpoint_saves if engine.committer else None,
        "outputs_path": str(engine.store.outputs_path),
        "record_count": len(records),
    }
    print("RECOVER_REPORT " + json.dumps(report, ensure_ascii=False))
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["ok"] else 1


def run_hard_crash_scenario(
    workdir: Optional[Path] = None,
    *,
    count: int = 24,
    crash_after: int = 9,
    checkpoint_every: int = 4,
    slow_delay_ms: int = 45,
) -> Dict[str, Any]:
    own_tmp = workdir is None
    if workdir is None:
        workdir = Path(tempfile.mkdtemp(prefix="pointwise-crash-"))
    else:
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )

    common = [
        sys.executable,
        "-m",
        "pointwise.crash_selftest",
    ]
    child_cmd = common + [
        "child",
        "--dir",
        str(workdir),
        "--count",
        str(count),
        "--crash-after",
        str(crash_after),
        "--checkpoint-every",
        str(checkpoint_every),
        "--slow-delay-ms",
        str(slow_delay_ms),
    ]
    t0 = time.time()
    child = subprocess.run(
        child_cmd,
        capture_output=True,
        text=True,
        timeout=45,
        env=env,
        cwd=str(REPO_ROOT),
    )
    child_elapsed = round((time.time() - t0) * 1000)

    recover_cmd = common + ["recover", "--dir", str(workdir)]
    recover = subprocess.run(
        recover_cmd,
        capture_output=True,
        text=True,
        timeout=45,
        env=env,
        cwd=str(REPO_ROOT),
    )

    report: Optional[dict] = None
    for line in recover.stdout.splitlines()[::-1]:
        line = line.strip()
        if line.startswith("{"):
            try:
                report = json.loads(line)
                break
            except json.JSONDecodeError:
                continue

    # 父进程也独立核对一次磁盘台账
    store_records = []
    try:
        from .checkpoint import CheckpointStore

        store = CheckpointStore(workdir)
        cp = store.load_checkpoint()
        store_records = store.read_outputs()
        planned = cp["indices"] if cp else list(range(count))
        disk_verification = verify_indices(planned, store_records)
    except Exception as e:  # pragma: no cover
        disk_verification = {"ok": False, "error": repr(e)}

    passed = (
        child.returncode == 42
        and recover.returncode == 0
        and report is not None
        and report.get("ok") is True
        and disk_verification.get("ok") is True
    )

    result = {
        "passed": passed,
        "workdir": str(workdir),
        "child_exit_code": child.returncode,
        "child_elapsed_ms": child_elapsed,
        "recover_exit_code": recover.returncode,
        "recover_report": report,
        "disk_verification": disk_verification,
        "disk_record_count": len(store_records),
        "child_stderr_tail": child.stderr[-800:],
        "recover_stderr_tail": recover.stderr[-800:],
        "recover_stdout_tail": recover.stdout[-1200:],
    }
    if own_tmp and passed:
        # 保留目录有助于排查; 自检场景由 /tmp 自行清理。这里不主动删除。
        pass
    return result


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="pointwise 硬崩溃恢复自检")
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("child")
    c.add_argument("--dir", required=True)
    c.add_argument("--count", type=int, default=24)
    c.add_argument("--crash-after", type=int, default=9)
    c.add_argument("--checkpoint-every", type=int, default=4)
    c.add_argument("--slow-delay-ms", type=int, default=45)
    c.add_argument("--timeout-s", type=float, default=30.0)
    c.set_defaults(func=child_main)

    r = sub.add_parser("recover")
    r.add_argument("--dir", required=True)
    r.add_argument("--timeout-s", type=float, default=30.0)
    r.set_defaults(func=recover_main)

    s = sub.add_parser("scenario")
    s.add_argument("--dir", default=None)
    s.set_defaults(
        func=lambda a: _scenario_cli(a.dir)
    )
    return p


def _scenario_cli(dir_str: Optional[str]) -> int:
    wd = Path(dir_str) if dir_str else None
    result = run_hard_crash_scenario(wd)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["passed"] else 1


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "cmd", None):
        return _scenario_cli(None)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
