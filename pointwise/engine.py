"""执行引擎: 装配拓扑、管理生命周期、分析瓶颈、恢复检查点并核对输出。"""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from .channels import BoundedChannel
from .checkpoint import CheckpointStore
from .cancellation import CancelToken
from .operators import (
    BlockOperatorWorker,
    DispatcherWorker,
    OpStats,
    SinkWorker,
    SourceWorker,
)
from .topology import (
    CHANNEL_CONSUMER,
    CHANNEL_PRODUCER,
    NODE_LABELS,
    topology,
    verify_indices,
)

DEFAULT_RUN_DIR = os.environ.get("POINTWISE_RUN_DIR", "/workspace/.runtime")


class InvalidStateError(RuntimeError):
    pass


@dataclass
class EngineConfig:
    # 受控输入索引: 显式列表优先; 否则用 [start, start+count)
    start: int = 0
    count: int = 36
    indices: Optional[List[int]] = None
    routing: str = "parity"  # parity | round_robin

    slow_delay_ms: int = 420
    fast_delay_ms: int = 20
    inject_interval_ms: int = 30

    cap_src: int = 4
    cap_branch: int = 2
    cap_merged: int = 3

    checkpoint_every: int = 4
    # 仅故障子进程自检使用: 第 K 条输出 fsync 后立即 os._exit
    crash_after_outputs: Optional[int] = None

    def planned_indices(self) -> List[int]:
        if self.indices is not None:
            return [int(x) for x in self.indices]
        return list(range(int(self.start), int(self.start) + int(self.count)))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["planned_preview"] = self.planned_indices()[:200]
        d["planned_count"] = len(self.planned_indices())
        return d

    def update(self, patch: Dict[str, Any]) -> List[str]:
        """按白名单校验并合并 patch, 返回实际变更的字段。"""
        changed: List[str] = []
        int_ranges = {
            "start": (0, 10**12),
            "count": (1, 100000),
            "slow_delay_ms": (0, 60000),
            "fast_delay_ms": (0, 60000),
            "inject_interval_ms": (0, 60000),
            "cap_src": (1, 10000),
            "cap_branch": (1, 10000),
            "cap_merged": (1, 10000),
            "checkpoint_every": (1, 10**9),
            "crash_after_outputs": (1, 10**9),
        }
        for key, value in patch.items():
            if key not in self.__dataclass_fields__:  # type: ignore[attr-defined]
                continue
            if key == "indices":
                if value is None:
                    self.indices = None
                    changed.append(key)
                    continue
                if not isinstance(value, list):
                    raise ValueError("indices 必须是整数数组或 null")
                try:
                    idxs = [int(x) for x in value]
                except (TypeError, ValueError):
                    raise ValueError("indices 必须全部是整数")
                if not idxs:
                    raise ValueError("indices 不能为空(或传 null 使用区间模式)")
                if len(idxs) > 100000:
                    raise ValueError("indices 最多 100000 项")
                if len(set(idxs)) != len(idxs):
                    dup = sorted({i for i in idxs if idxs.count(i) > 1})
                    raise ValueError(f"indices 必须唯一, 发现重复: {dup[:10]}")
                self.indices = idxs
                changed.append(key)
                continue
            if key == "routing":
                if value not in ("parity", "round_robin"):
                    raise ValueError("routing 只能是 parity 或 round_robin")
                if self.routing != value:
                    self.routing = value
                    changed.append(key)
                continue
            if key in int_ranges:
                lo, hi = int_ranges[key]
                if value is None and key == "crash_after_outputs":
                    self.crash_after_outputs = None
                    changed.append(key)
                    continue
                try:
                    iv = int(value)
                except (TypeError, ValueError):
                    raise ValueError(f"{key} 必须是整数")
                if not (lo <= iv <= hi):
                    raise ValueError(f"{key} 必须在 [{lo}, {hi}]")
                if getattr(self, key) != iv:
                    setattr(self, key, iv)
                    changed.append(key)
        return changed


class CommitManager:
    """Sink 提交: 输出台账 fsync + 去重集合 + 周期检查点。"""

    def __init__(
        self,
        store: CheckpointStore,
        planned: List[int],
        checkpoint_every: int,
        run_id: str,
        committed_positions: Optional[set] = None,
        checkpoint: Optional[dict] = None,
    ) -> None:
        self.lock = threading.RLock()
        self.store = store
        self.planned = planned
        self.every = max(1, int(checkpoint_every))
        self.run_id = run_id
        self.committed: set = set(committed_positions or set())
        self.checkpoint = checkpoint
        self.checkpoint_saves = 0
        self.last_saved_wall: Optional[float] = None
        self.next_save_at = self.every
        self.new_outputs_this_process = 0
        self.dedup_hits = 0

    def frontier(self) -> int:
        f = 0
        while f in self.committed:
            f += 1
        return f

    def _checkpoint_dict_locked(self, force: bool = False) -> Dict[str, Any]:
        return {
            "version": 1,
            "run_id": self.run_id,
            "updated_wall": time.time(),
            "indices": self.planned,
            "total": len(self.planned),
            "frontier": self.frontier(),
            "committed": sorted(self.committed),
            "checkpoint_every": self.every,
            "saves": self.checkpoint_saves + (1 if force else 0),
        }

    def save_checkpoint(self, force: bool = False) -> Dict[str, Any]:
        with self.lock:
            cp = self._checkpoint_dict_locked(force=force)
            self.store.save_checkpoint(cp)
            self.checkpoint = cp
            self.checkpoint_saves += 1
            self.last_saved_wall = cp["updated_wall"]
            f = cp["frontier"]
            # 下一个保存门槛: 大于当前 frontier 的下一个 every 倍数
            self.next_save_at = (f // self.every + 1) * self.every
            if self.next_save_at <= f:
                self.next_save_at += self.every
            return cp

    def commit(self, record: Dict[str, Any]) -> bool:
        pos = int(record["position"])
        with self.lock:
            if pos in self.committed:
                self.dedup_hits += 1
                return False

            # 崩溃注入: 输出先 durable 落盘(提交点), 随后进程立刻死亡,
            # 连检查点都来不及写 —— 最严酷的恢复情形。
            crash_k = getattr(self, "crash_after_outputs", None)
            self.store.append_output(record)
            self.committed.add(pos)
            self.new_outputs_this_process += 1

            if crash_k is not None and self.new_outputs_this_process >= crash_k:
                os._exit(42)

            f = self.frontier()
            if f >= self.next_save_at:
                self.save_checkpoint(force=True)
            return True


class ExecutionEngine:
    def __init__(self, run_dir: os.PathLike | str = DEFAULT_RUN_DIR) -> None:
        self.run_dir = Path(run_dir)
        self.store = CheckpointStore(self.run_dir)
        self.config = EngineConfig()

        self._lock = threading.RLock()
        self.state = "idle"  # idle|running|cancelling|cancelled|completed|failed
        self.generation = 0
        self.run_id: Optional[str] = None
        self.started_wall: Optional[float] = None
        self.finished_wall: Optional[float] = None

        self.token: Optional[CancelToken] = None
        self.channels: Dict[str, BoundedChannel] = {}
        self.stats: Dict[str, OpStats] = {}
        self.threads: Dict[str, threading.Thread] = {}
        self._supervisor: Optional[threading.Thread] = None
        self.committer: Optional[CommitManager] = None

        self.events: Deque[Dict[str, Any]] = deque(maxlen=150)
        self.recent_outputs: Deque[Dict[str, Any]] = deque(maxlen=60)
        self.last_cancel_report: Optional[Dict[str, Any]] = None
        self.last_verification: Optional[Dict[str, Any]] = None
        self.restore_info: Optional[Dict[str, Any]] = None
        self.last_bottleneck: Optional[Dict[str, Any]] = None

        # 磁盘上既有检查点的概览(服务重启后页面仍可展示/恢复)
        self._persisted_checkpoint: Optional[dict] = self.store.load_checkpoint()
        self._log("info", f"执行引擎就绪, 运行目录 {self.run_dir}")

    # ------------------------------------------------------------------ utils
    def _log(self, level: str, message: str) -> None:
        self.events.append(
            {"wall": time.time(), "mono": time.perf_counter(), "level": level, "message": message}
        )

    def _require_stopped(self) -> None:
        if self.state in ("running", "cancelling"):
            raise InvalidStateError("执行正在进行, 请先取消或等待完成")

    def configure(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._require_stopped()
            changed = self.config.update(patch or {})
            if changed:
                self._log("info", "配置更新: " + ", ".join(changed))
            return self.config.to_dict()

    def reset(self) -> None:
        # 先真正停掉活动运行并等待线程退出, 避免旧 sink 在清盘后继续写
        if self.state in ("running", "cancelling"):
            self.cancel(reason="reset", timeout=5.0)
        with self._lock:
            self.store.reset()
            self._persisted_checkpoint = None
            self.last_verification = None
            self.last_cancel_report = None
            self.restore_info = None
            self.recent_outputs.clear()
            self.committer = None
            self.state = "idle"
            self._log("info", "已重置: 检查点与输出台账已清除")

    # ------------------------------------------------------------- build/start
    def _new_stats(self) -> Dict[str, OpStats]:
        return {
            "source": OpStats("source", "输入源", "source"),
            "dispatcher": OpStats("dispatcher", "分发器", "dispatcher"),
            "op_fast": OpStats("op_fast", "快分支算子", "blocking"),
            "op_slow": OpStats("op_slow", "慢分支算子", "blocking"),
            "sink": OpStats("sink", "幂等 Sink", "sink"),
        }

    def _build_channels(self, cfg: EngineConfig) -> Dict[str, BoundedChannel]:
        return {
            "ch_src": BoundedChannel("ch_src", cfg.cap_src),
            "ch_fast": BoundedChannel("ch_fast", cfg.cap_branch),
            "ch_slow": BoundedChannel("ch_slow", cfg.cap_branch),
            "ch_merged": BoundedChannel("ch_merged", cfg.cap_merged),
        }

    def start(self) -> Dict[str, Any]:
        with self._lock:
            self._require_stopped()
            cfg = self.config
            planned = cfg.planned_indices()
            if not planned:
                raise ValueError("输入索引为空")
            if len(set(planned)) != len(planned):
                raise ValueError("输入索引必须唯一")

            # 全新运行: 清掉旧台账/检查点
            self.store.reset()
            self._persisted_checkpoint = None
            self.last_verification = None
            self.last_cancel_report = None
            self.restore_info = None
            self.recent_outputs.clear()

            self.generation += 1
            gen = self.generation
            self.run_id = f"run-{gen}-{uuid.uuid4().hex[:8]}"
            self.started_wall = time.time()
            self.finished_wall = None
            self.state = "running"

            token = CancelToken()
            self.token = token
            channels = self._build_channels(cfg)
            self.channels = channels
            stats = self._new_stats()
            self.stats = stats

            committer = CommitManager(
                self.store, planned, cfg.checkpoint_every, self.run_id
            )
            committer.crash_after_outputs = cfg.crash_after_outputs
            self.committer = committer
            committer.save_checkpoint(force=True)

            items = list(enumerate(planned))
            threads = self._wire_workers(cfg, planned, items, token, channels, stats, committer)
            self.threads = threads
            for t in threads.values():
                t.start()

            sup = threading.Thread(
                target=self._supervise, args=(gen,), name=f"supervisor-{gen}", daemon=True
            )
            self._supervisor = sup
            sup.start()

            self._log(
                "info",
                f"启动 {self.run_id}: {len(planned)} 个受控输入, "
                f"slow={cfg.slow_delay_ms}ms fast={cfg.fast_delay_ms}ms "
                f"cap(branch)={cfg.cap_branch}",
            )
            return self.snapshot()

    def _wire_workers(
        self, cfg, planned, items, token, channels, stats, committer
    ) -> Dict[str, threading.Thread]:
        event_cb = lambda level, msg: self._log(level, msg)  # noqa: E731

        source = SourceWorker(
            items=items,
            out=channels["ch_src"],
            token=token,
            stats=stats["source"],
            inject_interval_s=cfg.inject_interval_ms / 1000.0,
            on_emit=None,
            on_event=event_cb,
        )
        dispatcher = DispatcherWorker(
            input=channels["ch_src"],
            outputs={"fast": channels["ch_fast"], "slow": channels["ch_slow"]},
            token=token,
            stats=stats["dispatcher"],
            routing=cfg.routing,
            on_event=event_cb,
        )
        op_fast = BlockOperatorWorker(
            op_id="op_fast",
            input=channels["ch_fast"],
            output=channels["ch_merged"],
            token=token,
            stats=stats["op_fast"],
            delay_s=cfg.fast_delay_ms / 1000.0,
            on_event=event_cb,
        )
        op_slow = BlockOperatorWorker(
            op_id="op_slow",
            input=channels["ch_slow"],
            output=channels["ch_merged"],
            token=token,
            stats=stats["op_slow"],
            delay_s=cfg.slow_delay_ms / 1000.0,
            on_event=event_cb,
        )

        def on_output(record: dict, is_new: bool) -> None:
            if is_new:
                self.recent_outputs.appendleft(record)

        sink = SinkWorker(
            input=channels["ch_merged"],
            token=token,
            stats=stats["sink"],
            committer=committer,
            on_output=on_output,
            on_event=event_cb,
        )

        # 取消令牌 -> 立即取消式关闭所有通道, 唤醒全部阻塞 put/get
        for ch in channels.values():
            token.on_cancel(lambda reason, c=ch: c.cancel(reason))

        return {
            "source": threading.Thread(target=source.run, name="source", daemon=True),
            "dispatcher": threading.Thread(target=dispatcher.run, name="dispatcher", daemon=True),
            "op_fast": threading.Thread(target=op_fast.run, name="op_fast", daemon=True),
            "op_slow": threading.Thread(target=op_slow.run, name="op_slow", daemon=True),
            "sink": threading.Thread(target=sink.run, name="sink", daemon=True),
        }

    # --------------------------------------------------------------- restore
    def restore(self) -> Dict[str, Any]:
        with self._lock:
            self._require_stopped()
            cp = self.store.load_checkpoint()
            if cp is None:
                raise InvalidStateError("没有找到 checkpoint.json, 无法恢复")

            planned = [int(x) for x in cp["indices"]]
            records = self.store.read_outputs()
            committed_positions = {
                int(r["position"]) for r in records if "position" in r
            }
            ledger_frontier = 0
            while ledger_frontier in committed_positions:
                ledger_frontier += 1
            cp_frontier = int(cp.get("frontier", 0))

            # 以 durable 台账为最终事实; 正常情况下两者一致
            start_position = ledger_frontier

            # 让 UI 的输入控制与检查点中的受控序列保持一致
            self.config.indices = list(planned)

            self.generation += 1
            gen = self.generation
            self.run_id = str(cp.get("run_id", f"restore-{gen}")) + f"-r{gen}"
            self.started_wall = time.time()
            self.finished_wall = None
            self.state = "running"

            token = CancelToken()
            self.token = token
            channels = self._build_channels(self.config)
            self.channels = channels
            stats = self._new_stats()
            self.stats = stats

            committer = CommitManager(
                self.store,
                planned,
                int(cp.get("checkpoint_every", self.config.checkpoint_every)),
                self.run_id,
                committed_positions=committed_positions,
                checkpoint=cp,
            )
            self.committer = committer

            already_ahead = sorted(p for p in committed_positions if p >= start_position)
            self.restore_info = {
                "checkpoint_frontier": cp_frontier,
                "ledger_frontier": ledger_frontier,
                "replay_from_position": start_position,
                "replay_from_index": planned[start_position]
                if start_position < len(planned)
                else None,
                "already_committed_ahead_positions": already_ahead,
                "already_committed_ahead_count": len(already_ahead),
                "durable_records": len(records),
                "wall": time.time(),
            }
            self.last_verification = None
            self.last_cancel_report = None
            self.recent_outputs.clear()
            for r in records[-60:][::-1]:
                self.recent_outputs.append(r)

            replay_items = list(enumerate(planned))[start_position:]
            threads = self._wire_workers(
                self.config,
                planned,
                replay_items,
                token,
                channels,
                stats,
                committer,
            )
            self.threads = threads
            for t in threads.values():
                t.start()
            sup = threading.Thread(
                target=self._supervise, args=(gen,), name=f"supervisor-{gen}", daemon=True
            )
            self._supervisor = sup
            sup.start()

            self._log(
                "warn",
                f"从检查点恢复: cp_frontier={cp_frontier}, ledger_frontier="
                f"{ledger_frontier}, 重放位置>={start_position}, "
                f"其中 {len(already_ahead)} 项已提交将被幂等去重",
            )
            return self.snapshot()

    # -------------------------------------------------------------- cancel
    def _cancel_locked_signal(self, reason: str) -> Tuple[Optional[CancelToken], Dict[str, threading.Thread]]:
        self.state = "cancelling"
        token = self.token
        threads = dict(self.threads)
        if token is not None:
            token.cancel(reason)  # 同步触发通道 cancel 回调, 唤醒阻塞 worker
        return token, threads

    def cancel(self, reason: str = "user cancel", timeout: float = 5.0) -> Dict[str, Any]:
        with self._lock:
            if self.state not in ("running", "cancelling") or self.token is None:
                return {
                    "ok": True,
                    "cancelled": False,
                    "reason": "not running",
                    "state": self.state,
                    "alive": [],
                    "worker_wake_latency_ms": {},
                    "max_worker_wake_latency_ms": None,
                    "cancel_to_all_stopped_ms": 0.0,
                }
            token, threads = self._cancel_locked_signal(reason)
            supervisor = self._supervisor

        # 不能持锁 join: supervisor 结束时要拿锁做 finalize
        t0 = time.perf_counter()
        deadline = t0 + timeout
        for name, t in threads.items():
            remaining = max(0.0, deadline - time.perf_counter())
            t.join(remaining)
        if supervisor is not None:
            remaining = max(0.0, deadline - time.perf_counter())
            supervisor.join(remaining)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        alive = [name for name, t in threads.items() if t.is_alive()]
        wake = {
            name: self.stats[name].wake_latency_ms
            for name in threads
            if name in self.stats and self.stats[name].wake_latency_ms is not None
        }
        report = {
            "cancel_to_all_stopped_ms": round(elapsed_ms, 1),
            "alive": alive,
            "worker_wake_latency_ms": wake,
            "max_worker_wake_latency_ms": round(max(wake.values()), 2) if wake else None,
            "ok": not alive,
            "reason": reason,
        }
        with self._lock:
            self.last_cancel_report = report
            if not alive and self.state == "cancelling":
                self.state = "cancelled"
                self.finished_wall = time.time()
        self._log(
            "warn" if alive else "info",
            f"取消完成: 全部 worker 停止用时 {elapsed_ms:.1f}ms"
            + (f", 仍存活 {alive}" if alive else ""),
        )
        return report

    # ------------------------------------------------------------ supervise
    def _supervise(self, gen: int) -> None:
        # 有序等待 EOF 传播; 取消时所有通道已被 cancel, join 同样立即返回
        order = ["source", "dispatcher", "op_fast", "op_slow"]
        threads = self.threads
        token = self.token
        for name in order:
            t = threads.get(name)
            if t is not None:
                t.join(10.0)
        try:
            if token is not None and not token.cancelled:
                merged = self.channels.get("ch_merged")
                if merged is not None:
                    merged.close_eof()
            sink_t = threads.get("sink")
            if sink_t is not None:
                sink_t.join(10.0)
        finally:
            self._finalize(gen)

    def _finalize(self, gen: int) -> None:
        with self._lock:
            if gen != self.generation:
                return
            if self.state not in ("running", "cancelling"):
                return
            self.finished_wall = time.time()
            any_failed = any(s.state == "failed" for s in self.stats.values())
            cancelled = self.token is not None and self.token.cancelled

            if cancelled:
                self.state = "cancelled"
                self._log("warn", "执行已取消, 阻塞 worker 均已被唤醒退出")
                return
            if any_failed:
                self.state = "failed"
                self._log("error", "执行失败: 某个算子线程报错")
                return

            # 先落最终检查点并完成核对, 最后才发布 completed,
            # 这样观察到 completed 的调用方一定能读到 verification。
            try:
                if self.committer is not None:
                    cp = self.committer.save_checkpoint(force=True)
                    self._persisted_checkpoint = cp
                result = self.verify_now_locked()
                self._log(
                    "info",
                    f"执行完成并核对: {'PASS' if result['ok'] else 'FAIL'} "
                    f"({result['output_records']}/{result['expected_count']})",
                )
                self.state = "completed"
            except Exception as e:  # pragma: no cover
                self._log("error", f"完成阶段核对失败: {e!r}")
                self.state = "failed"

    # -------------------------------------------------------------- verify
    def _current_planned(self) -> List[int]:
        if self.committer is not None:
            return list(self.committer.planned)
        cp = self.store.load_checkpoint()
        if cp is not None:
            return [int(x) for x in cp["indices"]]
        return self.config.planned_indices()

    def verify_now(self) -> Dict[str, Any]:
        with self._lock:
            return self.verify_now_locked()

    def verify_now_locked(self) -> Dict[str, Any]:
        t0 = time.perf_counter()
        planned = self._current_planned()
        records = self.store.read_outputs()
        result = verify_indices(planned, records)
        result["checked_wall_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        self.last_verification = result
        return result

    # ----------------------------------------------------------- bottleneck
    def analyze_bottleneck(
        self, ch: Dict[str, dict], op: Optional[Dict[str, dict]] = None
    ) -> Optional[Dict[str, Any]]:
        """根据通道指标 + 依赖关系定位导致停顿的通道与根源算子。

        评分优先级: 发送方阻塞 > 通道打满 > 高水位; 若下游消费算子正处于
        processing(可控阻塞延时中), 给予“根因证据”加权; 再按拓扑深度偏向
        更靠近慢算子的通道, 避免把被背压传导顶住的上游通道误判为根因。
        """
        depth = {"ch_src": 1, "ch_fast": 2, "ch_slow": 2, "ch_merged": 3}
        op = op or {}
        candidates = []
        for cid, s in ch.items():
            score = 0.0
            reasons = []
            if s["put_waiters"] > 0:
                score += 10000
                reasons.append(
                    f"{s['put_waiters']} 个发送方阻塞写入已 {s['put_blocked_ms']:.0f}ms"
                )
            if s["full"]:
                score += 5000
                reasons.append(f"通道打满 {s['size']}/{s['capacity']}")
            elif s["fill"] >= 0.8:
                score += 1000
                reasons.append(f"接近满 {s['size']}/{s['capacity']}")
            if score == 0:
                continue

            consumer = CHANNEL_CONSUMER.get(cid)
            consumer_state = (op.get(consumer or "") or {}).get("state")
            if consumer_state == "processing":
                score += 100000  # 下游算子正卡在阻塞处理中: 根因证据
            score += depth.get(cid, 0) * 100
            score += min(s["put_blocked_ms"], 10000) / 100.0
            candidates.append((score, cid, s, reasons))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        _, cid, snap, reasons = candidates[0]

        consumer = CHANNEL_CONSUMER.get(cid)
        producer = CHANNEL_PRODUCER.get(cid)

        # 沿依赖链向上游回溯: 若生产算子的输入通道也在阻塞, 一并纳入链
        chain_channels = [cid]
        chain_nodes = [cid]
        if consumer:
            chain_nodes.append(consumer)

        upstream_trace: List[str] = []
        # producer 是单一算子时, 找它的输入通道
        producer_inputs = {
            "dispatcher": ["ch_src"],
            "op_fast": ["ch_fast"],
            "op_slow": ["ch_slow"],
            "sink": ["ch_merged"],
            "source": [],
        }
        p = producer
        if p in producer_inputs:
            chain_nodes.append(p)
            for in_cid in producer_inputs[p]:
                if in_cid != cid and in_cid in ch:
                    ins = ch[in_cid]
                    if ins["put_waiters"] > 0 or ins["full"] or ins["fill"] >= 0.8:
                        chain_channels.append(in_cid)
                        chain_nodes.append(in_cid)
                        upstream_trace.append(in_cid)
                        pp = CHANNEL_PRODUCER.get(in_cid)
                        if pp and pp not in chain_nodes:
                            chain_nodes.append(pp)

        source_stopped = ch.get("ch_src", {}).get("put_waiters", 0) > 0

        # 严重度
        if snap["put_waiters"] > 0 and snap["put_blocked_ms"] >= 200:
            severity = "critical"
        elif snap["put_waiters"] > 0 or snap["full"]:
            severity = "warning"
        else:
            severity = "notice"

        consumer_label = NODE_LABELS.get(consumer or "", consumer or "?")
        channel_label = NODE_LABELS.get(cid, cid)
        if cid == "ch_slow":
            cause = "慢分支阻塞算子 op_slow 处理延迟过高, 不取走 ch_slow 中的数据"
        elif cid == "ch_fast":
            cause = "快分支算子 op_fast 处理速度跟不上"
        elif cid == "ch_merged":
            cause = "Sink 写出/检查点提交速度跟不上汇合通道"
        elif cid == "ch_src":
            cause = "分发器被下游慢分支顶住, 不再从 ch_src 取数据"
        else:
            cause = "下游消费速度低于上游生产速度"

        propagation = " → ".join(
            NODE_LABELS.get(x, x) for x in [cid] + upstream_trace
        )
        message = (
            f"{channel_label} 停顿: {'; '.join(reasons)}。"
            f"直接原因: {cause}。"
            + (f"背压传导路径: {propagation}" if upstream_trace else "")
            + ("; Source 已阻塞停发。" if source_stopped else "")
        )

        return {
            "channel_id": cid,
            "severity": severity,
            "score": round(candidates[0][0], 1),
            "reasons": reasons,
            "cause": cause,
            "consumer_operator": consumer,
            "producer_operator": producer,
            "chain_channel_ids": chain_channels,
            "chain_node_ids": list(dict.fromkeys(chain_nodes)),
            "source_upstream_stopped": source_stopped,
            "message": message,
        }

    # -------------------------------------------------------------- snapshot
    def _checkpoint_view(self) -> Optional[dict]:
        if self.committer is not None and self.committer.checkpoint is not None:
            cp = dict(self.committer.checkpoint)
            cp["frontier_live"] = self.committer.frontier()
            cp["checkpoint_saves"] = self.committer.checkpoint_saves
            cp["last_saved_wall"] = self.committer.last_saved_wall
            cp["path"] = str(self.store.checkpoint_path)
            return cp
        if self._persisted_checkpoint is not None:
            cp = dict(self._persisted_checkpoint)
            cp["path"] = str(self.store.checkpoint_path)
            cp["persisted_only"] = True
            return cp
        if self.store.checkpoint_path.exists():
            cp = self.store.load_checkpoint()
            self._persisted_checkpoint = cp
            if cp:
                cp["path"] = str(self.store.checkpoint_path)
                cp["persisted_only"] = True
            return cp
        return None

    def index_states(self, planned: List[int]) -> List[Dict[str, Any]]:
        """给页面索引网格用: 每个受控索引的提交状态。"""
        committed: set = set()
        if self.committer is not None:
            with self.committer.lock:
                committed = set(self.committer.committed)
        elif self.state in ("idle",) and self.store.outputs_path.exists():
            committed = {
                int(r["position"])
                for r in self.store.read_outputs()
                if "position" in r
            }
        out = []
        for p, idx in enumerate(planned):
            out.append({"position": p, "idx": idx, "committed": p in committed})
        return out

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            cfg = self.config.to_dict()
            planned = self._current_planned()
            ch_snaps = {cid: ch.snapshot() for cid, ch in self.channels.items()}
            op_snaps = {oid: s.snapshot() for oid, s in self.stats.items()}
            bottleneck = self.analyze_bottleneck(ch_snaps, op_snaps) if ch_snaps else None
            self.last_bottleneck = bottleneck

            alive = {
                name: t.is_alive() for name, t in self.threads.items()
            }

            # 非运行态下, 如果还没有核对结果, 尝试从台账给一个实时核对
            verification = self.last_verification
            if verification is None and self.store.outputs_path.exists():
                try:
                    records = self.store.read_outputs()
                    if records:
                        verification = verify_indices(planned, records)
                except Exception:
                    verification = None

            return {
                "service": "pointwise-executor",
                "state": self.state,
                "generation": self.generation,
                "run_id": self.run_id,
                "server_wall": time.time(),
                "started_wall": self.started_wall,
                "finished_wall": self.finished_wall,
                "uptime_s": round(
                    (self.finished_wall or time.time()) - self.started_wall, 2
                )
                if self.started_wall
                else 0.0,
                "run_dir": str(self.run_dir),
                "config": cfg,
                "planned": planned,
                "planned_count": len(planned),
                "channels": list(ch_snaps.values()),
                "operators": list(op_snaps.values()),
                "threads_alive": alive,
                "bottleneck": bottleneck,
                "checkpoint": self._checkpoint_view(),
                "restore_info": self.restore_info,
                "cancel_report": self.last_cancel_report,
                "verification": verification,
                "events": list(self.events)[-30:][::-1],
                "recent_outputs": list(self.recent_outputs),
                "index_states": self.index_states(planned),
                "topology": topology(),
            }
