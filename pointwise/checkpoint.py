"""检查点与输出台账的持久化。

两个文件构成恢复依据(均在 ``run_dir`` 下):

* ``checkpoint.json`` —— 原子替换写入的检查点: 受控输入索引序列、连续
  提交前沿 frontier 等。先 fsync 输出台账再原子落检查点, 因此检查点中
  frontier 之前的项一定已在台账中。
* ``outputs.jsonl`` —— Sink 的幂等输出台账, 每条输出 fsync。恢复时以它
  为最终事实重建已提交位置集合; 末尾若有半行(极端崩溃)则忽略。

输出按输入索引(pointwise)去重, 所以 at-least-once 重放之后对外仍是
exactly-once: 不跳项、不重复。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional


CHECKPOINT_NAME = "checkpoint.json"
OUTPUTS_NAME = "outputs.jsonl"


def fsync_dir(path: Path) -> None:
    """best-effort 持久化目录项(文件 rename)。"""
    try:
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except (OSError, AttributeError):
        pass


class CheckpointStore:
    def __init__(self, run_dir: os.PathLike | str) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.run_dir / CHECKPOINT_NAME
        self.outputs_path = self.run_dir / OUTPUTS_NAME
        self._io_lock = threading.Lock()

    # ------------------------------------------------------------- checkpoint
    def save_checkpoint(self, state: Dict[str, Any]) -> Path:
        """原子写入 checkpoint.json(tmp + fsync + replace)。"""
        data = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=False)
        with self._io_lock:
            fd, tmp_name = tempfile.mkstemp(
                prefix=".checkpoint.", suffix=".tmp", dir=str(self.run_dir)
            )
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(data)
                    f.write("\n")
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.checkpoint_path)
                fsync_dir(self.run_dir)
            except BaseException:
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
        return self.checkpoint_path

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        if not self.checkpoint_path.exists():
            return None
        with self._io_lock:
            try:
                with self.checkpoint_path.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                # 原子 rename 保证不会读到半截文件; 真损坏时按无检查点处理
                return None

    # ---------------------------------------------------------------- outputs
    def append_output(self, record: Dict[str, Any]) -> None:
        """追加一条输出并 fsync, 这是 Sink 的“提交点”。"""
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._io_lock:
            with self.outputs_path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())

    def read_outputs(self) -> List[Dict[str, Any]]:
        if not self.outputs_path.exists():
            return []
        records: List[Dict[str, Any]] = []
        with self._io_lock:
            try:
                text = self.outputs_path.read_text(encoding="utf-8")
            except OSError:
                return []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # 末尾半行: 崩溃在 write 中途, 忽略
                continue
        return records

    def reset(self) -> None:
        with self._io_lock:
            for p in (self.checkpoint_path, self.outputs_path):
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
