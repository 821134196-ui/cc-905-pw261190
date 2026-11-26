"""执行器算子: Source / Dispatcher / BlockOperator / Sink。

每个算子就是一个常驻 worker 线程, 输入输出都是 :class:`BoundedChannel`。
所有等待点都可被取消唤醒:

* 通道 ``put/get`` 阻塞 —— 取消时通道被 ``cancel()``, Condition 通知唤醒;
* 阻塞算子的处理延时 —— 用 ``CancelToken.wait(delay)`` 实现, 取消立即返回。
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .channels import BoundedChannel, ChannelClosed, EndOfStream
from .cancellation import CancelToken


class OpStats:
    def __init__(self, op_id: str, label: str, kind: str) -> None:
        self.id = op_id
        self.label = label
        self.kind = kind  # source | dispatcher | blocking | sink
        self.state = "idle"  # idle|running|processing|completed|cancelled|failed
        self.processed = 0
        self.dedup_hits = 0
        self.error: Optional[str] = None
        self.last_index: Optional[int] = None
        self.last_branch: Optional[str] = None
        self.started_wall: Optional[float] = None
        self.finished_wall: Optional[float] = None
        self.last_duration_ms: Optional[float] = None
        self.wake_latency_ms: Optional[float] = None

    def mark_started(self) -> None:
        self.state = "running"
        self.started_wall = time.time()

    def mark_finished(self, state: str) -> None:
        self.state = state
        self.finished_wall = time.time()

    def mark_woken(self, token: CancelToken) -> None:
        lat = token.observed_latency_ms()
        if lat is not None:
            # 保留首次(退出路径上最有代表性的)唤醒延迟
            self.wake_latency_ms = round(lat, 2)

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "kind": self.kind,
            "state": self.state,
            "processed": self.processed,
            "dedup_hits": self.dedup_hits,
            "error": self.error,
            "last_index": self.last_index,
            "last_branch": self.last_branch,
            "started_wall": self.started_wall,
            "finished_wall": self.finished_wall,
            "last_duration_ms": (
                round(self.last_duration_ms, 2)
                if self.last_duration_ms is not None
                else None
            ),
            "wake_latency_ms": self.wake_latency_ms,
        }


EventCb = Callable[[str, str], None]


def _noop_event(level: str, msg: str) -> None:
    pass


# ---------------------------------------------------------------------- source
class SourceWorker:
    """按受控索引序列发射输入项。"""

    def __init__(
        self,
        *,
        items: List[Tuple[int, int]],
        out: BoundedChannel,
        token: CancelToken,
        stats: OpStats,
        inject_interval_s: float = 0.0,
        on_emit: Optional[Callable[[int], None]] = None,
        on_event: EventCb = _noop_event,
    ) -> None:
        self.items = items  # [(position, index), ...]
        self.out = out
        self.token = token
        self.stats = stats
        self.interval = max(0.0, inject_interval_s)
        self.on_emit = on_emit
        self.event = on_event

    def run(self) -> None:
        st = self.stats
        st.mark_started()
        try:
            for position, index in self.items:
                if self.token.cancelled:
                    st.mark_woken(self.token)
                    st.mark_finished("cancelled")
                    return
                item = {
                    "position": position,
                    "idx": index,
                    "branch": None,
                    "emitted_wall": time.time(),
                }
                # 通道满时在这里阻塞 —— 背压最终让上游停发
                self.out.put(item)
                st.processed += 1
                st.last_index = index
                if self.on_emit:
                    self.on_emit(position)
                if self.interval > 0:
                    if self.token.wait(self.interval):
                        st.mark_woken(self.token)
                        st.mark_finished("cancelled")
                        return
            # 数据发完: 关闭唯一输出通道(Eof 传播)
            self.out.close_eof()
            st.mark_finished("completed")
        except ChannelClosed as e:
            st.mark_woken(self.token)
            st.mark_finished("cancelled" if self.token.cancelled else "failed")
            if not self.token.cancelled:
                st.error = str(e)
                self.event("error", f"source 通道异常关闭: {e}")
        except Exception as e:  # pragma: no cover - 防御性
            st.mark_finished("failed")
            st.error = repr(e)
            self.event("error", f"source 失败: {e!r}")


# ------------------------------------------------------------------ dispatcher
class DispatcherWorker:
    """分发器: 按路由策略把每项送到对应分支的有界通道。

    对慢分支的 ``put`` 会阻塞分发器(队头阻塞), 随后 source→dispatcher
    通道也被填满, 背压一直传导到最上游。
    """

    def __init__(
        self,
        *,
        input: BoundedChannel,
        outputs: Dict[str, BoundedChannel],
        token: CancelToken,
        stats: OpStats,
        routing: str = "parity",
        on_event: EventCb = _noop_event,
    ) -> None:
        self.inp = input
        self.outputs = outputs
        self.token = token
        self.stats = stats
        self.routing = routing
        self.event = on_event
        self._rr = 0

    def _route(self, item: dict) -> str:
        branches = list(self.outputs.keys())
        if self.routing == "round_robin":
            b = branches[self._rr % len(branches)]
            self._rr += 1
            return b
        # parity(默认): 索引偶数走 fast, 奇数走 slow, 便于稳定复现背压
        return "fast" if int(item["idx"]) % 2 == 0 else "slow"

    def run(self) -> None:
        st = self.stats
        st.mark_started()
        try:
            while True:
                try:
                    item = self.inp.get()
                except EndOfStream:
                    break
                item["branch"] = self._route(item)
                st.last_branch = item["branch"]
                # 慢分支通道满时阻塞在此 —— 这是背压传导的关键一跳
                self.outputs[item["branch"]].put(item)
                st.processed += 1
                st.last_index = item["idx"]
            # 输入 EOF 且已排空: 关闭本算子作为唯一生产者的各分支通道
            for ch in self.outputs.values():
                ch.close_eof()
            st.mark_finished("completed")
        except ChannelClosed as e:
            st.mark_woken(self.token)
            st.mark_finished("cancelled" if self.token.cancelled else "failed")
            if not self.token.cancelled:
                st.error = str(e)
                self.event("error", f"dispatcher 通道异常关闭: {e}")
        except Exception as e:  # pragma: no cover
            st.mark_finished("failed")
            st.error = repr(e)
            self.event("error", f"dispatcher 失败: {e!r}")


# ------------------------------------------------------------- blocking operator
class BlockOperatorWorker:
    """带可控阻塞延时的分支算子(模拟慢处理 / 阻塞 IO)。"""

    def __init__(
        self,
        *,
        op_id: str,
        input: BoundedChannel,
        output: BoundedChannel,
        token: CancelToken,
        stats: OpStats,
        delay_s: float = 0.0,
        on_event: EventCb = _noop_event,
    ) -> None:
        self.op_id = op_id
        self.inp = input
        self.out = output
        self.token = token
        self.stats = stats
        self.delay_s = max(0.0, delay_s)
        self.event = on_event

    def run(self) -> None:
        st = self.stats
        st.mark_started()
        try:
            while True:
                try:
                    item = self.inp.get()
                except EndOfStream:
                    break
                t0 = time.perf_counter()
                st.state = "processing"
                # 可被取消立即唤醒的“阻塞算子”
                if self.delay_s > 0 and self.token.wait(self.delay_s):
                    st.mark_woken(self.token)
                    st.mark_finished("cancelled")
                    return
                st.last_duration_ms = (time.perf_counter() - t0) * 1000.0

                result = dict(item)
                result["processed_by"] = self.op_id
                result["processed_wall"] = time.time()

                st.state = "running"
                # 下游 merged 通道满时同样会阻塞
                self.out.put(result)
                st.processed += 1
                st.last_index = item["idx"]
                st.last_branch = item.get("branch")
            # 共享输出通道可能还有别的生产者, 不能由这里关闭
            st.mark_finished("completed")
        except ChannelClosed:
            st.mark_woken(self.token)
            st.mark_finished("cancelled" if self.token.cancelled else "failed")
        except Exception as e:  # pragma: no cover
            st.mark_finished("failed")
            st.error = repr(e)
            self.event("error", f"{self.op_id} 失败: {e!r}")


# ------------------------------------------------------------------------ sink
class SinkWorker:
    """幂等 Sink: 经 CommitManager 去重并 durable 提交。"""

    def __init__(
        self,
        *,
        input: BoundedChannel,
        token: CancelToken,
        stats: OpStats,
        committer: "Any",
        on_output: Optional[Callable[[dict, bool], None]] = None,
        on_event: EventCb = _noop_event,
    ) -> None:
        self.inp = input
        self.token = token
        self.stats = stats
        self.committer = committer
        self.on_output = on_output
        self.event = on_event

    def run(self) -> None:
        st = self.stats
        st.mark_started()
        try:
            while True:
                try:
                    item = self.inp.get()
                except EndOfStream:
                    break
                record = {
                    "position": item["position"],
                    "idx": item["idx"],
                    "branch": item.get("branch"),
                    "processed_by": item.get("processed_by"),
                    "committed_wall": time.time(),
                }
                is_new = self.committer.commit(record)
                if is_new:
                    st.processed += 1
                    st.last_index = record["idx"]
                else:
                    st.dedup_hits += 1
                if self.on_output:
                    self.on_output(record, is_new)
            st.mark_finished("completed")
        except ChannelClosed:
            st.mark_woken(self.token)
            st.mark_finished("cancelled" if self.token.cancelled else "failed")
        except Exception as e:  # pragma: no cover
            st.mark_finished("failed")
            st.error = repr(e)
            self.event("error", f"sink 失败: {e!r}")
