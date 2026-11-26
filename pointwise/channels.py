"""有界通道 —— 执行器里背压与取消唤醒的核心。

语义:

* ``put`` 在通道满时阻塞, 直到有空位; 这把下游的慢沿着依赖关系反向传导
  给上游(背压)。
* ``get`` 在通道空时阻塞, 直到有元素。
* ``close_eof()`` 表示正常结束: 排空残余元素后, ``get`` 抛
  :class:`EndOfStream`。
* ``cancel(reason)`` 表示异常/取消关闭: 所有阻塞中的 ``put/get`` 立即被
  唤醒并抛 :class:`ChannelClosed`(不等待排空)。

通道自带线程安全的指标(容量、水位、当前阻塞收发方、累计阻塞等待等),
页面与瓶颈分析直接消费 :meth:`snapshot`。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Optional


class ChannelError(Exception):
    """通道相关错误的基类。"""


class ChannelClosed(ChannelError):
    def __init__(self, name: str, reason: str = "closed") -> None:
        self.channel = name
        self.reason = reason
        super().__init__(f"channel {name!r} closed: {reason}")


class EndOfStream(ChannelClosed):
    """正常 EOF: 通道已关闭且队列已排空。"""

    def __init__(self, name: str) -> None:
        super().__init__(name, "eof")


class ChannelTimeout(ChannelError):
    def __init__(self, name: str, op: str) -> None:
        self.channel = name
        self.op = op
        super().__init__(f"channel {name!r} {op} timed out")


class BoundedChannel:
    def __init__(self, name: str, capacity: int = 4) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.id = name
        self.name = name
        self.capacity = int(capacity)

        self._q: Deque[Any] = deque()
        self._cond = threading.Condition()
        self._closed = False
        self._eof = False
        self.close_reason: Optional[str] = None

        # ---- metrics (均在 self._cond 下读写) ----
        self.enqueued_total = 0
        self.dequeued_total = 0
        self.high_watermark = 0
        self.put_wait_calls = 0
        self.get_wait_calls = 0
        self.put_wait_ns_total = 0
        self.get_wait_ns_total = 0
        self.last_put_wait_ms = 0.0
        self.last_get_wait_ms = 0.0
        # thread_ident -> 开始等待的 perf_counter
        self._put_waiters: Dict[int, float] = {}
        self._get_waiters: Dict[int, float] = {}

    # ------------------------------------------------------------------ close
    def close_eof(self) -> None:
        """正常结束关闭; 仍允许 get 排空, 排空后收到 EndOfStream。"""
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._eof = True
            self.close_reason = "eof"
            self._cond.notify_all()

    def cancel(self, reason: str = "cancelled") -> None:
        """取消式关闭: 唤醒所有等待者并令其抛 ChannelClosed。"""
        with self._cond:
            if self._closed:
                # 已经 EOF 关闭时, 升级为取消语义, 唤醒残余 get
                self._eof = False
                self.close_reason = reason or "cancelled"
            else:
                self._closed = True
                self._eof = False
                self.close_reason = reason or "cancelled"
            self._cond.notify_all()

    # -------------------------------------------------------------------- put
    def put(self, item: Any, timeout: Optional[float] = None) -> None:
        ident = threading.get_ident()
        deadline = None if timeout is None else time.monotonic() + timeout
        registered = False
        wait_started = 0.0

        with self._cond:
            try:
                while len(self._q) >= self.capacity and not self._closed:
                    if not registered:
                        registered = True
                        wait_started = time.perf_counter()
                        self.put_wait_calls += 1
                        self._put_waiters[ident] = wait_started
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise ChannelTimeout(self.name, "put")
                        self._cond.wait(remaining)
                        if (
                            time.monotonic() >= deadline
                            and len(self._q) >= self.capacity
                            and not self._closed
                        ):
                            raise ChannelTimeout(self.name, "put")
                    else:
                        self._cond.wait()

                if self._closed:
                    raise ChannelClosed(self.name, self.close_reason or "closed")

                self._q.append(item)
                self.enqueued_total += 1
                if len(self._q) > self.high_watermark:
                    self.high_watermark = len(self._q)
                # 唤醒可能在等数据的接收方(每次只可能有一个拿到, 用 notify 即可;
                # 为简单与稳妥使用 notify_all)
                self._cond.notify_all()
            finally:
                if registered:
                    started = self._put_waiters.pop(ident, wait_started)
                    waited_ns = max(0, int((time.perf_counter() - started) * 1e9))
                    self.put_wait_ns_total += waited_ns
                    self.last_put_wait_ms = waited_ns / 1e6

    # -------------------------------------------------------------------- get
    def get(self, timeout: Optional[float] = None) -> Any:
        ident = threading.get_ident()
        deadline = None if timeout is None else time.monotonic() + timeout
        registered = False
        wait_started = 0.0

        with self._cond:
            try:
                while not self._q and not self._closed:
                    if not registered:
                        registered = True
                        wait_started = time.perf_counter()
                        self.get_wait_calls += 1
                        self._get_waiters[ident] = wait_started
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise ChannelTimeout(self.name, "get")
                        self._cond.wait(remaining)
                        if (
                            time.monotonic() >= deadline
                            and not self._q
                            and not self._closed
                        ):
                            raise ChannelTimeout(self.name, "get")
                    else:
                        self._cond.wait()

                if not self._q:
                    # 队列空 + 已关闭
                    if self._eof:
                        raise EndOfStream(self.name)
                    raise ChannelClosed(self.name, self.close_reason or "cancelled")

                item = self._q.popleft()
                self.dequeued_total += 1
                self._cond.notify_all()  # 唤醒等空位的发送方
                return item
            finally:
                if registered:
                    started = self._get_waiters.pop(ident, wait_started)
                    waited_ns = max(0, int((time.perf_counter() - started) * 1e9))
                    self.get_wait_ns_total += waited_ns
                    self.last_get_wait_ms = waited_ns / 1e6

    # -------------------------------------------------------------- snapshots
    def _oldest_wait_ms(self, waiters: Dict[int, float]) -> float:
        if not waiters:
            return 0.0
        now = time.perf_counter()
        return max(0.0, (now - min(waiters.values())) * 1000.0)

    def snapshot(self) -> dict:
        with self._cond:
            size = len(self._q)
            put_waiters = len(self._put_waiters)
            get_waiters = len(self._get_waiters)
            put_blocked_ms = self._oldest_wait_ms(self._put_waiters)
            get_blocked_ms = self._oldest_wait_ms(self._get_waiters)
            put_avg_ms = (
                self.put_wait_ns_total / self.put_wait_calls / 1e6
                if self.put_wait_calls
                else 0.0
            )
            get_avg_ms = (
                self.get_wait_ns_total / self.get_wait_calls / 1e6
                if self.get_wait_calls
                else 0.0
            )
            return {
                "id": self.id,
                "name": self.name,
                "capacity": self.capacity,
                "size": size,
                "fill": round(size / self.capacity, 4),
                "full": size >= self.capacity,
                "empty": size == 0,
                "closed": self._closed,
                "eof": self._eof,
                "close_reason": self.close_reason,
                "enqueued_total": self.enqueued_total,
                "dequeued_total": self.dequeued_total,
                "high_watermark": self.high_watermark,
                "put_wait_calls": self.put_wait_calls,
                "get_wait_calls": self.get_wait_calls,
                "put_waiters": put_waiters,
                "get_waiters": get_waiters,
                "put_blocked_ms": round(put_blocked_ms, 1),
                "get_blocked_ms": round(get_blocked_ms, 1),
                "put_wait_total_ms": round(self.put_wait_ns_total / 1e6, 1),
                "get_wait_total_ms": round(self.get_wait_ns_total / 1e6, 1),
                "put_wait_avg_ms": round(put_avg_ms, 2),
                "get_wait_avg_ms": round(get_avg_ms, 2),
                "last_put_wait_ms": round(self.last_put_wait_ms, 2),
                "last_get_wait_ms": round(self.last_get_wait_ms, 2),
            }
