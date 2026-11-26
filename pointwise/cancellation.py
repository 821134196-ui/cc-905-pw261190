"""协作式取消令牌。

``cancel()`` 会置位 Event 并同步执行所有登记的回调。有界通道在构造时把
``channel.cancel`` 登记进来, 因此取消发生时, 阻塞在 ``put/get`` 上的
worker 会被 Condition.notify_all 立即唤醒; 阻塞算子的可中断延时则直接
等待这个 Event, 同样被立即唤醒。
"""

from __future__ import annotations

import threading
import time
from typing import Callable, List, Optional


class CancelToken:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._reason: Optional[str] = None
        self._callbacks: List[Callable[[str], None]] = []
        # 取消时刻的 monotonic 时间, 用于统计 worker 感知取消的延迟
        self.cancel_perf: Optional[float] = None
        self.cancel_wall: Optional[float] = None

    def cancel(self, reason: str = "cancelled") -> bool:
        """触发取消。首次调用返回 True, 重复调用返回 False。"""
        with self._lock:
            if self._event.is_set():
                return False
            self._reason = reason
            self.cancel_perf = time.perf_counter()
            self.cancel_wall = time.time()
            callbacks = list(self._callbacks)
            self._event.set()
        # 回调在锁外执行(通道关闭只会短暂取自己的锁)
        for cb in callbacks:
            try:
                cb(reason)
            except Exception:
                # 取消路径上的回调异常不应影响其它回调与调用方
                pass
        return True

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    def wait(self, timeout: Optional[float] = None) -> bool:
        """等待取消。返回 True 表示已取消(等待被唤醒), False 表示超时。"""
        return self._event.wait(timeout)

    def on_cancel(self, callback: Callable[[str], None]) -> None:
        """登记取消回调; 若已经取消则立即执行一次。"""
        run_now = False
        reason = None
        with self._lock:
            if self._event.is_set():
                run_now = True
                reason = self._reason
            else:
                self._callbacks.append(callback)
        if run_now:
            callback(reason or "cancelled")

    def observed_latency_ms(self) -> Optional[float]:
        """从取消时刻到当前的毫秒数(供 worker 记录自己的唤醒延迟)。"""
        if self.cancel_perf is None:
            return None
        return (time.perf_counter() - self.cancel_perf) * 1000.0
