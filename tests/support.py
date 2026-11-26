"""测试辅助: 轮询等待条件成立(容忍线程调度抖动)。"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def wait_until(
    predicate: Callable[[], T],
    timeout: float = 8.0,
    interval: float = 0.02,
    message: str = "",
) -> T:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s: {message} (last={last!r})")
