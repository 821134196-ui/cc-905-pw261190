"""有界通道: 容量阻塞、EOF 排空、取消即时唤醒。"""

import threading
import time
import unittest

from pointwise.channels import BoundedChannel, ChannelClosed, EndOfStream
from pointwise.cancellation import CancelToken
from tests.support import wait_until


class TestBoundedChannel(unittest.TestCase):
    def test_capacity_blocks_sender_until_receiver(self):
        ch = BoundedChannel("t", capacity=1)
        ch.put("a")

        outcome = {}

        def sender():
            t0 = time.perf_counter()
            ch.put("b")  # 必须阻塞, 直到主线程 get
            outcome["waited_ms"] = (time.perf_counter() - t0) * 1000
            outcome["done"] = True

        th = threading.Thread(target=sender)
        th.start()
        snap = wait_until(lambda: ch.snapshot() if ch.snapshot()["put_waiters"] == 1 else None,
                          message="sender should be blocked")
        self.assertEqual(snap["size"], 1)
        self.assertTrue(snap["full"])

        self.assertEqual(ch.get(), "a")
        th.join(2)
        self.assertFalse(th.is_alive())
        self.assertTrue(outcome["done"])
        self.assertGreaterEqual(outcome["waited_ms"], 0)
        self.assertEqual(ch.get(), "b")

    def test_eof_allows_drain_then_end_of_stream(self):
        ch = BoundedChannel("t", capacity=4)
        ch.put(1)
        ch.put(2)
        ch.close_eof()
        self.assertEqual(ch.get(), 1)
        self.assertEqual(ch.get(), 2)
        with self.assertRaises(EndOfStream):
            ch.get()

    def test_cancel_wakes_blocked_put_promptly(self):
        ch = BoundedChannel("t", capacity=1)
        ch.put("full")
        token = CancelToken()
        token.on_cancel(lambda reason: ch.cancel(reason))

        err = []

        def sender():
            try:
                ch.put("x")
            except ChannelClosed as e:
                err.append(e)

        th = threading.Thread(target=sender)
        th.start()
        wait_until(lambda: ch.snapshot()["put_waiters"] == 1)

        t0 = time.perf_counter()
        token.cancel("test")
        th.join(2.0)
        latency_ms = (time.perf_counter() - t0) * 1000

        self.assertFalse(th.is_alive(), "阻塞 put 必须被取消唤醒")
        self.assertEqual(len(err), 1)
        self.assertLess(latency_ms, 1000, f"唤醒延迟过高: {latency_ms:.0f}ms")

    def test_cancel_wakes_blocked_get_promptly(self):
        ch = BoundedChannel("t", capacity=2)
        token = CancelToken()
        token.on_cancel(lambda reason: ch.cancel(reason))

        err = []

        def receiver():
            try:
                ch.get()  # 空通道上阻塞
            except ChannelClosed as e:
                err.append(e)

        th = threading.Thread(target=receiver)
        th.start()
        wait_until(lambda: ch.snapshot()["get_waiters"] == 1)

        t0 = time.perf_counter()
        token.cancel("stop")
        th.join(2.0)
        latency_ms = (time.perf_counter() - t0) * 1000

        self.assertFalse(th.is_alive())
        self.assertEqual(len(err), 1)
        self.assertLess(latency_ms, 1000)

    def test_timeout_raises(self):
        ch = BoundedChannel("t", capacity=1)
        ch.put("x")
        from pointwise.channels import ChannelTimeout

        with self.assertRaises(ChannelTimeout):
            ch.put("y", timeout=0.05)
        with self.assertRaises(ChannelTimeout):
            BoundedChannel("empty").get(timeout=0.05)


if __name__ == "__main__":
    unittest.main()
