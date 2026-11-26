"""背压 + 取消: 慢分支打满通道后上游必须停发; 取消必须快速唤醒所有 worker。"""

import tempfile
import unittest
from pathlib import Path

from pointwise.engine import ExecutionEngine, EngineConfig
from tests.support import wait_until


def slow_branch_config(**over) -> EngineConfig:
    base = dict(
        count=24,
        slow_delay_ms=120,
        fast_delay_ms=5,
        inject_interval_ms=0,
        cap_src=4,
        cap_branch=1,
        cap_merged=4,
        checkpoint_every=2,
    )
    base.update(over)
    return EngineConfig(**base)


class TestBackpressureAndCancel(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.engine = ExecutionEngine(self.dir)
        self.engine.config = slow_branch_config()

    def tearDown(self):
        try:
            if self.engine.state in ("running", "cancelling"):
                self.engine.cancel(timeout=5)
        finally:
            self._tmp.cleanup()

    def test_slow_branch_backpressures_upstream(self):
        # 足够慢, 让 dispatcher 在 ch_slow 上的单次阻塞超过 critical 阈值(200ms)
        self.engine.config = slow_branch_config(slow_delay_ms=260)
        self.engine.start()

        def stall_seen():
            s = self.engine.snapshot()
            bn = s["bottleneck"]
            src = next(c for c in s["channels"] if c["id"] == "ch_src")
            slow = next(c for c in s["channels"] if c["id"] == "ch_slow")
            if (
                bn
                and bn["channel_id"] == "ch_slow"
                and bn["severity"] == "critical"
                and slow["put_waiters"] > 0
                and src["put_waiters"] > 0
                and bn["source_upstream_stopped"]
            ):
                return s
            return None

        s = wait_until(stall_seen, timeout=8, message="应观察到 ch_slow 停顿且 source 停发")
        bn = s["bottleneck"]
        # 归因链必须按依赖关系把 ch_slow -> op_slow/dispatcher -> ch_src -> source 串起来
        self.assertIn("ch_slow", bn["chain_channel_ids"])
        self.assertEqual(bn["consumer_operator"], "op_slow")
        chain = set(bn["chain_node_ids"])
        self.assertTrue({"dispatcher", "ch_src", "source"} <= chain)
        self.assertIn("op_slow", chain)
        self.assertEqual(bn["severity"], "critical")

    def test_cancel_wakes_all_blocked_workers_promptly(self):
        self.engine.start()
        # 先确保已经进入大面积阻塞状态
        wait_until(
            lambda: (self.engine.snapshot()["bottleneck"] or {}).get("channel_id") == "ch_slow",
            timeout=8,
        )
        report = self.engine.cancel(timeout=5)
        self.assertTrue(report["ok"], f"仍有 worker 存活: {report['alive']}")
        self.assertEqual(report["alive"], [])
        self.assertLess(
            report["cancel_to_all_stopped_ms"],
            2000,
            f"取消耗时过长: {report['cancel_to_all_stopped_ms']}ms",
        )
        # 至少有一个 worker 记录到亚秒级唤醒延迟(阻塞在通道或延时上)
        lat = report["worker_wake_latency_ms"]
        self.assertTrue(lat, "应记录 worker 唤醒延迟")
        self.assertLess(report["max_worker_wake_latency_ms"], 1000)

        s = self.engine.snapshot()
        self.assertEqual(s["state"], "cancelled")
        alive = [n for n, t in self.engine.threads.items() if t.is_alive()]
        self.assertEqual(alive, [])

    def test_cancel_wakes_worker_in_long_blocking_operator_sleep(self):
        # 10 秒的阻塞延时; 算子必须在 token 上等待并被立即唤醒, 而不是睡满
        self.engine.config = slow_branch_config(count=4, slow_delay_ms=10_000, cap_branch=4)
        self.engine.start()
        wait_until(
            lambda: self.engine.stats["op_slow"].state == "processing",
            timeout=5,
            message="op_slow 应进入 processing(长阻塞)",
        )
        report = self.engine.cancel(timeout=3)
        self.assertTrue(report["ok"], report)
        self.assertLess(report["cancel_to_all_stopped_ms"], 1500)
        self.assertIsNotNone(self.engine.stats["op_slow"].wake_latency_ms)
        self.assertLess(self.engine.stats["op_slow"].wake_latency_ms, 1000)


if __name__ == "__main__":
    unittest.main()
