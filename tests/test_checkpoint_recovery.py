"""检查点恢复: 取消后续跑, 输出按受控输入索引不跳项、不重复, 且确实发生重放去重。"""

import tempfile
import unittest
from pathlib import Path

from pointwise.engine import ExecutionEngine, EngineConfig
from tests.support import wait_until


CONTROLLED_INDICES = [3, 10, 7, 22, 1, 18, 9, 4, 13, 26, 5, 2,
                      15, 30, 11, 8, 21, 16, 19, 0]


class TestCheckpointRecovery(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.engine = ExecutionEngine(self.dir)
        self.engine.config = EngineConfig(
            indices=list(CONTROLLED_INDICES),
            slow_delay_ms=90,
            fast_delay_ms=3,
            inject_interval_ms=0,
            cap_src=4,
            cap_branch=1,
            cap_merged=4,
            checkpoint_every=2,
        )

    def tearDown(self):
        try:
            if self.engine.state in ("running", "cancelling"):
                self.engine.cancel(timeout=5)
        finally:
            self._tmp.cleanup()

    def _committed_ahead(self):
        c = self.engine.committer
        if c is None:
            return 0, 0
        with c.lock:
            return len(c.committed), c.frontier()

    def test_cancel_then_restore_completes_exactly_once(self):
        self.engine.start()

        # 等到出现“快分支超前提交、连续 frontier 之后已有提交”的交错状态,
        # 这样恢复时必然重放一部分已提交项并触发幂等去重。
        def ahead_exists():
            total, frontier = self._committed_ahead()
            return total >= 5 and total >= frontier + 1

        wait_until(ahead_exists, timeout=8, message="应出现分支交错导致的超前提交")
        cp_before = self.engine.store.load_checkpoint()
        self.assertIsNotNone(cp_before)

        report = self.engine.cancel(timeout=5)
        self.assertTrue(report["ok"], report)

        # 从检查点恢复(输入索引序列来自 checkpoint)
        snap = self.engine.restore()
        self.assertEqual(snap["state"], "running")
        self.assertIsNotNone(self.engine.restore_info)

        state = wait_until(
            lambda: self.engine.state if self.engine.state == "completed" else None,
            timeout=25,
            message="恢复后应运行至 completed",
        )
        self.assertEqual(state, "completed")

        v = self.engine.last_verification
        self.assertIsNotNone(v)
        self.assertEqual(v["expected_count"], len(CONTROLLED_INDICES))
        self.assertEqual(v["output_records"], len(CONTROLLED_INDICES))
        self.assertEqual(v["missing_indices"], [])
        self.assertEqual(v["duplicate_indices"], [])
        self.assertEqual(v["extra_indices"], [])
        self.assertTrue(v["ok"], v)

        # 磁盘上的台账独立再核对一次
        records = self.engine.store.read_outputs()
        self.assertEqual(len(records), len(CONTROLLED_INDICES))
        self.assertEqual(sorted(r["idx"] for r in records), sorted(CONTROLLED_INDICES))

        # 恢复确实重放并去重了超前完成的项(at-least-once 重放 -> exactly-once 台账)
        ri = self.engine.restore_info
        self.assertGreaterEqual(ri["already_committed_ahead_count"], 1)
        self.assertGreaterEqual(self.engine.committer.dedup_hits, 1)

    def test_restore_without_checkpoint_rejected(self):
        with self.assertRaises(Exception):
            self.engine.restore()

    def test_completed_run_passes_verification(self):
        self.engine.config = EngineConfig(
            count=30, slow_delay_ms=4, fast_delay_ms=1,
            inject_interval_ms=0, checkpoint_every=3, cap_branch=2,
        )
        self.engine.start()
        wait_until(lambda: self.engine.state == "completed", timeout=20)
        v = self.engine.last_verification
        self.assertTrue(v["ok"], v)
        self.assertEqual(v["output_records"], 30)


if __name__ == "__main__":
    unittest.main()
