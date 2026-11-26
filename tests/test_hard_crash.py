"""真实硬崩溃(os._exit 模拟 kill -9) -> 第二进程检查点恢复。"""

import tempfile
import unittest
from pathlib import Path

from pointwise.crash_selftest import run_hard_crash_scenario


class TestHardCrashRecovery(unittest.TestCase):
    def test_kill_midrun_then_subprocess_recover(self):
        with tempfile.TemporaryDirectory() as d:
            result = run_hard_crash_scenario(
                Path(d), count=24, crash_after=9, checkpoint_every=4, slow_delay_ms=40
            )
            self.assertEqual(
                result["child_exit_code"], 42,
                f"崩溃子进程未按预期退出: {result.get('child_stderr_tail')}",
            )
            self.assertEqual(
                result["recover_exit_code"], 0,
                f"恢复进程失败: {result.get('recover_stderr_tail')}\n"
                f"{result.get('recover_stdout_tail')}",
            )
            vr = result["disk_verification"]
            self.assertTrue(vr.get("ok"), vr)
            self.assertEqual(vr["missing_indices"], [])
            self.assertEqual(vr["duplicate_indices"], [])
            self.assertTrue(result["passed"], result)


if __name__ == "__main__":
    unittest.main()
