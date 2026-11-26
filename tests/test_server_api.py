"""HTTP API + 静态页面的端到端测试(端口 0 随机端口)。"""

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from pointwise.server import create_server
from tests.support import wait_until


class TestServerAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.httpd, cls.state = create_server(
            host="127.0.0.1", port=0, run_dir=str(Path(cls._tmp.name) / "runtime")
        )
        cls.port = cls.httpd.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls._tmp.cleanup()

    def _get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=10) as r:
            return r.status, json.loads(r.read().decode())

    def _post(self, path, payload=None):
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(payload or {}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_01_health_and_page(self):
        status, data = self._get("/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])

        with urllib.request.urlopen(self.base + "/", timeout=5) as r:
            html = r.read().decode()
        self.assertIn("Pointwise", html)
        self.assertIn('id="dag"', html)

        with urllib.request.urlopen(self.base + "/app.js", timeout=5) as r:
            js = r.read().decode()
        self.assertIn("buildGraph", js)

    def test_02_topology_has_dependencies(self):
        _, data = self._get("/api/topology")
        ids = {n["id"] for n in data["nodes"]}
        self.assertEqual(
            ids,
            {"source", "ch_src", "dispatcher", "ch_fast", "op_fast",
             "ch_slow", "op_slow", "ch_merged", "sink"},
        )
        self.assertGreaterEqual(len(data["edges"]), 9)

    def test_03_config_validation(self):
        status, data = self._post("/api/config", {"indices": [1, 1, 2]})
        self.assertEqual(status, 400)
        self.assertIn("唯一", data["error"])

        status, data = self._post("/api/config", {"slow_delay_ms": 999999})
        self.assertEqual(status, 400)

    def test_04_run_cancel_restore_verify_flow(self):
        # 配置成容易观察背压的小拓扑
        status, data = self._post("/api/start", {
            "indices": [4, 1, 8, 3, 12, 7, 16, 5, 20, 9],
            "slow_delay_ms": 120,
            "fast_delay_ms": 3,
            "inject_interval_ms": 0,
            "cap_branch": 1,
            "checkpoint_every": 2,
        })
        self.assertEqual(status, 200, data)

        def bottleneck_seen():
            _, s = self._get("/api/state")
            return (s.get("bottleneck") or {}).get("channel_id") == "ch_slow" or None

        wait_until(bottleneck_seen, timeout=8, message="API 状态应出现 ch_slow 瓶颈")

        status, data = self._post("/api/cancel", {})
        self.assertEqual(status, 200)
        self.assertTrue(data["cancel"]["ok"], data["cancel"])

        status, data = self._post("/api/restore", {})
        self.assertEqual(status, 200, data)

        def completed():
            _, s = self._get("/api/state")
            return s if s["state"] == "completed" else None

        s = wait_until(completed, timeout=25)
        v = s["verification"]
        self.assertTrue(v["ok"], v)
        self.assertEqual(v["expected_count"], 10)
        self.assertEqual(v["missing_indices"], [])
        self.assertEqual(v["duplicate_indices"], [])

        status, data = self._post("/api/verify", {})
        self.assertTrue(data["verification"]["ok"])

    def test_05_reset(self):
        status, data = self._post("/api/reset", {})
        self.assertEqual(status, 200)
        self.assertEqual(data["state"]["state"], "idle")


if __name__ == "__main__":
    unittest.main()
