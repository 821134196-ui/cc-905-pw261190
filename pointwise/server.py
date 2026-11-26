"""零依赖 HTTP 服务: JSON API + 静态仪表盘页面。"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from .engine import DEFAULT_RUN_DIR, ExecutionEngine, InvalidStateError

STATIC_DIR = Path(__file__).resolve().parent / "static"

STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}

# 常见路径别名, 兼容不同检查器/脚本的路由约定
GET_ALIASES = {
    "/health": "/api/health",
    "/status": "/api/state",
    "/state": "/api/state",
    "/api/status": "/api/state",
    "/graph": "/api/topology",
    "/topology": "/api/topology",
    "/api/graph": "/api/topology",
}
POST_ALIASES = {
    "/start": "/api/start",
    "/cancel": "/api/cancel",
    "/checkpoint": "/api/checkpoint",
    "/restore": "/api/restore",
    "/reset": "/api/reset",
    "/verify": "/api/verify",
    "/config": "/api/config",
}


class AppState:
    def __init__(self, run_dir: str = DEFAULT_RUN_DIR) -> None:
        self.engine = ExecutionEngine(run_dir)
        self.selftest_lock = threading.Lock()
        self.last_selftest: Optional[dict] = None


def make_handler(state: AppState) -> type:
    class Handler(BaseHTTPRequestHandler):
        server_version = "PointwiseExecutor/1.0"

        # 静默默认访问日志, 用我们自己的精简格式(可选)
        def log_message(self, fmt: str, *args: Any) -> None:
            pass

        # ------------------------------------------------------ helpers
        def _send_json(self, obj: Any, status: int = 200) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(body)

        def _send_static(self, path: str) -> None:
            if path not in STATIC_FILES:
                self._send_json({"error": "not found", "path": path}, 404)
                return
            filename, ctype = STATIC_FILES[path]
            file_path = STATIC_DIR / filename
            try:
                body = file_path.read_bytes()
            except OSError:
                self._send_json({"error": f"missing static file {filename}"}, 500)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            if length > 1_000_000:
                raise ValueError("请求体过大")
            raw = self.rfile.read(length)
            if not raw:
                return {}
            data = json.loads(raw.decode("utf-8"))
            if data is None:
                return {}
            if not isinstance(data, dict):
                raise ValueError("请求体必须是 JSON 对象")
            return data

        def do_OPTIONS(self) -> None:  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        # ---------------------------------------------------------- GET
        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            path = GET_ALIASES.get(url.path, url.path)
            try:
                if path == "/api/health":
                    self._send_json({"ok": True, "service": "pointwise-executor"})
                    return
                if path == "/api/state":
                    self._send_json(state.engine.snapshot())
                    return
                if path == "/api/topology":
                    self._send_json(state.engine.snapshot()["topology"])
                    return
                if path == "/api/selftest/crash-recovery":
                    self._send_json(
                        {"last": state.last_selftest, "running": state.selftest_lock.locked()}
                    )
                    return
                if path.startswith("/api/"):
                    self._send_json({"error": "unknown api", "path": path}, 404)
                    return
                if path == "/favicon.ico":
                    self.send_response(204)
                    self.end_headers()
                    return
                self._send_static(path)
            except BrokenPipeError:
                pass
            except Exception as e:  # pragma: no cover
                try:
                    self._send_json({"error": repr(e)}, 500)
                except Exception:
                    pass

        # --------------------------------------------------------- POST
        def do_POST(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            path = POST_ALIASES.get(url.path, url.path)
            try:
                body = self._read_json_body()
                engine = state.engine

                if path == "/api/config":
                    cfg = engine.configure(body)
                    self._send_json({"ok": True, "config": cfg})
                    return

                if path == "/api/start":
                    if body:
                        engine.configure(body)
                    snap = engine.start()
                    self._send_json({"ok": True, "state": snap})
                    return

                if path == "/api/cancel":
                    report = engine.cancel(reason=str(body.get("reason", "user cancel")))
                    self._send_json({"ok": True, "cancel": report, "state": engine.snapshot()})
                    return

                if path == "/api/checkpoint":
                    if engine.committer is None:
                        self._send_json({"ok": False, "error": "当前没有活动运行"}, 409)
                        return
                    cp = engine.committer.save_checkpoint(force=True)
                    engine._persisted_checkpoint = cp
                    engine._log("info", "手动触发检查点保存")
                    self._send_json({"ok": True, "checkpoint": cp})
                    return

                if path == "/api/restore":
                    snap = engine.restore()
                    self._send_json({"ok": True, "state": snap})
                    return

                if path == "/api/reset":
                    engine.reset()
                    self._send_json({"ok": True, "state": engine.snapshot()})
                    return

                if path == "/api/verify":
                    result = engine.verify_now()
                    self._send_json({"ok": True, "verification": result})
                    return

                if path == "/api/selftest/crash-recovery":
                    self._run_selftest(state)
                    return

                self._send_json({"error": "unknown api", "path": path}, 404)
            except InvalidStateError as e:
                self._send_json({"ok": False, "error": str(e)}, 409)
            except ValueError as e:
                self._send_json({"ok": False, "error": str(e)}, 400)
            except BrokenPipeError:
                pass
            except Exception as e:
                self._send_json({"ok": False, "error": repr(e)}, 500)

        def _run_selftest(self, state: AppState) -> None:
            # 同一时刻只允许一个故障自检(它会拉起子进程)
            if not state.selftest_lock.acquire(blocking=False):
                self._send_json({"ok": False, "error": "自检已在运行中"}, 409)
                return
            try:
                from .crash_selftest import run_hard_crash_scenario

                result = run_hard_crash_scenario()
                state.last_selftest = result
                self._send_json({"ok": True, "result": result}, status=200 if result["passed"] else 500)
            except Exception as e:
                self._send_json({"ok": False, "error": repr(e)}, 500)
            finally:
                state.selftest_lock.release()

    return Handler


def create_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    run_dir: str = DEFAULT_RUN_DIR,
) -> Tuple[ThreadingHTTPServer, AppState]:
    state = AppState(run_dir)
    handler = make_handler(state)

    class _Server(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = True

    httpd = _Server((host, port), handler)
    return httpd, state


def serve(
    host: str = "0.0.0.0",
    ports: Optional[list] = None,
    run_dir: str = DEFAULT_RUN_DIR,
) -> None:
    if ports is None:
        env_port = __import__("os").environ.get("PORT")
        if env_port:
            ports = [int(env_port)]
        else:
            ports = [8000, 3000, 8080]

    primary: Optional[ThreadingHTTPServer] = None
    bound_ports = []
    for p in ports:
        try:
            httpd, state = create_server(host, p, run_dir)
        except OSError as e:
            print(f"[pointwise] 端口 {p} 不可用: {e}")
            continue
        bound_ports.append(p)
        if primary is None:
            primary = httpd
            primary_state = state
        else:
            t = threading.Thread(target=httpd.serve_forever, name=f"http-{p}", daemon=True)
            t.start()

    if primary is None:
        raise SystemExit(f"无法在任何端口 {ports} 上启动服务")

    print("=" * 64)
    print(" Pointwise 本地执行器已启动")
    for p in bound_ports:
        print(f"   仪表盘:  http://{host if host != '0.0.0.0' else 'localhost'}:{p}/")
        print(f"   状态API: http://localhost:{p}/api/state")
    print(f"   运行目录: {run_dir}")
    print("   Ctrl+C 停止")
    print("=" * 64)
    try:
        primary.serve_forever()
    except KeyboardInterrupt:
        print("\n[pointwise] 正在关闭 ...")
    finally:
        primary.shutdown()
