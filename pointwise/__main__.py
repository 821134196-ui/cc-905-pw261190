"""``python -m pointwise`` 入口。"""

from __future__ import annotations

import argparse
import os

from .server import serve


def main() -> None:
    parser = argparse.ArgumentParser(description="Pointwise 本地执行器")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=None, help="只监听单个端口")
    parser.add_argument(
        "--run-dir",
        default=os.environ.get("POINTWISE_RUN_DIR", "/workspace/.runtime"),
        help="检查点与输出台账目录",
    )
    args = parser.parse_args()

    if args.port is not None:
        ports = [args.port]
    elif os.environ.get("PORT"):
        ports = [int(os.environ["PORT"])]
    else:
        ports = [8000, 3000, 8080]
    serve(host=args.host, ports=ports, run_dir=args.run_dir)


if __name__ == "__main__":
    main()
