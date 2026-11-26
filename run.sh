#!/usr/bin/env bash
# 启动 Pointwise 仪表盘/API。默认同时监听 8000 与 3000；可用 PORT 或 --port 覆盖。
set -euo pipefail
cd "$(dirname "$0")"
exec python3 -m pointwise "$@"
