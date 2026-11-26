#!/usr/bin/env bash
# 一键验证: 前端 JS 语法 + 全部 Python 测试(含真实硬崩溃恢复子进程用例)。
set -euo pipefail
cd "$(dirname "$0")"

echo "== node --check static/app.js =="
node --check pointwise/static/app.js

echo "== python compileall =="
python3 -m compileall -q pointwise tests main.py

echo "== unittest discover =="
python3 -m unittest discover -s tests -v
