# Pointwise · 含分发 / 阻塞算子 / 有界通道的本地执行器

一个**零第三方依赖**（Python 3.11 标准库 + 原生 JS/SVG）的本地数据流执行器，用来演示四件事：

1. **分发**：`Dispatcher` 按奇偶（或轮询）把输入分发到快 / 慢两个分支；
2. **阻塞算子 + 有界通道**：分支算子带可控阻塞延时，算子之间全部是容量有界的
   `BoundedChannel`（`threading.Condition` 实现），满则阻塞发送方、空则阻塞接收方；
3. **背压与取消唤醒**：慢分支打满后，阻塞沿依赖链反向传导，最终 **Source 停发**；
   取消信号会 `notify_all` 所有通道并置位取消 Event，**即时唤醒**阻塞在
   `put/get` 或长延时上的 worker，并给出每个 worker 的唤醒延迟；
4. **检查点恢复 + pointwise 核对**：原子检查点（tmp+fsync+rename）与逐条 fsync 的
   幂等输出台账（`outputs.jsonl`）。恢复后重放检查点 frontier 之后的项，Sink 按输入
   位置/索引去重；用**可控制的输入索引序列**独立核对输出**不跳项、不重复、无多余**。
   页面按 DAG 依赖关系实时标出**是哪个通道导致停顿**以及背压传导链。

另含一个**真实硬崩溃自检**：独立子进程在第 9 条输出 fsync 后 `os._exit(42)`
（模拟被 `kill -9`，连检查点都来不及写），第二个进程从台账重建 frontier、恢复并核对。

---

## 快速开始

```bash
# 方式一
./run.sh                 # 默认同时监听 8000 / 3000 / 8080

# 方式二
python3 -m pointwise     # 等价；可用 --port 8765 / --run-dir /tmp/pw 覆盖

# 方式三（npm 壳，便于按 Node 约定启动）
npm start

# 根目录便捷入口
python3 main.py
```

打开 <http://localhost:8000/>（或 <http://localhost:3000/>、<http://localhost:8080/>）。
端口可用 `PORT=8010 python3 -m pointwise` 或 `--port 8010` 覆盖（指定 PORT 时只监听该端口）。

### 30 秒体验四个需求

1. 直接点 **▶ 启动新运行**（默认 36 个索引，慢分支 420ms）。
2. 观察 DAG：`ch_slow` 变红、满水位、出现阻塞发送方角标；红色链路
   `op_slow(根源) → ch_slow → dispatcher → ch_src → source`，
   右侧“停顿归因”明确写出 **哪个通道** 停顿以及 **Source 已阻塞停发**。
3. 点 **■ 取消**：“取消唤醒时延”卡片显示所有 worker 在数百毫秒内被唤醒退出。
4. 点 **↩ 从检查点恢复**：跑完后“输出核对”显示 ✓ 通过（缺失 0 / 重复 0），
   并显示重放与幂等去重次数；下方索引网格全绿。
5. 点 **🧪 硬崩溃(kill)恢复自检**：一键运行双子进程崩溃/恢复场景并展示完整报告。

输入索引可在顶部自由控制：用 `起 + 数量` 生成区间，或在“显式索引”里填
`100,102,104,106 …`（必须唯一，优先级高于区间）。

---

## 拓扑与依赖

```
source → ch_src → dispatcher ┬→ ch_fast → op_fast ─┐
                             └→ ch_slow → op_slow ─┴→ ch_merged → sink
```

| 节点 | 含义 |
| --- | --- |
| `source` | 按受控索引序列发射，发射间隔可控；`ch_src` 满时阻塞 → 上游停发 |
| `dispatcher` | `parity`：偶数→fast、奇数→slow（稳定复现背压）；或 `round_robin` |
| `op_fast` / `op_slow` | 阻塞算子，延时可控；延时通过 `CancelToken.wait(delay)` 等待，取消即醒 |
| `ch_*` | 有界通道，指标含量/容量、高水位、当前阻塞收发方与阻塞时长、累计等待 |
| `sink` | 幂等提交：台账 fsync → 内存去重集合 → 周期原子检查点 |

停顿归因（`ExecutionEngine.analyze_bottleneck`）先按“有发送方阻塞 > 打满 > 高水位”
打分，再给“下游算子正处于 `processing`（卡在可控阻塞中）”的通道加根因证据分，
并沿 DAG 向上游回溯传导链，因此根因通道 `ch_slow` 不会被同为满的 `ch_src` 抢判。

---

## 检查点 / 恢复 / 核对语义

运行目录（默认 `/workspace/.runtime/`，可用 `--run-dir` 或
`POINTWISE_RUN_DIR` 配置）：

- `checkpoint.json`：原子替换写入，含受控索引序列 `indices`、连续提交前沿
  `frontier`、已提交位置集合、步长、run_id；
- `outputs.jsonl`：Sink 输出台账，**每条 `append + flush + fsync`，即提交点**。

提交顺序保证：**先 fsync 输出台账，再更新/落检查点**。恢复时以台账为最终事实，
重建已提交位置集合并计算连续 frontier，Source 从该位置重放；frontier 之后可能已有
乱序（快分支）提交，这些重放项在 Sink 处命中幂等去重（计入 `dedup_hits`，不重复写台账）。

核对器 `pointwise.topology.verify_indices` **直接对原始台账行独立计数**（不读 Sink
内存集合），同时按 `position` 与 `idx` 统计：

- `missing_indices`：期望但没有输出 —— 跳项；
- `duplicate_indices`：台账中出现 >1 次 —— 重复；
- `extra_indices` / 位置越界 —— 多余；
- `ok` 当且仅当数量恰好等于输入且无缺 / 重 / 多。

---

## HTTP / JSON API

| 方法 & 路径 | 作用 |
| --- | --- |
| `GET /` | 仪表盘（原生 JS/SVG，700ms 轮询 `/api/state`） |
| `GET /api/health` | 健康检查 |
| `GET /api/state` | 全量快照：通道/算子指标、瓶颈归因、检查点、核对、事件、索引网格 |
| `GET /api/topology` | DAG 节点与依赖边 |
| `POST /api/config` | 白名单校验并合并配置（运行中返回 409） |
| `POST /api/start` | 可带配置 patch；清空运行目录后启动全新运行 |
| `POST /api/cancel` | 取消并等待 worker 退出，返回唤醒时延报告 |
| `POST /api/checkpoint` | 立即强制保存检查点 |
| `POST /api/restore` | 从 `checkpoint.json` + 台账恢复续跑 |
| `POST /api/reset` | 取消活动运行并清除检查点/台账 |
| `POST /api/verify` | 立即独立核对，返回结果 |
| `POST /api/selftest/crash-recovery` | 运行真实硬崩溃恢复双子进程自检（约 5–15s） |

常见路径也提供无 `/api` 前缀别名：`/health`、`/state`（及 `/status`）、
`/topology`（及 `/graph`）、`/start`、`/cancel`、`/checkpoint`、`/restore`、
`/reset`、`/verify`、`/config`。

示例：

```bash
curl -s -X POST localhost:8000/api/start -H 'Content-Type: application/json' -d '{
  "indices": [3,10,7,22,1,18], "slow_delay_ms": 300, "cap_branch": 1
}'
curl -s localhost:8000/api/state | python3 -m json.tool
curl -s -X POST localhost:8000/api/cancel -d '{}'
curl -s -X POST localhost:8000/api/restore -d '{}'
```

---

## 测试与自检

```bash
# 17 个 unittest（通道、背压、取消唤醒、检查点恢复、硬崩溃子进程、HTTP API）
npm test
# 或
python3 -m unittest discover -s tests -v

# 单独跑硬崩溃场景并打印完整 JSON
python3 -m pointwise.crash_selftest
./verify.sh        # JS 语法检查 + 全部 Python 测试
```

最近一次结果：17/17 通过；硬崩溃场景稳定复现
`child_exit_code=42`、恢复进程 0、24 条输出无缺无重，并有实际重放去重
（例如 checkpoint frontier=5、台账 frontier=7、2 项超前提交被去重）。

---

## 目录结构

```
pointwise/
├── __main__.py          # python -m pointwise
├── cancellation.py      # CancelToken(Event + 取消回调, 记录取消时刻)
├── channels.py          # BoundedChannel: 有界/Condition/EOF/取消唤醒/指标
├── operators.py         # Source / Dispatcher / BlockOperator / Sink worker
├── checkpoint.py        # 原子 checkpoint + fsync outputs.jsonl 台账
├── topology.py          # DAG 依赖定义 + verify_indices 独立核对
├── engine.py            # 装配、生命周期、背压归因、恢复、快照
├── server.py            # ThreadingHTTPServer + REST + 静态页(零依赖)
├── crash_selftest.py    # os._exit 硬崩溃 + 子进程恢复场景
└── static/
    ├── index.html       # 仪表盘结构
    ├── styles.css       # 设计令牌 + 深浅色 + 响应式
    └── app.js           # SVG 依赖图构建/实时刷新/全部交互
tests/                   # unittest 套件(也可被 pytest 收集)
main.py                  # python main.py 入口
run.sh / verify.sh
package.json             # npm start / npm test 壳
```

## 设计说明与边界

- 这是**单进程多线程**执行器：阻塞语义是真实的 OS 线程 + `Condition` 等待，
  不是模拟时钟；因此取消唤醒延迟、背压阻塞时长都是真实测量值。
- 检查点用于演示 pointwise exactly-once 对外效果（at-least-once 重放 + 幂等 Sink）。
  崩溃注入用 `os._exit` 保证不执行任何清理/finalize，输出持久性依赖已 fsync 的台账。
- 页面无构建步骤、无 CDN、无外部字体/图表库，离线可用；支持系统深色模式与手动切换。
