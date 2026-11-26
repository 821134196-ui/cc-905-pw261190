/* Pointwise dashboard — vanilla JS, no dependencies. */
"use strict";

const SVGNS = "http://www.w3.org/2000/svg";
const $ = (id) => document.getElementById(id);

/* ------------------------------------------------------------------ theme */
(function initTheme() {
  const saved = localStorage.getItem("pw-theme");
  const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  document.documentElement.dataset.theme = saved || (prefersDark ? "dark" : "light");
})();
$("themeToggle").addEventListener("click", () => {
  const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  localStorage.setItem("pw-theme", next);
});

/* ------------------------------------------------------------------- api */
async function api(path, body) {
  const opt = body === undefined
    ? { method: "GET" }
    : {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      };
  const res = await fetch(path, opt);
  let data = null;
  try { data = await res.json(); } catch (_) { /* no json */ }
  if (!res.ok || (data && data.ok === false)) {
    throw new Error((data && data.error) || `HTTP ${res.status}`);
  }
  return data;
}

function escapeHtml(v) {
  return String(v ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}
function fmtTime(wall) {
  if (!wall) return "—";
  const d = new Date(wall * 1000);
  const p = (n, l = 2) => String(n).padStart(l, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
function fmtMs(v) {
  if (v === null || v === undefined) return "—";
  return v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${Math.round(v)} ms`;
}

/* ------------------------------------------------------------- SVG graph */
const LAYOUT = {
  source:       { x: 20,  y: 175, w: 140, h: 70, type: "op", tone: "neutral" },
  ch_src:       { x: 185, y: 187, w: 132, h: 46, type: "ch" },
  dispatcher:   { x: 342, y: 175, w: 150, h: 70, type: "op", tone: "neutral" },
  ch_fast:      { x: 520, y: 67,  w: 132, h: 46, type: "ch" },
  op_fast:      { x: 680, y: 55,  w: 150, h: 70, type: "op", tone: "fast" },
  ch_slow:      { x: 520, y: 307, w: 132, h: 46, type: "ch" },
  op_slow:      { x: 680, y: 295, w: 150, h: 70, type: "op", tone: "slow" },
  ch_merged:    { x: 858, y: 187, w: 132, h: 46, type: "ch" },
  sink:         { x: 1018, y: 175, w: 150, h: 70, type: "op", tone: "neutral" },
};

// 非水平连线的显式贝塞尔路径
const CUSTOM_EDGE_D = {
  "dispatcher->ch_fast": "M492,200 C516,200 496,90 520,90",
  "dispatcher->ch_slow": "M492,220 C516,220 496,330 520,330",
  "op_fast->ch_merged": "M830,90 C868,90 884,187 924,187",
  "op_slow->ch_merged": "M830,330 C868,330 884,233 924,233",
};

function svgEl(name, attrs = {}, parent) {
  const e = document.createElementNS(SVGNS, name);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

const graphRefs = { edges: {}, nodes: {} };
let prevChannelTotals = {};

function edgeD(from, to) {
  const key = `${from}->${to}`;
  if (CUSTOM_EDGE_D[key]) return CUSTOM_EDGE_D[key];
  const a = LAYOUT[from], b = LAYOUT[to];
  const x1 = a.x + a.w, y1 = a.y + a.h / 2;
  const x2 = b.x, y2 = b.y + b.h / 2;
  return `M${x1},${y1} L${x2},${y2}`;
}

function buildGraph(topology) {
  const svg = $("dag");
  svg.innerHTML = "";
  const defs = svgEl("defs", {}, svg);
  for (const [id, color] of [["arrow", "#898781"], ["arrow-critical", "#d03b3b"], ["arrow-blue", "#2a78d6"]]) {
    const m = svgEl("marker", {
      id, viewBox: "0 0 10 10", refX: "9", refY: "5",
      markerWidth: "9", markerHeight: "9", orient: "auto-start-reverse",
      markerUnits: "userSpaceOnUse",
    }, defs);
    svgEl("path", { d: "M0,0 L10,5 L0,10 z", fill: color }, m);
  }

  const edgeLayer = svgEl("g", { class: "edges" }, svg);
  for (const edge of topology.edges) {
    const path = svgEl("path", {
      class: "edge", d: edgeD(edge.from, edge.to), "data-from": edge.from, "data-to": edge.to,
      "marker-end": "url(#arrow)",
    }, edgeLayer);
    graphRefs.edges[`${edge.from}->${edge.to}`] = path;
  }

  const nodeLayer = svgEl("g", { class: "nodes" }, svg);
  for (const node of topology.nodes) {
    const L = LAYOUT[node.id];
    if (!L) continue;
    if (L.type === "ch") buildChannelNode(node, L, nodeLayer);
    else buildOpNode(node, L, nodeLayer);
  }
}

function buildOpNode(node, L, layer) {
  const g = svgEl("g", { class: `op-node tone-${L.tone || "neutral"}`, "data-id": node.id }, layer);
  svgEl("rect", { class: "body", x: L.x, y: L.y, width: L.w, height: L.h, rx: 12 }, g);
  svgEl("rect", { class: "accent", x: L.x, y: L.y + 10, width: 4, height: L.h - 20 }, g);
  svgEl("text", { class: "op-label", x: L.x + 16, y: L.y + 26 }, g).textContent = node.label;
  svgEl("text", { class: "op-count", x: L.x + L.w - 12, y: L.y + 26, "text-anchor": "end" }, g);
  svgEl("text", { class: "op-sub", x: L.x + 16, y: L.y + 44 }, g).textContent = node.sub;
  svgEl("circle", { class: "op-state-dot", cx: L.x + 19, cy: L.y + 61, r: 3 }, g);
  svgEl("text", { class: "op-state", x: L.x + 27, y: L.y + 65 }, g);
  svgEl("text", { class: "op-dedup", x: L.x + L.w - 12, y: L.y + 65, "text-anchor": "end" }, g);

  const tag = svgEl("g", { class: "root-tag-g", visibility: "hidden" }, g);
  svgEl("rect", { class: "root-tag", x: L.x + L.w / 2 - 22, y: L.y - 9, width: 44, height: 18, rx: 9 }, tag);
  const tt = svgEl("text", { class: "root-tag-text", x: L.x + L.w / 2, y: L.y }, tag);
  tt.textContent = "根源";

  graphRefs.nodes[node.id] = {
    g,
    count: g.querySelector(".op-count"),
    state: g.querySelector(".op-state"),
    stateDot: g.querySelector(".op-state-dot"),
    dedup: g.querySelector(".op-dedup"),
    tag,
  };
}

function buildChannelNode(node, L, layer) {
  const g = svgEl("g", { class: "ch-node", "data-id": node.id }, layer);
  svgEl("rect", { class: "body", x: L.x, y: L.y, width: L.w, height: L.h, rx: 9 }, g);
  svgEl("text", { class: "ch-name", x: L.x + 10, y: L.y + 17 }, g).textContent = node.label;
  svgEl("text", { class: "ch-count", x: L.x + L.w - 10, y: L.y + 17, "text-anchor": "end" }, g);
  svgEl("rect", { class: "ch-track", x: L.x + 10, y: L.y + 27, width: L.w - 20, height: 7, rx: 3.5 }, g);
  const fill = svgEl("rect", { class: "ch-fill", x: L.x + 10, y: L.y + 27, width: 0, height: 7, rx: 3.5 }, g);

  const badge = svgEl("g", { class: "badge", visibility: "hidden" }, g);
  const cx = L.x + L.w - 4, cy = L.y + 2;
  svgEl("circle", { class: "pulse-ring", cx, cy, r: 9 }, badge);
  svgEl("circle", { class: "ch-badge", cx, cy, r: 9 }, badge);
  svgEl("text", { class: "ch-badge-text", x: cx, y: cy + 0.5 }, badge);

  const title = svgEl("title", {}, g);
  graphRefs.nodes[node.id] = { g, fill, count: g.querySelector(".ch-count"), badge, badgeText: badge.querySelector(".ch-badge-text"), title };
}

const OP_STATE_TEXT = {
  idle: "空闲", running: "运行中", processing: "处理中…",
  completed: "已完成", cancelled: "已取消", failed: "失败",
};

function renderGraph(s) {
  if (!s.topology || !graphRefs.built) return;
  const chById = Object.fromEntries(s.channels.map((c) => [c.id, c]));
  const opById = Object.fromEntries(s.operators.map((o) => [o.id, o]));
  const chain = new Set(s.bottleneck ? s.bottleneck.chain_node_ids : []);
  const rootOp = s.bottleneck ? s.bottleneck.consumer_operator : null;
  const empty = s.channels.length === 0;

  // channels
  for (const [id, ref] of Object.entries(graphRefs.nodes)) {
    const c = chById[id];
    if (c && ref.fill) {
      const L = LAYOUT[id];
      const maxW = L.w - 20;
      ref.fill.setAttribute("width", Math.max(0, maxW * c.fill));
      ref.count.textContent = `${c.size}/${c.capacity}`;
      const blocked = c.put_waiters > 0;
      ref.g.classList.toggle("blocked", blocked);
      ref.g.classList.toggle("full", c.full && !blocked);
      ref.g.classList.toggle("warn", !c.full && c.fill >= 0.8);
      ref.g.classList.toggle("chain", chain.has(id));
      if (blocked) {
        ref.badge.setAttribute("visibility", "visible");
        ref.badgeText.textContent = String(c.put_waiters);
      } else {
        ref.badge.setAttribute("visibility", "hidden");
      }
      ref.title.textContent =
        `${id}: 积压 ${c.size}/${c.capacity}, 高水位 ${c.high_watermark}; ` +
        `累计入 ${c.enqueued_total} / 出 ${c.dequeued_total}; ` +
        `阻塞发送 ${c.put_wait_calls} 次 共 ${fmtMs(c.put_wait_total_ms)} ` +
        `(当前最长 ${fmtMs(c.put_blocked_ms)}); 阻塞接收 ${c.get_wait_calls} 次`;
    } else if (ref.fill && empty) {
      ref.fill.setAttribute("width", "0");
      ref.count.textContent = "–/–";
      ref.g.classList.remove("blocked", "full", "warn", "chain");
      ref.badge.setAttribute("visibility", "hidden");
    }
    const o = opById[id];
    if (o && ref.state) {
      ref.count.textContent = `×${o.processed}`;
      ref.state.textContent = OP_STATE_TEXT[o.state] || o.state;
      ref.state.setAttribute("class", `op-state state-${o.state}`);
      if (ref.stateDot) ref.stateDot.setAttribute("class", `op-state-dot dot-${o.state}`);
      if (ref.dedup) ref.dedup.textContent = o.dedup_hits ? `去重 ${o.dedup_hits}` : "";
      ref.g.classList.toggle("root-cause", rootOp === id);
      ref.g.classList.toggle("in-chain", rootOp !== id && chain.has(id));
      ref.tag.setAttribute("visibility", rootOp === id ? "visible" : "hidden");
      ref.g.querySelector("title")?.remove();
      const t = svgEl("title", {}, ref.g);
      t.textContent = `${o.label}: ${OP_STATE_TEXT[o.state] || o.state}, 已处理 ${o.processed}` +
        (o.dedup_hits ? `, 去重 ${o.dedup_hits}` : "") +
        (o.wake_latency_ms != null ? `, 取消唤醒延迟 ${o.wake_latency_ms}ms` : "") +
        (o.error ? `, 错误 ${o.error}` : "");
    } else if (ref.state && empty) {
      ref.count.textContent = "";
      ref.state.textContent = "空闲";
      ref.state.setAttribute("class", "op-state state-idle");
      if (ref.stateDot) ref.stateDot.setAttribute("class", "op-state-dot dot-idle");
      if (ref.dedup) ref.dedup.textContent = "";
      ref.g.classList.remove("root-cause", "in-chain");
      ref.tag.setAttribute("visibility", "hidden");
    }
  }

  // edges: chain highlight + flow animation when channel counters advance
  for (const [key, path] of Object.entries(graphRefs.edges)) {
    const [from, to] = key.split("->");
    const isChain = !empty && chain.has(from) && chain.has(to);
    path.classList.toggle("chain", isChain);
    const chId = LAYOUT[from]?.type === "ch" ? from : (LAYOUT[to]?.type === "ch" ? to : null);
    let flowing = false;
    if (chId && chById[chId]) {
      const c = chById[chId];
      const prev = prevChannelTotals[chId];
      const cur = c.enqueued_total + c.dequeued_total;
      if (prev !== undefined && cur !== prev) flowing = true;
      prevChannelTotals[chId] = cur;
    }
    flowing = flowing && s.state === "running" && !isChain;
    path.classList.toggle("flowing", flowing);
    const marker = isChain ? "url(#arrow-critical)" : flowing ? "url(#arrow-blue)" : "url(#arrow)";
    path.setAttribute("marker-end", marker);
  }
}

/* -------------------------------------------------------------- panels */
function renderStatus(s) {
  const map = {
    idle: ["badge-idle", "空闲"],
    running: ["badge-running", "运行中"],
    cancelling: ["badge-cancelling", "取消中…"],
    cancelled: ["badge-cancelled", "已取消(worker 已唤醒)"],
    completed: ["badge-completed", "已完成"],
    failed: ["badge-failed", "失败"],
  };
  const [cls, text] = map[s.state] || ["badge-idle", s.state];
  const badge = $("runBadge");
  badge.className = `badge ${cls}`;
  $("runStateText").textContent = text + (s.run_id ? ` · ${s.run_id}` : "");

  const active = s.state === "running" || s.state === "cancelling";
  $("btnStart").disabled = active;
  $("btnCancel").disabled = s.state !== "running";
  $("btnCheckpoint").disabled = s.state !== "running";
  $("btnRestore").disabled = active || !s.checkpoint;
}

function renderBottleneck(s) {
  const b = s.bottleneck;
  const card = $("bottleneckCard");
  const sev = $("bnSeverity");
  const body = $("bottleneckBody");
  card.classList.toggle("has-critical", !!b && b.severity === "critical");
  if (!b) {
    sev.className = "sev sev-none";
    sev.textContent = "无停顿";
    body.innerHTML = s.state === "running"
      ? '<p class="bn-ok">✓ 各通道流动正常，没有发送方阻塞。</p>'
      : '<p class="muted">启动后，这里会按依赖关系指出是哪个通道导致停顿。</p>';
    return;
  }
  const sevText = { notice: "注意", warning: "停顿预警", critical: "严重背压" }[b.severity] || b.severity;
  sev.className = `sev sev-${b.severity}`;
  sev.textContent = sevText;
  const chainPills = (b.chain_node_ids || [])
    .map((id) => {
      const label = s.topology.nodes.find((n) => n.id === id)?.label || id;
      return `<span class="chain-pill">${escapeHtml(label)}</span>`;
    })
    .join('<span class="chain-arrow">←</span>');
  body.innerHTML = `
    <p class="bn-title"><span class="bn-icon">⛔</span> 停顿通道：<code>${escapeHtml(b.channel_id)}</code>
      ${b.source_upstream_stopped ? '<span class="chain-pill">上游已停发</span>' : ""}
    </p>
    <p class="bn-message">${escapeHtml(b.message)}</p>
    <div class="bn-chain"><span class="muted" style="font-size:11.5px">归因/传导链：</span>${chainPills}</div>`;
}

function renderVerify(s) {
  const v = s.verification;
  const body = $("verifyBody");
  if (!v) {
    body.innerHTML = '<p class="muted">尚无输出。</p>';
    return;
  }
  const missChips = (v.missing_indices || []).slice(0, 60)
    .map((i) => `<span class="mini-chip missing">缺 ${escapeHtml(i)}</span>`).join("");
  const dupChips = (v.duplicate_indices || []).slice(0, 60)
    .map((i) => `<span class="mini-chip dup">重 ${escapeHtml(i)}</span>`).join("");
  const ri = s.restore_info;
  body.innerHTML = `
    <div class="verify-hero">
      <div class="verify-mark ${v.ok ? "pass" : "fail"}">${v.ok ? "✓" : "✕"}</div>
      <div>
        <div class="verify-title">${v.ok ? "核对通过：不跳项、不重复" : "核对未通过"}</div>
        <div class="verify-sub">对原始 outputs.jsonl 台账逐行独立计数${v.checked_wall_ms != null ? ` · 核对耗时 ${v.checked_wall_ms}ms` : ""}</div>
      </div>
    </div>
    <div class="stat-grid">
      <div class="stat"><div class="k">输入索引</div><div class="v">${v.expected_count}</div></div>
      <div class="stat ${v.ok ? "v-good" : ""}"><div class="k">输出记录</div><div class="v">${v.output_records}</div></div>
      <div class="stat ${v.missing_indices.length ? "v-critical" : ""}"><div class="k">缺失/跳项</div><div class="v">${v.missing_indices.length}</div></div>
      <div class="stat ${v.duplicate_indices.length ? "v-critical" : "v-good"}"><div class="k">重复</div><div class="v">${v.duplicate_indices.length}</div></div>
    </div>
    ${missChips || dupChips ? `<div class="idx-chip-row">${missChips}${dupChips}</div>` : ""}
    ${ri ? `<div class="replay-line">↩ 本次从检查点恢复：checkpoint frontier=<b>${ri.checkpoint_frontier}</b>，
        台账 frontier=<b>${ri.ledger_frontier}</b>，从位置 <b>#${ri.replay_from_position}</b>
        ${ri.replay_from_index != null ? `(idx=${escapeHtml(ri.replay_from_index)})` : ""} 重放；
        <b>${ri.already_committed_ahead_count}</b> 项超前提交被 Sink 幂等去重。
        Sink 实际去重 <b>${s.operators.find((o) => o.id === "sink")?.dedup_hits ?? 0}</b> 次。</div>` : ""}`;
}

function renderCheckpoint(s) {
  const cp = s.checkpoint;
  const body = $("checkpointBody");
  if (!cp) {
    body.innerHTML = '<p class="muted">尚未生成检查点。</p>';
    return;
  }
  body.innerHTML = `
    <dl class="kv">
      <dt>检查点文件</dt><dd class="mono">${escapeHtml(cp.path || "")}</dd>
      <dt>run_id</dt><dd class="mono">${escapeHtml(cp.run_id || "—")}</dd>
      <dt>提交前沿</dt><dd><b>${cp.frontier_live ?? cp.frontier}</b> / ${cp.total ?? (cp.indices || []).length}
        <span class="muted">(连续提交前缀，恢复重放起点)</span></dd>
      <dt>保存次数</dt><dd>${cp.checkpoint_saves ?? cp.saves ?? "—"} <span class="muted">· 步长 ${cp.checkpoint_every}</span></dd>
      <dt>最近保存</dt><dd>${fmtTime(cp.last_saved_wall || cp.updated_wall)} ${cp.persisted_only ? '<span class="muted">(磁盘)</span>' : ""}</dd>
      <dt>受控索引数</dt><dd>${(cp.indices || []).length}</dd>
    </dl>`;
}

function renderCancel(s) {
  const body = $("cancelBody");
  const rep = s.cancel_report;
  if (!rep) {
    body.innerHTML = '<p class="muted">取消后在此显示每个阻塞 worker 被唤醒并退出的延迟。</p>';
    return;
  }
  const max = rep.max_worker_wake_latency_ms || 1;
  const rows = Object.entries(rep.worker_wake_latency_ms || {}).map(([name, ms]) => {
    const label = s.topology.nodes.find((n) => n.id === name)?.label || name;
    const w = Math.max(4, Math.min(100, (ms / Math.max(500, max)) * 100));
    return `<div class="wake-row">
      <span>${escapeHtml(label)}</span>
      <span class="wake-bar"><i style="width:${w}%"></i></span>
      <span class="wake-val">${ms} ms</span>
    </div>`;
  }).join("");
  body.innerHTML = `
    <p class="cancel-summary ${rep.ok ? "bn-ok" : ""}">
      ${rep.ok ? "✓" : "⚠"} 取消信号到全部 worker 停止：<b>${rep.cancel_to_all_stopped_ms} ms</b>
      ${rep.alive.length ? `；仍存活：${escapeHtml(rep.alive.join(","))}` : "（无残留线程）"}
    </p>
    ${rows || '<p class="muted">没有 worker 记录到唤醒延迟。</p>'}
    <p class="muted" style="font-size:11.5px;margin:8px 0 0">延迟 = worker 从 CancelToken 触发到退出等待点的时间；通道 Condition.notify_all 与取消 Event 负责唤醒。</p>`;
}

let indexGridSig = "";
function renderIndexGrid(s) {
  const grid = $("indexGrid");
  const v = s.verification;
  const missing = new Set(v?.missing_indices || []);
  const dup = new Set(v?.duplicate_indices || []);
  const sig = (s.index_states || []).map((x) => x.committed ? "1" : "0").join("")
    + `|${[...missing].join(",")}|${[...dup].join(",")}|${s.planned_count}`;
  if (sig === indexGridSig) return;
  indexGridSig = sig;

  if (!s.index_states.length) {
    grid.innerHTML = '<p class="muted">—</p>';
    return;
  }
  const MAX = 2000;
  const shown = s.index_states.slice(0, MAX);
  grid.innerHTML = shown.map((x) => {
    let cls = "idx";
    if (dup.has(x.idx)) cls += " dup";
    else if (missing.has(x.idx)) cls += " missing";
    else if (x.committed) cls += " done";
    return `<span class="${cls}" title="position=${x.position}, idx=${x.idx}">${escapeHtml(x.idx)}</span>`;
  }).join("") + (s.index_states.length > MAX ? `<p class="muted">仅显示前 ${MAX} 项…</p>` : "");
}

function renderOutputs(s) {
  const tb = $("outputsBody");
  $("outputCount").textContent = s.verification ? `台账共 ${s.verification.output_records} 条` : "";
  if (!s.recent_outputs.length) {
    tb.innerHTML = '<tr><td colspan="5" class="muted">暂无</td></tr>';
    return;
  }
  tb.innerHTML = s.recent_outputs.map((r) => `
    <tr>
      <td class="mono">${r.position ?? "—"}</td>
      <td class="mono"><b>${escapeHtml(r.idx)}</b></td>
      <td>${r.branch ? `<span class="branch-pill branch-${r.branch}">${r.branch}</span>` : "—"}</td>
      <td class="mono">${escapeHtml(r.processed_by || "—")}</td>
      <td class="mono">${fmtTime(r.committed_wall)}</td>
    </tr>`).join("");
}

function renderEvents(s) {
  const log = $("eventsLog");
  log.innerHTML = s.events.map((e) => `
    <div class="event-line">
      <span class="event-time">${fmtTime(e.wall)}</span>
      <span class="event-level ${e.level}">${{ info: "信息", warn: "警告", error: "错误" }[e.level] || e.level}</span>
      <span class="event-msg">${escapeHtml(e.message)}</span>
    </div>`).join("");
}

function renderSelftestResult(result) {
  const body = $("selftestBody");
  if (!result) {
    body.innerHTML = '<p class="muted">启动独立子进程，在第 9 条输出 fsync 后 <code>os._exit(42)</code> 模拟被 kill，再用第二个进程从检查点 + 台账恢复并独立核对。点击上方按钮运行。</p>';
    return;
  }
  const v = result.disk_verification || {};
  const rr = result.recover_report || {};
  body.innerHTML = `
    <div class="verify-hero">
      <div class="verify-mark ${result.passed ? "pass" : "fail"}">${result.passed ? "✓" : "✕"}</div>
      <div>
        <div class="verify-title">${result.passed ? "硬崩溃恢复自检通过" : "自检失败"}</div>
        <div class="verify-sub">子进程退出码 ${result.child_exit_code}（预期 42）· 恢复进程退出码 ${result.recover_exit_code} · 磁盘台账 ${result.disk_record_count} 条 · 缺失 ${v.missing_indices?.length ?? "?"} · 重复 ${v.duplicate_indices?.length ?? "?"}</div>
      </div>
    </div>
    <div class="replay-line">恢复报告：terminal=${escapeHtml(rr.terminal_state)}，Sink 去重 ${rr.dedup_hits ?? "—"} 次，checkpoint frontier=${rr.checkpoint_frontier ?? "—"}。工作目录 <code>${escapeHtml(result.workdir)}</code></div>
    <pre>${escapeHtml(JSON.stringify(result, null, 2))}</pre>`;
}

/* ------------------------------------------------------------- form */
let formInitialized = false;
function initForm(cfg) {
  if (formInitialized || !cfg) return;
  $("cfgStart").value = cfg.start;
  $("cfgCount").value = cfg.count;
  $("cfgIndices").value = cfg.indices ? cfg.indices.join(",") : "";
  $("cfgSlow").value = cfg.slow_delay_ms;
  $("cfgFast").value = cfg.fast_delay_ms;
  $("cfgInterval").value = cfg.inject_interval_ms;
  $("cfgCap").value = cfg.cap_branch;
  $("cfgEvery").value = cfg.checkpoint_every;
  $("cfgRouting").value = cfg.routing;
  syncSliderLabels();
  formInitialized = true;
}
function syncSliderLabels() {
  $("slowVal").textContent = `${$("cfgSlow").value} ms`;
  $("fastVal").textContent = `${$("cfgFast").value} ms`;
  $("intervalVal").textContent = `${$("cfgInterval").value} ms`;
}
["cfgSlow", "cfgFast", "cfgInterval"].forEach((id) => $(id).addEventListener("input", syncSliderLabels));

function readConfigPatch() {
  const text = $("cfgIndices").value.trim();
  const patch = {
    slow_delay_ms: Number($("cfgSlow").value),
    fast_delay_ms: Number($("cfgFast").value),
    inject_interval_ms: Number($("cfgInterval").value),
    cap_branch: Number($("cfgCap").value),
    checkpoint_every: Number($("cfgEvery").value),
    routing: $("cfgRouting").value,
  };
  if (text) {
    const parts = text.split(/[\s,，;；]+/).filter(Boolean);
    const idxs = parts.map((x) => {
      if (!/^[+-]?\d+$/.test(x)) throw new Error(`无法解析索引：${x}`);
      return Number(x);
    });
    const dup = idxs.filter((x, i) => idxs.indexOf(x) !== i);
    if (dup.length) throw new Error(`输入索引必须唯一，重复：${[...new Set(dup)].slice(0, 8).join(",")}`);
    patch.indices = idxs;
  } else {
    patch.indices = null;
    patch.start = Number($("cfgStart").value);
    patch.count = Number($("cfgCount").value);
  }
  return patch;
}

function showActionError(e) {
  $("actionError").textContent = e ? `⚠ ${e.message || e}` : "";
}

/* ------------------------------------------------------------ actions */
$("btnStart").addEventListener("click", async () => {
  try {
    showActionError(null);
    const patch = readConfigPatch();
    indexGridSig = "";
    await api("/api/start", patch);
    refresh();
  } catch (e) { showActionError(e); }
});
$("btnCancel").addEventListener("click", async () => {
  try { showActionError(null); await api("/api/cancel", { reason: "页面按钮取消" }); refresh(); }
  catch (e) { showActionError(e); }
});
$("btnCheckpoint").addEventListener("click", async () => {
  try { showActionError(null); await api("/api/checkpoint", {}); refresh(); }
  catch (e) { showActionError(e); }
});
$("btnRestore").addEventListener("click", async () => {
  try {
    showActionError(null);
    indexGridSig = "";
    await api("/api/restore", {});
    refresh();
  } catch (e) { showActionError(e); }
});
$("btnReset").addEventListener("click", async () => {
  try {
    showActionError(null);
    await api("/api/reset", {});
    indexGridSig = "";
    $("verifyBody").innerHTML = '<p class="muted">尚无输出。</p>';
    refresh();
  } catch (e) { showActionError(e); }
});
$("btnVerify").addEventListener("click", async () => {
  try {
    showActionError(null);
    const r = await api("/api/verify", {});
    if (lastState) { lastState.verification = r.verification; renderVerify(lastState); renderIndexGrid(lastState); }
  } catch (e) { showActionError(e); }
});
$("btnSelftest").addEventListener("click", async () => {
  const btn = $("btnSelftest");
  const body = $("selftestBody");
  btn.disabled = true;
  body.innerHTML = '<p><span class="spinner"></span>正在运行真实子进程崩溃/恢复场景，约需 5–15 秒…</p>';
  try {
    const r = await api("/api/selftest/crash-recovery", {});
    renderSelftestResult(r.result);
  } catch (e) {
    // 服务端在自检失败时返回 500 + result; fetch helper throws but keeps data?
    body.innerHTML = `<p class="verify-title" style="color:var(--critical-text)">自检调用失败：${escapeHtml(e.message)}</p>`;
  } finally {
    btn.disabled = false;
  }
});

/* ------------------------------------------------------------- polling */
let lastState = null;
async function refresh() {
  try {
    const s = await api("/api/state");
    lastState = s;
    if (!graphRefs.built) {
      buildGraph(s.topology);
      graphRefs.built = true;
    }
    initForm(s.config);
    renderStatus(s);
    renderGraph(s);
    renderBottleneck(s);
    renderVerify(s);
    renderCheckpoint(s);
    renderCancel(s);
    renderIndexGrid(s);
    renderOutputs(s);
    renderEvents(s);
    $("apiBase").textContent = location.origin;
  } catch (e) {
    $("runStateText").textContent = "无法连接服务";
  }
}

api("/api/selftest/crash-recovery").then((r) => {
  if (r.last) renderSelftestResult(r.last);
}).catch(() => {});

refresh();
setInterval(refresh, 700);
