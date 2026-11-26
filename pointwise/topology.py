"""拓扑(依赖关系)定义与 pointwise 输出核对。

逻辑 DAG::

    source -> ch_src -> dispatcher -> ch_fast -> op_fast -\\
                                      \\-> ch_slow -> op_slow -> ch_merged -> sink

通道在页面上作为独立节点(队列条)渲染, 这样可以明确指出“是哪个通道导致
停顿”。
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

# 节点元信息(不含布局坐标; 坐标属于前端表现层)
NODES: List[Dict[str, str]] = [
    {"id": "source", "label": "输入源", "sub": "受控索引 Source", "kind": "operator"},
    {"id": "ch_src", "label": "ch_src", "sub": "有界通道", "kind": "channel"},
    {"id": "dispatcher", "label": "分发器", "sub": "Dispatcher", "kind": "operator"},
    {"id": "ch_fast", "label": "ch_fast", "sub": "快分支通道", "kind": "channel"},
    {"id": "op_fast", "label": "快分支算子", "sub": "Blocking · 低延迟", "kind": "operator"},
    {"id": "ch_slow", "label": "ch_slow", "sub": "慢分支通道", "kind": "channel"},
    {"id": "op_slow", "label": "慢分支算子", "sub": "Blocking · 高延迟", "kind": "operator"},
    {"id": "ch_merged", "label": "ch_merged", "sub": "汇合通道", "kind": "channel"},
    {"id": "sink", "label": "幂等 Sink", "sub": "台账 + 检查点", "kind": "operator"},
]

EDGES: List[Dict[str, str]] = [
    {"from": "source", "to": "ch_src"},
    {"from": "ch_src", "to": "dispatcher"},
    {"from": "dispatcher", "to": "ch_fast"},
    {"from": "ch_fast", "to": "op_fast"},
    {"from": "dispatcher", "to": "ch_slow"},
    {"from": "ch_slow", "to": "op_slow"},
    {"from": "op_fast", "to": "ch_merged"},
    {"from": "op_slow", "to": "ch_merged"},
    {"from": "ch_merged", "to": "sink"},
]

# 通道 -> 下游消费算子 / 上游生产算子(瓶颈归因用)
CHANNEL_CONSUMER: Dict[str, str] = {
    "ch_src": "dispatcher",
    "ch_fast": "op_fast",
    "ch_slow": "op_slow",
    "ch_merged": "sink",
}
CHANNEL_PRODUCER: Dict[str, str] = {
    "ch_src": "source",
    "ch_fast": "dispatcher",
    "ch_slow": "dispatcher",
    "ch_merged": "op_fast/op_slow",
}

NODE_LABELS: Dict[str, str] = {n["id"]: n["label"] for n in NODES}


def topology() -> Dict[str, Any]:
    return {
        "nodes": NODES,
        "edges": EDGES,
        "channel_consumer": CHANNEL_CONSUMER,
        "channel_producer": CHANNEL_PRODUCER,
    }


def verify_indices(planned: List[int], records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """用受控输入索引核对输出: 不跳项、不重复、无多余。

    同时按 ``position`` 与 ``idx`` 两个维度统计(正常情况下二者一一对应)。
    该函数直接对原始台账行计数, 不依赖 Sink 内存去重集合, 因此是独立核对。
    """
    expected_positions = list(range(len(planned)))
    idx_by_position = {p: planned[p] for p in expected_positions}
    expected_idx = list(planned)

    pos_counter: Counter = Counter()
    idx_counter: Counter = Counter()
    extra_idx: List[Any] = []
    extra_positions: List[Any] = []

    valid_positions = set(expected_positions)
    valid_idx = set(expected_idx)

    for r in records:
        if "position" in r:
            p = r["position"]
            pos_counter[p] += 1
            if p not in valid_positions:
                extra_positions.append(p)
        if "idx" in r:
            i = r["idx"]
            idx_counter[i] += 1
            if i not in valid_idx:
                extra_idx.append(i)

    missing_positions = [p for p in expected_positions if pos_counter[p] == 0]
    duplicate_positions = sorted(p for p, c in pos_counter.items() if c > 1 and p in valid_positions)
    missing_idx = [idx_by_position[p] for p in missing_positions]
    duplicate_idx = sorted(i for i, c in idx_counter.items() if c > 1 and i in valid_idx)

    total = len(records)
    unique_idx = len(idx_counter)

    ok = (
        not missing_positions
        and not duplicate_positions
        and not extra_positions
        and not duplicate_idx
        and not extra_idx
        and total == len(planned)
    )

    return {
        "ok": ok,
        "expected_count": len(planned),
        "output_records": total,
        "unique_index_count": unique_idx,
        "missing_positions": missing_positions,
        "missing_indices": missing_idx,
        "duplicate_positions": duplicate_positions,
        "duplicate_indices": duplicate_idx,
        "extra_positions": sorted(set(extra_positions)),
        "extra_indices": sorted(set(extra_idx), key=lambda x: str(x)),
        "checked_wall_ms": None,
    }
