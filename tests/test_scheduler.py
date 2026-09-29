"""调度器测试：资源/标签/规格约束、拒绝原因区分、稳定可复现。"""

from __future__ import annotations

import logging

from orchestrator.models import (
    Node,
    NodeState,
    RejectReason,
    Resources,
    Spec,
    Task,
)
from orchestrator.scheduler import Scheduler


def make_nodes() -> list[Node]:
    return [
        Node("node-a", Resources(1000, 512), labels={"zone": "a", "disk": "ssd"},
             supported_versions=frozenset({1})),
        Node("node-b", Resources(2000, 1024), labels={"zone": "b", "disk": "ssd"},
             supported_versions=frozenset({1, 2})),
        Node("node-c", Resources(500, 256), labels={"zone": "a", "disk": "hdd"},
             supported_versions=frozenset({1})),
    ]


def test_selects_smallest_node_id_that_fits(caplog) -> None:
    caplog.set_level(logging.INFO, logger="orchestrator.scheduler")
    sched = Scheduler()
    task = Task("t1", Resources(800, 100), node_selector={"zone": "a"})

    decision = sched.evaluate(task, make_nodes())

    assert decision.accepted
    assert decision.node_id == "node-a"  # node-c 在 zone=a 但装不下
    assert decision.evaluated_nodes == ("node-a", "node-b", "node-c")
    # 日志必须打印调度输入、判定依据与最终归属
    joined = "\n".join(r.message for r in caplog.records)
    assert "调度输入" in joined and "判定依据" in joined and "最终归属" in joined


def test_capacity_rejection_when_selector_matches_but_too_large() -> None:
    sched = Scheduler()
    # zone=a 有两个节点（a:1000 / c:500），但都装不下 4000
    task = Task("big", Resources(4000, 100), node_selector={"zone": "a"})

    decision = sched.evaluate(task, make_nodes())

    assert not decision.accepted
    assert decision.reason == RejectReason.NO_CAPACITY
    assert "资源不足" in decision.detail


def test_constraint_rejection_for_unknown_label_value() -> None:
    sched = Scheduler()
    task = Task("t", Resources(10, 10), node_selector={"zone": "zz"})

    decision = sched.evaluate(task, make_nodes())

    assert not decision.accepted
    assert decision.reason == RejectReason.CONSTRAINT_CONFLICT
    assert "标签冲突" in decision.detail


def test_constraint_rejection_for_unsupported_spec_version() -> None:
    sched = Scheduler()
    # zone=b 的 node-b 支持 v2，但要求 zone=a -> 所有 zone=a 节点都只支持 v1
    task = Task("t", Resources(10, 10), node_selector={"zone": "a"}, spec=Spec(2))

    decision = sched.evaluate(task, make_nodes())

    assert decision.reason == RejectReason.CONSTRAINT_CONFLICT
    assert "v2" in decision.detail


def test_spec_version_supported_on_matching_node() -> None:
    sched = Scheduler()
    task = Task("t", Resources(10, 10), node_selector={"zone": "b"}, spec=Spec(2))

    decision = sched.evaluate(task, make_nodes())

    assert decision.accepted and decision.node_id == "node-b"


def test_no_available_node_when_all_offline() -> None:
    sched = Scheduler()
    nodes = make_nodes()
    for n in nodes:
        n.state = NodeState.DOWN

    decision = sched.evaluate(Task("t", Resources(1, 1)), nodes)

    assert decision.reason == RejectReason.NO_AVAILABLE_NODE


def test_cordoned_node_not_selected_but_existing_one_runs() -> None:
    sched = Scheduler()
    nodes = make_nodes()
    nodes[0].state = NodeState.CORDONED  # node-a

    decision = sched.evaluate(Task("t", Resources(10, 10), {"zone": "a"}), nodes)

    assert decision.accepted and decision.node_id == "node-c"


def test_repeated_batch_scheduling_is_stable_and_reproducible() -> None:
    """同一批任务、同一批节点，打乱顺序重复评估，归属必须完全一致。"""
    sched = Scheduler()
    tasks = [
        Task("t1", Resources(900, 100), {"zone": "a"}),    # 只能去 node-a
        Task("t2", Resources(1500, 100), {"zone": "b"}),   # 只能去 node-b
        Task("t3", Resources(400, 100), {"zone": "a"}),    # a 剩 100 装不下 -> c(500)
        Task("t4", Resources(400, 100), {"disk": "hdd"}),  # 仅 node-c 是 hdd
    ]

    def run_batch(order: list[Node]) -> dict[str, str | None]:
        # 每个批次使用全新节点副本，模拟“同一批任务多次调度”
        import copy

        nodes = copy.deepcopy(order)
        placement: dict[str, str | None] = {}
        for task in sorted(tasks, key=lambda t: (t.submission_seq, t.task_id)):
            d = sched.evaluate(task, nodes)
            placement[task.task_id] = d.node_id
            if d.accepted:
                nodes = {n.node_id: n for n in nodes}
                nodes[d.node_id].allocations[task.task_id] = task.request
                nodes = list(nodes.values())
        return placement

    first = run_batch(make_nodes())
    # 打乱节点输入顺序，结果必须一致（评估前按 node_id 排序）
    second = run_batch(list(reversed(make_nodes())))

    # t4 只匹配 node-c（hdd），但 t3 已占用 400/500，剩余不足 -> 确定性拒绝
    assert first == second
    assert first == {"t1": "node-a", "t2": "node-b", "t3": "node-c", "t4": None}

    # 同一批次跑第三遍，结果依然一致（可复现）
    third = run_batch(make_nodes())
    assert third == first


def test_decision_detail_explains_every_rejected_node() -> None:
    sched = Scheduler()
    nodes = make_nodes()
    nodes[1].state = NodeState.CORDONED
    decision = sched.evaluate(
        Task("t", Resources(4000, 10), node_selector={"zone": "b"}), nodes
    )
    # node-b 被封锁后没有 ONLINE 节点满足选择器；无节点硬匹配 -> 约束冲突路径不触发，
    # 因为在线节点里没有任何 zone=b -> CONSTRAINT_CONFLICT；node-b 不参与评估说明中需可见
    assert decision.reason == RejectReason.CONSTRAINT_CONFLICT
