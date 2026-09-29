"""Upgrade compatibility: old tasks preserved, explainable verdicts."""

from __future__ import annotations

from orchestrator.compatibility import check_placement, check_upgraded_task
from orchestrator.models import (
    Node,
    NodeState,
    RejectReason,
    Resources,
    Task,
    TaskSpec,
    TaskState,
)


def _node(**over):
    base = dict(
        node_id="n1",
        capacity=Resources(8, 8192, 1),
        used=Resources(0, 0, 0),
        labels={},
        capabilities={},
        state=NodeState.ONLINE,
    )
    base.update(over)
    return Node(**base)


def _running_task(spec, node_id="n1", gen=1):
    return Task(
        task_id="t1",
        spec=spec,
        state=TaskState.RUNNING,
        submitted_at=1.0,
        node_id=node_id,
        assignment_generation=gen,
    )


def test_new_spec_fitting_same_node_is_compatible():
    node = _node()
    task = _running_task(TaskSpec("img:v1"))
    verdict = check_upgraded_task(task, TaskSpec("img:v2", generation=2), node)
    assert verdict.compatible is True


def test_new_spec_needing_missing_capability_is_incompatible_but_old_task_kept():
    node = _node(capabilities={})
    old_spec = TaskSpec("img:v1")
    task = _running_task(old_spec)
    new_spec = TaskSpec("img:v2", generation=2, required_capabilities=frozenset(["cuda"]))
    verdict = check_upgraded_task(task, new_spec, node)
    assert verdict.compatible is False
    assert verdict.reason == RejectReason.CAPABILITY_INCOMPATIBLE
    # callers rely on the old task object staying put
    assert task.spec is old_spec
    assert task.node_id == "n1"
    assert task.assignment_generation == 1


def test_spec_downgrade_refused():
    node = _node()
    task = _running_task(TaskSpec("img:v2", generation=2), gen=2)
    verdict = check_upgraded_task(task, TaskSpec("img:v1", generation=1), node)
    assert verdict.compatible is False
    assert "older than admitted" in verdict.detail


def test_upgrade_target_wrong_node_rejected():
    node = _node(node_id="n9")
    task = _running_task(TaskSpec("img"), node_id="n1")
    verdict = check_upgraded_task(task, TaskSpec("img", generation=2), node)
    assert verdict.compatible is False
    assert "lives on" in verdict.detail


def test_drained_node_rejects_even_for_existing_task_upgrade():
    node = _node(state=NodeState.DRAINING)
    task = _running_task(TaskSpec("img"))
    verdict = check_upgraded_task(task, TaskSpec("img", generation=2), node)
    assert verdict.compatible is False
    assert verdict.reason == RejectReason.NODE_UNAVAILABLE


def test_node_capability_upgrade_does_not_move_running_task():
    # A node losing "cuda" must not silently invalidate or relocate a task;
    # placement compatibility is independently explainable.
    node = _node(capabilities={"cuda": 1})
    verdict_before = check_placement(node, TaskSpec("img", required_capabilities=frozenset(["cuda"])))
    assert verdict_before.compatible is True
    node.capabilities = {}
    verdict_after = check_placement(node, TaskSpec("img", required_capabilities=frozenset(["cuda"])))
    assert verdict_after.compatible is False
    assert verdict_after.reason == RejectReason.CAPABILITY_INCOMPATIBLE


def test_controlplane_upgrade_preserves_assignment_when_incompatible(cp):
    cp.register_node("n1", Resources(2, 2048, 0), capabilities={})
    decision = cp.submit_task("t1", TaskSpec("img:v1", generation=1))
    assert decision.accepted
    verdict = cp.upgrade_task_spec(
        "t1", TaskSpec("img:v2", generation=2, required_capabilities=frozenset(["cuda"]))
    )
    assert verdict.compatible is False
    task = cp.get_task("t1")
    assert task.node_id == "n1"
    assert task.spec.image == "img:v1"
    assert task.assignment_generation == 1
