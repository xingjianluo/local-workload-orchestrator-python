"""Scheduler behaviour: resources, labels, capabilities, distinct
rejection reasons, and deterministic repeatable placement."""

from __future__ import annotations

from orchestrator.models import (
    Node,
    NodeState,
    RejectReason,
    Resources,
    TaskSpec,
)
from orchestrator.scheduler import Scheduler
from orchestrator.trace import TraceRecorder


def _node(node_id, cpu=8, mem=8192, gpu=0, labels=None, caps=None, state=NodeState.ONLINE, used=None):
    return Node(
        node_id=node_id,
        capacity=Resources(cpu, mem, gpu),
        used=used or Resources(0, 0, 0),
        labels=labels or {},
        capabilities=caps or {},
        state=state,
    )


def test_places_on_lexicographically_first_fitting_node():
    sched = Scheduler()
    nodes = {
        "n2": _node("n2"),
        "n1": _node("n1"),
        "n3": _node("n3"),
    }
    decision = sched.select_node(TaskSpec("img", Resources(1, 1024, 0)), nodes)
    assert decision.accepted is True
    assert decision.node_id == "n1"


def test_skips_full_node_and_uses_next():
    sched = Scheduler()
    nodes = {"n1": _node("n1", cpu=2, used=Resources(2, 2048, 0)), "n2": _node("n2")}
    decision = sched.select_node(TaskSpec("img", Resources(3, 1024, 0)), nodes)
    assert decision.accepted is True
    assert decision.node_id == "n2"
    evals = {e.node_id: e for e in decision.evaluations}
    assert evals["n1"].feasible is False
    assert evals["n1"].reject_reason == RejectReason.INSUFFICIENT_RESOURCES


def test_reject_insufficient_resources_when_no_node_fits():
    sched = Scheduler()
    decision = sched.select_node(
        TaskSpec("img", Resources(99, 99, 0)), {"n1": _node("n1")}
    )
    assert decision.accepted is False
    assert decision.reject_reason == RejectReason.INSUFFICIENT_RESOURCES


def test_reject_unavailable_node():
    sched = Scheduler()
    nodes = {"n1": _node("n1", state=NodeState.DRAINING)}
    decision = sched.select_node(TaskSpec("img"), nodes)
    assert decision.reject_reason == RejectReason.NODE_UNAVAILABLE


def test_reject_label_conflict_distinct_from_other_reasons():
    sched = Scheduler()
    nodes = {"n1": _node("n1", labels={"zone": "b"})}
    decision = sched.select_node(
        TaskSpec("img", required_labels=frozenset([("zone", "a")])), nodes
    )
    assert decision.reject_reason == RejectReason.LABEL_CONSTRAINT_CONFLICT
    assert "zone" in decision.reject_detail


def test_reject_missing_capability():
    sched = Scheduler()
    nodes = {"n1": _node("n1", caps={})}
    decision = sched.select_node(
        TaskSpec("img", required_capabilities=frozenset(["cuda"])), nodes
    )
    assert decision.reject_reason == RejectReason.CAPABILITY_INCOMPATIBLE


def test_capability_match_accepts():
    sched = Scheduler()
    nodes = {"n1": _node("n1", caps={"cuda": 1})}
    decision = sched.select_node(
        TaskSpec("img", required_capabilities=frozenset(["cuda"])), nodes
    )
    assert decision.accepted is True


def test_no_candidate_node_reason():
    decision = Scheduler().select_node(TaskSpec("img"), {})
    assert decision.accepted is False
    assert decision.reject_reason == RejectReason.NO_CANDIDATE_NODE


def test_first_blocking_reason_is_stable_in_node_order():
    sched = Scheduler()
    nodes = {
        "n1": _node("n1", state=NodeState.DRAINING),
        "n2": _node("n2", labels={"zone": "x"}),
    }
    spec = TaskSpec("img", required_labels=frozenset([("zone", "a")]))
    decision = sched.select_node(spec, nodes)
    assert decision.reject_reason == RejectReason.NODE_UNAVAILABLE


def test_repeated_decisions_are_reproducible():
    sched = Scheduler()
    nodes_a = {"n2": _node("n2"), "n1": _node("n1")}
    nodes_b = {"n1": _node("n1"), "n2": _node("n2")}
    spec = TaskSpec("img", Resources(1, 1024, 0))
    picks = {
        sched.select_node(spec, nodes_a).node_id,
        sched.select_node(spec, nodes_b).node_id,
    }
    assert picks == {"n1"}


def test_trace_records_input_evidence_and_assignment():
    trace = TraceRecorder()
    sched = Scheduler(trace)
    nodes = {"n1": _node("n1", cpu=1, used=Resources(1, 0, 0)), "n2": _node("n2")}
    sched.select_node(TaskSpec("img", Resources(2, 0, 0)), nodes)
    assert len(trace.records) == 1
    rec = trace.records[0]
    assert rec["candidate_order"] == ["n1", "n2"]
    assert rec["accepted"] is True
    assert rec["node_id"] == "n2"
    reasons = {e["node_id"]: e["reject_reason"] for e in rec["evaluations"]}
    assert reasons["n1"] == RejectReason.INSUFFICIENT_RESOURCES.value
