"""Deterministic resource- and label-aware scheduler.

``select_node`` is a pure function of (spec, nodes snapshot):

* candidate nodes are iterated in ascending ``node_id`` order;
* the first feasible node wins (lexicographic tie-break);
* every candidate gets an explainable :class:`NodeEvaluation`;
* if nothing fits, the rejection reason distinguishes:
  - NODE_UNAVAILABLE / LABEL / CAPABILITY / RESOURCE conflicts when at
    least one candidate existed (the first-blocking reason from the
    ordered evaluation), and
  - NO_CANDIDATE_NODE when the cluster has no registered node.

Because ordering and predicates are fixed, the same batch submitted to
the same cluster state always produces the same placements.
"""

from __future__ import annotations

from .compatibility import check_placement
from .models import (
    Node,
    NodeEvaluation,
    RejectReason,
    ScheduleDecision,
    TaskSpec,
)
from .trace import TraceRecorder


class Scheduler:
    def __init__(self, trace: TraceRecorder | None = None) -> None:
        self._trace = trace

    def select_node(self, spec: TaskSpec, nodes: dict[str, Node]) -> ScheduleDecision:
        ordered_ids = sorted(nodes)
        evaluations: list[NodeEvaluation] = []

        for node_id in ordered_ids:
            node = nodes[node_id]
            verdict = check_placement(node, spec)
            evaluations.append(
                NodeEvaluation(
                    node_id=node_id,
                    feasible=verdict.compatible,
                    reject_reason=verdict.reason,
                    detail=verdict.detail,
                )
            )

        feasible = [e for e in evaluations if e.feasible]
        if feasible:
            chosen = feasible[0].node_id
            decision = ScheduleDecision(
                task_id="",
                accepted=True,
                node_id=chosen,
                evaluations=tuple(evaluations),
            )
        else:
            if not evaluations:
                reason = RejectReason.NO_CANDIDATE_NODE
                detail = "no nodes registered in the cluster"
            else:
                # First-blocking reason in deterministic node order makes the
                # rejection stable even when multiple conflicts coexist.
                first = evaluations[0]
                reason = first.reject_reason or RejectReason.NO_CANDIDATE_NODE
                detail = first.detail
            decision = ScheduleDecision(
                task_id="",
                accepted=False,
                reject_reason=reason,
                reject_detail=detail,
                evaluations=tuple(evaluations),
            )

        if self._trace is not None:
            self._trace.record_decision(spec, ordered_ids, decision)
        return decision
