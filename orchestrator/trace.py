"""Scheduling trace/audit logging.

Every scheduling decision records the input task, the ordered candidate
nodes, per-node evaluation evidence and the final placement, so unit
test logs explain *why* a task landed where it did (or why rejected).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .models import ScheduleDecision, TaskSpec

logger = logging.getLogger("orchestrator.scheduler")


@dataclass
class TraceRecorder:
    records: list[dict] = field(default_factory=list)

    def record_decision(
        self,
        spec: TaskSpec,
        candidate_node_ids: list[str],
        decision: ScheduleDecision,
    ) -> None:
        need = spec.resources
        record: dict = {
            "task_image": spec.image,
            "generation": spec.generation,
            "need": {
                "cpu": need.cpu,
                "memory_mb": need.memory_mb,
                "gpu": need.gpu,
            },
            "required_labels": sorted(spec.required_labels),
            "required_capabilities": sorted(spec.required_capabilities),
            "candidate_order": list(candidate_node_ids),
            "evaluations": [
                {
                    "node_id": e.node_id,
                    "feasible": e.feasible,
                    "reject_reason": e.reject_reason.value if e.reject_reason else None,
                    "detail": e.detail,
                }
                for e in decision.evaluations
            ],
            "accepted": decision.accepted,
            "node_id": decision.node_id,
            "reject_reason": decision.reject_reason.value
            if decision.reject_reason
            else None,
            "reject_detail": decision.reject_detail,
        }
        self.records.append(record)

        lines = [
            "[schedule] image=%s gen=%s need(cpu=%s,mem=%s,gpu=%s) "
            "labels=%s caps=%s candidates=%s"
            % (
                spec.image,
                spec.generation,
                need.cpu,
                need.memory_mb,
                need.gpu,
                record["required_labels"],
                record["required_capabilities"],
                candidate_node_ids,
            )
        ]
        for ev in decision.evaluations:
            lines.append(
                "    - node=%s feasible=%s reason=%s detail=%s"
                % (
                    ev.node_id,
                    ev.feasible,
                    ev.reject_reason.value if ev.reject_reason else "-",
                    ev.detail,
                )
            )
        verdict = (
            f"ACCEPTED -> {decision.node_id}"
            if decision.accepted
            else f"REJECTED [{decision.reject_reason.value if decision.reject_reason else '-'}] "
            f"{decision.reject_detail}"
        )
        lines.append("    => " + verdict)
        logger.info("\n".join(lines))

    def reset(self) -> None:
        self.records.clear()
