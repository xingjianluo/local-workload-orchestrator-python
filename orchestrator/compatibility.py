"""Capability / spec compatibility across upgrades.

Old tasks must survive node capability and task-spec upgrades. Each
check returns an explainable verdict rather than silently re-interpreting
an old task under new rules.

Compatibility rules (all must hold)
-----------------------------------
1. Node must be ONLINE (DRAINING/UNKNOWN are not eligible).
2. Every ``required_labels`` entry (key, value) must match the node label.
3. Every required capability name must exist on the node and the node's
   capability level must be >= the required level. A capability name
   without a required level defaults to level 1 (``TaskSpec`` stores
   named capabilities only, so this module treats membership as level 1).
4. Free capacity (capacity - used) must dominate the spec resources.

Upgrade semantics
-----------------
A task already running was admitted under *its own* spec generation and
the node generation at assignment time. An upgraded spec is a *new*
admission decision: it is checked against the node's current state and
must never silently evict or relocate the old task. Callers keep the old
assignment whenever the verdict is incompatible.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import Node, NodeState, RejectReason, Resources, Task, TaskSpec


@dataclass(frozen=True)
class CompatibilityVerdict:
    compatible: bool
    reason: RejectReason | None = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.compatible


def _free(node: Node) -> Resources:
    return Resources(
        cpu=node.capacity.cpu - node.used.cpu,
        memory_mb=node.capacity.memory_mb - node.used.memory_mb,
        gpu=node.capacity.gpu - node.used.gpu,
    )


def _fits(free: Resources, need: Resources) -> bool:
    return free.cpu >= need.cpu and free.memory_mb >= need.memory_mb and free.gpu >= need.gpu


def check_placement(node: Node, spec: TaskSpec) -> CompatibilityVerdict:
    """Check one task spec against one node's current state."""
    if node.state is not NodeState.ONLINE:
        return CompatibilityVerdict(
            False,
            RejectReason.NODE_UNAVAILABLE,
            f"node {node.node_id} state={node.state.value} is not ONLINE",
        )

    for key, value in sorted(spec.required_labels):
        actual = node.labels.get(key)
        if actual != value:
            return CompatibilityVerdict(
                False,
                RejectReason.LABEL_CONSTRAINT_CONFLICT,
                f"node {node.node_id} label {key!r}={actual!r}, required {value!r}",
            )

    missing = sorted(
        cap for cap in spec.required_capabilities if cap not in node.capabilities
    )
    if missing:
        return CompatibilityVerdict(
            False,
            RejectReason.CAPABILITY_INCOMPATIBLE,
            f"node {node.node_id} missing capabilities {missing}",
        )

    free = _free(node)
    if not _fits(free, spec.resources):
        return CompatibilityVerdict(
            False,
            RejectReason.INSUFFICIENT_RESOURCES,
            f"node {node.node_id} free=({free.cpu},{free.memory_mb},{free.gpu}) "
            f"cannot fit need=({spec.resources.cpu},{spec.resources.memory_mb},"
            f"{spec.resources.gpu})",
        )

    return CompatibilityVerdict(True, None, f"node {node.node_id} fits")


def check_upgraded_task(task: Task, new_spec: TaskSpec, node: Node) -> CompatibilityVerdict:
    """Decide whether an in-flight task may adopt a new spec on its node.

    The old task is never removed by this function. A compatible verdict
    means the *new* generation may replace the running one; an
    incompatible verdict means the caller must keep the old assignment.
    """
    if task.node_id != node.node_id:
        return CompatibilityVerdict(
            False,
            RejectReason.CAPABILITY_INCOMPATIBLE,
            f"task {task.task_id} lives on {task.node_id!r}, verdict requested "
            f"for node {node.node_id!r}",
        )
    if new_spec.generation < (task.assignment_generation or task.spec.generation):
        return CompatibilityVerdict(
            False,
            RejectReason.CAPABILITY_INCOMPATIBLE,
            f"new spec generation {new_spec.generation} is older than admitted "
            f"generation {task.assignment_generation}; refusing to downgrade",
        )
    return check_placement(node, new_spec)
