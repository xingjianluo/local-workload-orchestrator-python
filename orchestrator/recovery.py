"""Restart / heartbeat-timeout reconciliation.

Responsibilities
----------------
* ``recompute_used`` rebuilds every node's used-resource account purely
  from the authoritative set of RUNNING task leases and the task specs
  that consumed them. After any crash, calling this once guarantees
  capacity used == sum(admitted running workloads): no double-booking,
  no leaked reservation.
* ``reconcile`` then:
  1. marks nodes whose heartbeat is older than
     ``heartbeat_timeout_seconds`` as UNKNOWN;
  2. revokes every expired lease and every lease on a UNKNOWN node,
     returning its task to PENDING so the scheduler places it once more;
  3. drops the revoked leases and rebuilds the resource account.

Tasks are never duplicated: a PENDING task has no lease, and only one
lease can exist per task id. Revocation frees the reservation exactly
once because accounting is reconstructed rather than incrementally
patched.
"""

from __future__ import annotations

import logging

from .clock import Clock
from .config import Config
from .models import (
    Node,
    NodeState,
    ReconcileEvent,
    Resources,
    Task,
    TaskState,
)
from .storage import StateSnapshot

logger = logging.getLogger("orchestrator.recovery")


class Reconciler:
    def __init__(self, clock: Clock, config: Config) -> None:
        self._clock = clock
        self._config = config

    def recompute_used(self, state: StateSnapshot) -> None:
        totals: dict[str, Resources] = {
            node_id: Resources(0, 0, 0) for node_id in state.nodes
        }
        running: dict[str, set[str]] = {node_id: set() for node_id in state.nodes}

        for task_id, task in state.tasks.items():
            lease = state.leases.get(task_id)
            if task.state is TaskState.RUNNING and lease is not None:
                if lease.node_id in totals:
                    need = task.spec.resources
                    prev = totals[lease.node_id]
                    totals[lease.node_id] = Resources(
                        cpu=prev.cpu + need.cpu,
                        memory_mb=prev.memory_mb + need.memory_mb,
                        gpu=prev.gpu + need.gpu,
                    )
                    running[lease.node_id].add(task_id)
                else:
                    logger.warning(
                        "task %s references missing node %s during recompute",
                        task_id,
                        lease.node_id,
                    )

        for node_id, node in state.nodes.items():
            node.used = totals[node_id]
            node.running_task_ids = running[node_id]

    def _revoke(self, state: StateSnapshot, task: Task, events: list[ReconcileEvent], detail: str) -> None:
        lease = state.leases.pop(task.task_id, None)
        node_id = lease.node_id if lease else task.node_id
        task.state = TaskState.PENDING
        task.node_id = None
        task.lease = None
        task.assigned_at = None
        events.append(
            ReconcileEvent(
                kind="LEASE_REVOKED",
                task_id=task.task_id,
                node_id=node_id,
                detail=detail,
            )
        )
        logger.info(
            "[reconcile] revoke lease task=%s node=%s detail=%s",
            task.task_id,
            node_id,
            detail,
        )

    def reconcile(self, state: StateSnapshot) -> list[ReconcileEvent]:
        now = self._clock.now()
        events: list[ReconcileEvent] = []

        # 1) heartbeat liveness
        for node_id, node in state.nodes.items():
            if node.state is NodeState.ONLINE and (
                node.last_heartbeat == 0.0
                or now - node.last_heartbeat > self._config.heartbeat_timeout_seconds
            ):
                node.state = NodeState.UNKNOWN
                events.append(
                    ReconcileEvent(
                        kind="NODE_UNKNOWN",
                        task_id="",
                        node_id=node_id,
                        detail=(
                            f"last heartbeat {node.last_heartbeat}, now={now}, "
                            f"timeout={self._config.heartbeat_timeout_seconds}s"
                        ),
                    )
                )
                logger.warning(
                    "[reconcile] node %s marked UNKNOWN (heartbeat stale)", node_id
                )

        # 2) revoke expired / dead-node leases
        for task_id in list(state.leases.keys()):
            task = state.tasks.get(task_id)
            lease = state.leases[task_id]
            if task is None or task.state is not TaskState.RUNNING:
                # Orphan lease: no authoritative running task -> drop it.
                state.leases.pop(task_id, None)
                events.append(
                    ReconcileEvent(
                        kind="ORPHAN_LEASE_DROPPED",
                        task_id=task_id,
                        node_id=lease.node_id,
                        detail="no RUNNING task backs this lease",
                    )
                )
                continue

            node = state.nodes.get(lease.node_id)
            if now > lease.expires_at:
                self._revoke(
                    state,
                    task,
                    events,
                    f"lease expired at {lease.expires_at} (now={now})",
                )
            elif node is None or node.state is NodeState.UNKNOWN:
                self._revoke(
                    state,
                    task,
                    events,
                    f"node {lease.node_id} is UNKNOWN/gone; fencing token "
                    f"{lease.fencing_token} retired",
                )

        # 3) authoritative accounting
        self.recompute_used(state)
        return events
