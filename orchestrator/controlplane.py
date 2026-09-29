"""Control plane facade: single serialized entry point for all mutations.

One ``threading.RLock`` guards every read-modify-write sequence against
node/task/lease state. Capacity is reserved synchronously inside
``submit_task`` before the lock is released, so concurrent submissions
can never oversell: a candidate's feasibility is re-checked against the
*current* used-account under the same lock that later debits it.

Durability: every successful mutation bumps ``version`` and is persisted
atomically. On ``start`` (i.e. process restart) the snapshot is loaded
and reconciled, rebuilding resource accounts and requeuing tasks whose
leases did not survive.
"""

from __future__ import annotations

import copy
import threading

from .clock import Clock, SystemClock
from .compatibility import check_upgraded_task
from .config import Config
from .errors import (
    FencingError,
    NodeNotFoundError,
    TaskNotFoundError,
)
from .lease import LeaseManager
from .models import (
    Node,
    NodeState,
    ReconcileEvent,
    Resources,
    RejectReason,
    ScheduleDecision,
    Task,
    TaskSpec,
    TaskState,
)
from .recovery import Reconciler
from .scheduler import Scheduler
from .storage import StateSnapshot, Storage
from .trace import TraceRecorder


class ControlPlane:
    def __init__(
        self,
        config: Config | None = None,
        storage: Storage | None = None,
        clock: Clock | None = None,
        scheduler: Scheduler | None = None,
        trace: TraceRecorder | None = None,
    ) -> None:
        self._config = config or Config.from_env()
        self._storage = storage or Storage(self._config.state_path)
        self._clock = clock or SystemClock()
        self._trace = trace or TraceRecorder()
        self._scheduler = scheduler or Scheduler(self._trace)
        self._leases = LeaseManager(self._clock, self._config)
        self._reconciler = Reconciler(self._clock, self._config)
        self._lock = threading.RLock()
        self._state = StateSnapshot()

    # --- internal helpers ---------------------------------------------
    def _commit(self) -> None:
        self._state.version += 1
        self._storage.save(self._state)

    def _get_node(self, node_id: str) -> Node:
        node = self._state.nodes.get(node_id)
        if node is None:
            raise NodeNotFoundError(f"node {node_id!r} is not registered")
        return node

    def _get_task(self, task_id: str) -> Task:
        task = self._state.tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(f"task {task_id!r} is not submitted")
        return task

    # --- lifecycle -----------------------------------------------------
    def start(self) -> None:
        """Load durable state (control-plane (re)start) and reconcile."""
        with self._lock:
            self._state = self._storage.load()
            events = self._reconciler.reconcile(self._state)
            self._commit()
        return events

    def persist(self) -> None:
        with self._lock:
            self._commit()

    # --- nodes ---------------------------------------------------------
    def register_node(
        self,
        node_id: str,
        capacity: Resources,
        labels: dict[str, str] | None = None,
        capabilities: dict[str, int] | None = None,
    ) -> Node:
        with self._lock:
            now = self._clock.now()
            node = Node(
                node_id=node_id,
                capacity=capacity,
                used=Resources(0, 0, 0),
                labels=dict(labels or {}),
                capabilities=dict(capabilities or {}),
                state=NodeState.ONLINE,
                last_heartbeat=now,
            )
            self._state.nodes[node_id] = node
            self._commit()
            return node

    def heartbeat(self, node_id: str) -> float:
        with self._lock:
            node = self._get_node(node_id)
            now = self._clock.now()
            node.last_heartbeat = now
            # A recovered node rejoins only explicitly via heartbeat; its
            # previously requeued tasks are scheduled onto whoever fits.
            if node.state is NodeState.UNKNOWN:
                node.state = NodeState.ONLINE
            self._commit()
            return now

    def mark_node_unavailable(self, node_id: str) -> None:
        """Administratively drain a node; its leases are revoked."""
        with self._lock:
            node = self._get_node(node_id)
            node.state = NodeState.UNKNOWN
            self._reconciler.reconcile(self._state)
            self._commit()

    def upgrade_node(
        self,
        node_id: str,
        capacity: Resources | None = None,
        labels: dict[str, str] | None = None,
        capabilities: dict[str, int] | None = None,
    ) -> Node:
        """Apply a node generation bump without disturbing running tasks."""
        with self._lock:
            node = self._get_node(node_id)
            if capacity is not None:
                node.capacity = capacity
            if labels is not None:
                node.labels = dict(labels)
            if capabilities is not None:
                node.capabilities = dict(capabilities)
            node.generation += 1
            self._commit()
            return node

    # --- tasks ---------------------------------------------------------
    def submit_task(self, task_id: str, spec: TaskSpec) -> ScheduleDecision:
        with self._lock:
            now = self._clock.now()
            if task_id in self._state.tasks:
                existing = self._state.tasks[task_id]
                probe = self._scheduler.select_node(
                    spec, self._state.nodes
                )  # records per-node evidence for the duplicate attempt
                decision = ScheduleDecision(
                    task_id=task_id,
                    accepted=False,
                    node_id=None,
                    reject_reason=RejectReason.DUPLICATE_TASK,
                    reject_detail=(
                        f"task {task_id!r} already exists in state "
                        f"{existing.state.value}"
                    ),
                    evaluations=probe.evaluations,
                )
                return decision

            decision = self._scheduler.select_node(spec, self._state.nodes)
            decision = ScheduleDecision(
                task_id=task_id,
                accepted=decision.accepted,
                node_id=decision.node_id,
                reject_reason=decision.reject_reason,
                reject_detail=decision.reject_detail,
                evaluations=decision.evaluations,
                fencing_token=decision.fencing_token,
            )

            if decision.accepted:
                node = self._state.nodes[decision.node_id]  # type: ignore[index]
                token = self._state.next_fencing_token
                self._state.next_fencing_token += 1
                lease = self._leases.issue(
                    task_id,
                    node.node_id,
                    token,
                    generation=spec.generation,
                )
                node.used = Resources(
                    cpu=node.used.cpu + spec.resources.cpu,
                    memory_mb=node.used.memory_mb + spec.resources.memory_mb,
                    gpu=node.used.gpu + spec.resources.gpu,
                )
                node.running_task_ids.add(task_id)
                task = Task(
                    task_id=task_id,
                    spec=spec,
                    state=TaskState.RUNNING,
                    submitted_at=now,
                    node_id=node.node_id,
                    lease=lease,
                    assigned_at=now,
                    assignment_generation=spec.generation,
                )
                decision = ScheduleDecision(
                    task_id=task_id,
                    accepted=True,
                    node_id=node.node_id,
                    evaluations=decision.evaluations,
                    fencing_token=token,
                )
                self._state.leases[task_id] = lease
            else:
                task = Task(
                    task_id=task_id,
                    spec=spec,
                    state=TaskState.REJECTED,
                    submitted_at=now,
                    reject_reason=decision.reject_reason,
                    reject_detail=decision.reject_detail,
                )

            self._state.tasks[task_id] = task
            self._commit()
            return decision

    def renew_lease(self, task_id: str, fencing_token: int):
        with self._lock:
            task = self._get_task(task_id)
            if task.state is not TaskState.RUNNING or task.lease is None:
                raise FencingError(
                    f"task {task_id} is not RUNNING (state={task.state.value}); "
                    "no active lease to renew"
                )
            new_lease = self._leases.renew(task.lease, fencing_token)
            task.lease = new_lease
            self._state.leases[task_id] = new_lease
            self._commit()
            return new_lease

    def complete_task(self, task_id: str, fencing_token: int) -> None:
        with self._lock:
            task = self._get_task(task_id)
            if task.lease is None or fencing_token != task.lease.fencing_token:
                raise FencingError(
                    f"task {task_id}: stale or missing fencing token {fencing_token}"
                )
            node = self._state.nodes.get(task.node_id) if task.node_id else None
            if node is not None:
                need = task.spec.resources
                node.used = Resources(
                    cpu=max(0, node.used.cpu - need.cpu),
                    memory_mb=max(0, node.used.memory_mb - need.memory_mb),
                    gpu=max(0, node.used.gpu - need.gpu),
                )
                node.running_task_ids.discard(task_id)
            self._state.leases.pop(task_id, None)
            task.state = TaskState.COMPLETED
            task.lease = None
            task.node_id = None
            self._commit()

    def upgrade_task_spec(self, task_id: str, new_spec: TaskSpec):
        """Upgrade a RUNNING task spec only if its current node still fits.

        Returns a compatibility verdict; on incompatibility the old
        assignment is preserved untouched.
        """
        with self._lock:
            task = self._get_task(task_id)
            if task.state is not TaskState.RUNNING or task.node_id is None:
                from .compatibility import CompatibilityVerdict

                return CompatibilityVerdict(
                    False,
                    RejectReason.CAPABILITY_INCOMPATIBLE,
                    f"task {task_id} is not RUNNING; nothing to upgrade",
                )
            node = self._get_node(task.node_id)
            # Account as if the old reservation were replaced by the new one.
            probe = copy.copy(node)
            old_need = task.spec.resources
            probe.used = Resources(
                cpu=max(0, node.used.cpu - old_need.cpu),
                memory_mb=max(0, node.used.memory_mb - old_need.memory_mb),
                gpu=max(0, node.used.gpu - old_need.gpu),
            )
            verdict = check_upgraded_task(task, new_spec, probe)
            if verdict.compatible:
                node.used = probe.used
                node.used = Resources(
                    cpu=node.used.cpu + new_spec.resources.cpu,
                    memory_mb=node.used.memory_mb + new_spec.resources.memory_mb,
                    gpu=node.used.gpu + new_spec.resources.gpu,
                )
                task.spec = new_spec
                task.assignment_generation = new_spec.generation
                if task.lease is not None:
                    task.lease = self._leases.issue(
                        task_id,
                        node.node_id,
                        self._state.next_fencing_token,
                        generation=new_spec.generation,
                    )
                    self._state.next_fencing_token += 1
                    self._state.leases[task_id] = task.lease
                self._commit()
            return verdict

    def get_task(self, task_id: str) -> Task:
        with self._lock:
            return self._get_task(task_id)

    def list_tasks(self) -> list[Task]:
        with self._lock:
            return [self._state.tasks[tid] for tid in sorted(self._state.tasks)]

    def get_node(self, node_id: str) -> Node:
        with self._lock:
            return self._get_node(node_id)

    # --- reconciliation ------------------------------------------------
    def reconcile(self) -> list[ReconcileEvent]:
        with self._lock:
            events = self._reconciler.reconcile(self._state)
            self._commit()
            return events

    def snapshot(self) -> StateSnapshot:
        with self._lock:
            return self._state.clone()
