"""Core domain types for the local workload orchestrator.

This module contains only data types and enums. Behaviour lives in the
dedicated modules (scheduler, lease, recovery, ...).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeState(str, Enum):
    ONLINE = "ONLINE"
    DRAINING = "DRAINING"
    UNKNOWN = "UNKNOWN"


class TaskState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RejectReason(str, Enum):
    """Distinct, machine-readable rejection causes."""

    NODE_NOT_FOUND = "NODE_NOT_FOUND"
    NODE_UNAVAILABLE = "NODE_UNAVAILABLE"
    INSUFFICIENT_RESOURCES = "INSUFFICIENT_RESOURCES"
    LABEL_CONSTRAINT_CONFLICT = "LABEL_CONSTRAINT_CONFLICT"
    CAPABILITY_INCOMPATIBLE = "CAPABILITY_INCOMPATIBLE"
    DUPLICATE_TASK = "DUPLICATE_TASK"
    NO_CANDIDATE_NODE = "NO_CANDIDATE_NODE"


@dataclass(frozen=True)
class Resources:
    cpu: int = 0
    memory_mb: int = 0
    gpu: int = 0


@dataclass(frozen=True)
class TaskSpec:
    image: str
    resources: Resources = field(default_factory=Resources)
    required_labels: frozenset[tuple[str, str]] = frozenset()
    required_capabilities: frozenset[str] = frozenset()
    generation: int = 1


@dataclass
class Node:
    node_id: str
    capacity: Resources
    used: Resources
    labels: dict[str, str]
    capabilities: dict[str, int]
    state: NodeState
    generation: int = 1
    last_heartbeat: float = 0.0
    running_task_ids: set[str] = field(default_factory=set)


@dataclass
class Lease:
    task_id: str
    node_id: str
    fencing_token: int
    acquired_at: float
    expires_at: float
    generation: int


@dataclass
class Task:
    task_id: str
    spec: TaskSpec
    state: TaskState
    submitted_at: float
    node_id: str | None = None
    lease: Lease | None = None
    reject_reason: RejectReason | None = None
    reject_detail: str = ""
    assigned_at: float | None = None
    assignment_generation: int | None = None


@dataclass(frozen=True)
class NodeEvaluation:
    node_id: str
    feasible: bool
    reject_reason: RejectReason | None = None
    detail: str = ""


@dataclass(frozen=True)
class ScheduleDecision:
    task_id: str
    accepted: bool
    node_id: str | None = None
    reject_reason: RejectReason | None = None
    reject_detail: str = ""
    evaluations: tuple[NodeEvaluation, ...] = ()
    fencing_token: int | None = None


@dataclass(frozen=True)
class ReconcileEvent:
    kind: str
    task_id: str
    node_id: str | None
    detail: str
