"""Persistence layer: atomic JSON file storage of the control-plane state.

The state snapshot is the single source of truth; all mutation happens
under the control plane lock and is persisted via atomic
write-temp-then-rename so a crash cannot leave a torn file.

``Storage(path=None)`` selects pure in-memory mode, used by unit tests.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from typing import Any

from .errors import StorageCorruptionError
from .models import (
    Lease,
    Node,
    NodeState,
    RejectReason,
    Resources,
    Task,
    TaskSpec,
    TaskState,
)


@dataclass
class StateSnapshot:
    nodes: dict[str, Node] = field(default_factory=dict)
    tasks: dict[str, Task] = field(default_factory=dict)
    leases: dict[str, Lease] = field(default_factory=dict)
    next_fencing_token: int = 1
    version: int = 0

    def clone(self) -> "StateSnapshot":
        return _snapshot_from_dict(_snapshot_to_dict(self))


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

def _resources_to_dict(r: Resources) -> dict[str, Any]:
    return {"cpu": r.cpu, "memory_mb": r.memory_mb, "gpu": r.gpu}


def _resources_from_dict(d: dict[str, Any]) -> Resources:
    return Resources(cpu=d["cpu"], memory_mb=d["memory_mb"], gpu=d["gpu"])


def _spec_to_dict(spec: TaskSpec) -> dict[str, Any]:
    return {
        "image": spec.image,
        "resources": _resources_to_dict(spec.resources),
        "required_labels": sorted(spec.required_labels),
        "required_capabilities": sorted(spec.required_capabilities),
        "generation": spec.generation,
    }


def _spec_from_dict(d: dict[str, Any]) -> TaskSpec:
    return TaskSpec(
        image=d["image"],
        resources=_resources_from_dict(d["resources"]),
        required_labels=frozenset(tuple(x) for x in d.get("required_labels", [])),
        required_capabilities=frozenset(d.get("required_capabilities", [])),
        generation=d.get("generation", 1),
    )


def _lease_to_dict(lease: Lease) -> dict[str, Any]:
    return asdict(lease)


def _lease_from_dict(d: dict[str, Any]) -> Lease:
    return Lease(
        task_id=d["task_id"],
        node_id=d["node_id"],
        fencing_token=d["fencing_token"],
        acquired_at=d["acquired_at"],
        expires_at=d["expires_at"],
        generation=d["generation"],
    )


def _node_to_dict(node: Node) -> dict[str, Any]:
    return {
        "node_id": node.node_id,
        "capacity": _resources_to_dict(node.capacity),
        "used": _resources_to_dict(node.used),
        "labels": dict(node.labels),
        "capabilities": dict(node.capabilities),
        "state": node.state.value,
        "generation": node.generation,
        "last_heartbeat": node.last_heartbeat,
        "running_task_ids": sorted(node.running_task_ids),
    }


def _node_from_dict(d: dict[str, Any]) -> Node:
    return Node(
        node_id=d["node_id"],
        capacity=_resources_from_dict(d["capacity"]),
        used=_resources_from_dict(d["used"]),
        labels=dict(d.get("labels", {})),
        capabilities=dict(d.get("capabilities", {})),
        state=NodeState(d["state"]),
        generation=d.get("generation", 1),
        last_heartbeat=d.get("last_heartbeat", 0.0),
        running_task_ids=set(d.get("running_task_ids", [])),
    )


def _task_to_dict(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "spec": _spec_to_dict(task.spec),
        "state": task.state.value,
        "submitted_at": task.submitted_at,
        "node_id": task.node_id,
        "lease": _lease_to_dict(task.lease) if task.lease else None,
        "reject_reason": task.reject_reason.value if task.reject_reason else None,
        "reject_detail": task.reject_detail,
        "assigned_at": task.assigned_at,
        "assignment_generation": task.assignment_generation,
    }


def _task_from_dict(d: dict[str, Any]) -> Task:
    rr = d.get("reject_reason")
    return Task(
        task_id=d["task_id"],
        spec=_spec_from_dict(d["spec"]),
        state=TaskState(d["state"]),
        submitted_at=d["submitted_at"],
        node_id=d.get("node_id"),
        lease=_lease_from_dict(d["lease"]) if d.get("lease") else None,
        reject_reason=RejectReason(rr) if rr else None,
        reject_detail=d.get("reject_detail", ""),
        assigned_at=d.get("assigned_at"),
        assignment_generation=d.get("assignment_generation"),
    )


def _snapshot_to_dict(snapshot: StateSnapshot) -> dict[str, Any]:
    return {
        "nodes": {nid: _node_to_dict(n) for nid, n in snapshot.nodes.items()},
        "tasks": {tid: _task_to_dict(t) for tid, t in snapshot.tasks.items()},
        "leases": {tid: _lease_to_dict(l) for tid, l in snapshot.leases.items()},
        "next_fencing_token": snapshot.next_fencing_token,
        "version": snapshot.version,
    }


def _snapshot_from_dict(d: dict[str, Any]) -> StateSnapshot:
    return StateSnapshot(
        nodes={nid: _node_from_dict(nd) for nid, nd in d.get("nodes", {}).items()},
        tasks={tid: _task_from_dict(td) for tid, td in d.get("tasks", {}).items()},
        leases={tid: _lease_from_dict(ld) for tid, ld in d.get("leases", {}).items()},
        next_fencing_token=d.get("next_fencing_token", 1),
        version=d.get("version", 0),
    )


class Storage:
    """File-backed storage. ``path=None`` selects in-memory mode."""

    def __init__(self, path: str | None = "data/state.json") -> None:
        self._path = path

    @property
    def path(self) -> str | None:
        return self._path

    def load(self) -> StateSnapshot:
        if self._path is None or not os.path.exists(self._path):
            return StateSnapshot()
        try:
            with open(self._path, encoding="utf-8") as fh:
                data = json.load(fh)
            return _snapshot_from_dict(data)
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            raise StorageCorruptionError(
                f"state file {self._path} is unreadable: {exc}"
            ) from exc

    def save(self, snapshot: StateSnapshot) -> None:
        if self._path is None:
            return
        parent = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(parent, exist_ok=True)
        payload = json.dumps(_snapshot_to_dict(snapshot), ensure_ascii=False, indent=2)
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
