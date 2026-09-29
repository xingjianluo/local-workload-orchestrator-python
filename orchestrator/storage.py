"""持久化存储。

控制面所有状态变更（任务/节点/租约/提交序号）都先落盘再对外可见，
保证重启后能对账恢复。默认 JSON 文件持久化；:class:`MemoryStore`
用于测试和纯内存场景，接口完全一致。
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
from dataclasses import dataclass, field

from .models import (
    Lease,
    Node,
    NodeState,
    RejectReason,
    Resources,
    Spec,
    Task,
    TaskState,
)


@dataclass
class StoreSnapshot:
    """存储的完整状态快照（持久化与恢复的单位）。"""

    nodes: dict[str, Node] = field(default_factory=dict)
    tasks: dict[str, Task] = field(default_factory=dict)
    leases: dict[str, Lease] = field(default_factory=dict)
    revision: int = 0
    submission_counter: int = 0
    clock_ms: int = 0   # 逻辑时钟（毫秒），随状态持久化，保证重启后时间仍可比


class Store:
    """存储接口。所有方法要求在 service 层的全局锁内调用。"""

    def load(self) -> StoreSnapshot:
        raise NotImplementedError

    def persist(self, snapshot: StoreSnapshot) -> None:
        raise NotImplementedError


def _resources_to_dict(r: Resources) -> dict:
    return {"cpu_millis": r.cpu_millis, "memory_mib": r.memory_mib}


def _resources_from_dict(d: dict) -> Resources:
    return Resources(cpu_millis=d["cpu_millis"], memory_mib=d["memory_mib"])


def _node_to_dict(n: Node) -> dict:
    return {
        "node_id": n.node_id,
        "capacity": _resources_to_dict(n.capacity),
        "labels": dict(n.labels),
        "supported_versions": sorted(n.supported_versions),
        "state": n.state.value,
        "last_heartbeat_ms": n.last_heartbeat_ms,
        "allocations": {tid: _resources_to_dict(r) for tid, r in n.allocations.items()},
        "generation": n.generation,
    }


def _node_from_dict(d: dict) -> Node:
    return Node(
        node_id=d["node_id"],
        capacity=_resources_from_dict(d["capacity"]),
        labels=dict(d["labels"]),
        supported_versions=frozenset(d["supported_versions"]),
        state=NodeState(d["state"]),
        last_heartbeat_ms=d["last_heartbeat_ms"],
        allocations={tid: _resources_from_dict(r) for tid, r in d["allocations"].items()},
        generation=d["generation"],
    )


def _task_to_dict(t: Task) -> dict:
    return {
        "task_id": t.task_id,
        "request": _resources_to_dict(t.request),
        "node_selector": dict(t.node_selector),
        "spec": {
            "spec_version": t.spec.spec_version,
            "required_caps": sorted(t.spec.required_caps),
        },
        "state": t.state.value,
        "assigned_node": t.assigned_node,
        "spec_at_schedule": t.spec_at_schedule,
        "reject_reason": t.reject_reason.value if t.reject_reason else None,
        "reject_detail": t.reject_detail,
        "submission_seq": t.submission_seq,
    }


def _task_from_dict(d: dict) -> Task:
    spec = d["spec"]
    return Task(
        task_id=d["task_id"],
        request=_resources_from_dict(d["request"]),
        node_selector=dict(d["node_selector"]),
        spec=Spec(
            spec_version=spec["spec_version"],
            required_caps=frozenset(spec["required_caps"]),
        ),
        state=TaskState(d["state"]),
        assigned_node=d["assigned_node"],
        spec_at_schedule=d["spec_at_schedule"],
        reject_reason=RejectReason(d["reject_reason"]) if d["reject_reason"] else None,
        reject_detail=d["reject_detail"],
        submission_seq=d["submission_seq"],
    )


def _lease_to_dict(l: Lease) -> dict:
    return {
        "task_id": l.task_id,
        "node_id": l.node_id,
        "acquired_ms": l.acquired_ms,
        "expires_at_ms": l.expires_at_ms,
        "epoch": l.epoch,
        "holder": l.holder,
    }


def _lease_from_dict(d: dict) -> Lease:
    return Lease(
        task_id=d["task_id"],
        node_id=d["node_id"],
        acquired_ms=d["acquired_ms"],
        expires_at_ms=d["expires_at_ms"],
        epoch=d["epoch"],
        holder=d["holder"],
    )


def snapshot_to_dict(snapshot: StoreSnapshot) -> dict:
    """把快照序列化为可 JSON 化的普通字典（键排序，保证文件稳定）。"""
    return {
        "revision": snapshot.revision,
        "submission_counter": snapshot.submission_counter,
        "clock_ms": snapshot.clock_ms,
        "nodes": {nid: _node_to_dict(n) for nid, n in sorted(snapshot.nodes.items())},
        "tasks": {tid: _task_to_dict(t) for tid, t in sorted(snapshot.tasks.items())},
        "leases": {tid: _lease_to_dict(l) for tid, l in sorted(snapshot.leases.items())},
    }


def snapshot_from_dict(data: dict) -> StoreSnapshot:
    """从字典重建快照。"""
    return StoreSnapshot(
        nodes={nid: _node_from_dict(d) for nid, d in data.get("nodes", {}).items()},
        tasks={tid: _task_from_dict(d) for tid, d in data.get("tasks", {}).items()},
        leases={tid: _lease_from_dict(d) for tid, d in data.get("leases", {}).items()},
        revision=data.get("revision", 0),
        submission_counter=data.get("submission_counter", 0),
        clock_ms=data.get("clock_ms", 0),
    )


class MemoryStore(Store):
    """进程内存储，不落盘；持久化时做深拷贝，隔离外部变更。"""

    def __init__(self) -> None:
        self._snapshot = StoreSnapshot()

    def load(self) -> StoreSnapshot:
        return copy.deepcopy(self._snapshot)

    def persist(self, snapshot: StoreSnapshot) -> None:
        self._snapshot = copy.deepcopy(snapshot)


class JsonFileStore(Store):
    """JSON 文件存储：原子写入（临时文件 + os.replace）。

    文件不存在时视为空存储，便于首次启动。
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def load(self) -> StoreSnapshot:
        if not os.path.exists(self._path):
            return StoreSnapshot()
        with open(self._path, encoding="utf-8") as f:
            return snapshot_from_dict(json.load(f))

    def persist(self, snapshot: StoreSnapshot) -> None:
        directory = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".orch-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(snapshot_to_dict(snapshot), f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
