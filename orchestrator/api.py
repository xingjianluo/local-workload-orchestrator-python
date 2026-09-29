"""FastAPI 接口层：把控制面能力暴露为 HTTP 接口。

错误码约定（HTTP 状态码 + body.code 双重区分）：

| 异常                       | HTTP | body.code              |
|----------------------------|------|------------------------|
| NoCapacityError            | 409  | NO_CAPACITY            |
| NoAvailableNodeError       | 503  | NO_AVAILABLE_NODE      |
| ConstraintConflictError    | 422  | CONSTRAINT_CONFLICT    |
| LeaseExpiredError          | 410  | LEASE_EXPIRED          |
| LeaseAlreadyHeldError      | 409  | LEASE_ALREADY_HELD     |
| TaskNotFoundError          | 404  | NOT_FOUND              |
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    ConstraintConflictError,
    LeaseAlreadyHeldError,
    LeaseExpiredError,
    NoAvailableNodeError,
    NoCapacityError,
    OrchestratorError,
    TaskNotFoundError,
)
from .models import (
    Node,
    NodeState,
    Resources,
    Spec,
    Task,
    TaskState,
)
from .service import OrchestratorService

_HTTP_STATUS = {
    NoCapacityError: (409, "NO_CAPACITY"),
    NoAvailableNodeError: (503, "NO_AVAILABLE_NODE"),
    ConstraintConflictError: (422, "CONSTRAINT_CONFLICT"),
    LeaseExpiredError: (410, "LEASE_EXPIRED"),
    LeaseAlreadyHeldError: (409, "LEASE_ALREADY_HELD"),
    TaskNotFoundError: (404, "NOT_FOUND"),
}


class ResourcesIn(BaseModel):
    cpu_millis: int = Field(ge=0)
    memory_mib: int = Field(ge=0)


class NodeIn(BaseModel):
    node_id: str
    capacity: ResourcesIn
    labels: dict[str, str] = {}
    supported_versions: list[int] = [1]


class NodeUpgradeIn(BaseModel):
    new_capacity: ResourcesIn | None = None
    add_supported_versions: list[int] = []


class TaskIn(BaseModel):
    task_id: str
    request: ResourcesIn
    node_selector: dict[str, str] = {}
    spec_version: int = 1
    required_caps: list[str] = []


class LeaseActionIn(BaseModel):
    holder: str
    epoch: int


def _resources_out(r: Resources) -> dict:
    return {"cpu_millis": r.cpu_millis, "memory_mib": r.memory_mib}


def _node_out(n: Node) -> dict:
    return {
        "node_id": n.node_id,
        "state": n.state.value,
        "capacity": _resources_out(n.capacity),
        "allocated": _resources_out(n.allocated()),
        "available": _resources_out(n.available()),
        "labels": n.labels,
        "supported_versions": sorted(n.supported_versions),
        "generation": n.generation,
        "last_heartbeat_ms": n.last_heartbeat_ms,
    }


def _task_out(t: Task) -> dict:
    return {
        "task_id": t.task_id,
        "state": t.state.value,
        "request": _resources_out(t.request),
        "node_selector": t.node_selector,
        "spec_version": t.spec.spec_version,
        "spec_at_schedule": t.spec_at_schedule,
        "assigned_node": t.assigned_node,
        "reject_reason": t.reject_reason.value if t.reject_reason else None,
        "reject_detail": t.reject_detail,
        "submission_seq": t.submission_seq,
    }


def create_app(service: OrchestratorService | None = None) -> FastAPI:
    """构造 FastAPI 应用，可注入 service（测试用）。"""
    svc = service or OrchestratorService()
    app = FastAPI(title="本地工作负载编排控制面", version="0.1.0")

    @app.exception_handler(OrchestratorError)
    async def _on_orchestrator_error(_request, exc: OrchestratorError) -> JSONResponse:
        status, code = _HTTP_STATUS.get(type(exc), (400, "ORCHESTRATOR_ERROR"))
        return JSONResponse(
            status_code=status,
            content={"code": code, "message": str(exc)},
        )

    @app.get("/health")
    def health() -> dict:
        return {
            "status": "ok",
            "now_ms": svc.now_ms(),
            "ledger_balances": svc.resource_ledger_balances(),
        }

    @app.post("/nodes", status_code=201)
    def register_node(body: NodeIn) -> dict:
        node = Node(
            node_id=body.node_id,
            capacity=Resources(body.capacity.cpu_millis, body.capacity.memory_mib),
            labels=dict(body.labels),
            supported_versions=frozenset(body.supported_versions),
            state=NodeState.ONLINE,
        )
        svc.register_node(node)
        return _node_out(svc.get_node(body.node_id))

    @app.post("/nodes/{node_id}/heartbeat", status_code=204)
    def node_heartbeat(node_id: str) -> None:
        svc.heartbeat(node_id)

    @app.post("/nodes/{node_id}/cordon")
    def node_cordon(node_id: str) -> dict:
        svc.cordon_node(node_id)
        return _node_out(svc.get_node(node_id))

    @app.post("/nodes/{node_id}/upgrade")
    def node_upgrade(node_id: str, body: NodeUpgradeIn) -> dict:
        svc.upgrade_node(
            node_id,
            new_capacity=(
                Resources(body.new_capacity.cpu_millis, body.new_capacity.memory_mib)
                if body.new_capacity is not None
                else None
            ),
            add_supported_versions=frozenset(body.add_supported_versions),
        )
        return _node_out(svc.get_node(node_id))

    @app.get("/nodes")
    def list_nodes() -> dict:
        return {"nodes": [_node_out(n) for n in svc.list_nodes()]}

    @app.get("/nodes/{node_id}")
    def get_node(node_id: str) -> dict:
        return _node_out(svc.get_node(node_id))

    @app.post("/tasks", status_code=201)
    def submit_task(body: TaskIn) -> dict:
        task = Task(
            task_id=body.task_id,
            request=Resources(body.request.cpu_millis, body.request.memory_mib),
            node_selector=dict(body.node_selector),
            spec=Spec(spec_version=body.spec_version, required_caps=frozenset(body.required_caps)),
            state=TaskState.PENDING,
        )
        svc.submit_task(task)
        return _task_out(task)

    @app.post("/tasks/{task_id}/renew")
    def renew_task(task_id: str, body: LeaseActionIn) -> dict:
        lease = svc.renew_lease(task_id, body.holder, body.epoch)
        return {
            "task_id": lease.task_id,
            "node_id": lease.node_id,
            "epoch": lease.epoch,
            "expires_at_ms": lease.expires_at_ms,
            "holder": lease.holder,
        }

    @app.post("/tasks/{task_id}/complete", status_code=204)
    def complete_task(task_id: str, body: LeaseActionIn) -> None:
        svc.complete_task(task_id, body.holder, body.epoch)

    @app.post("/reconcile")
    def reconcile() -> dict:
        report = svc.reconcile()
        return {
            "reacquired": list(report.reacquired),
            "marked_lost": list(report.marked_lost),
            "expired_leases": list(report.expired_leases),
            "rescheduled": list(report.rescheduled),
        }

    @app.get("/tasks")
    def list_tasks() -> dict:
        return {"tasks": [_task_out(t) for t in svc.list_tasks()]}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        return _task_out(svc.get_task(task_id))

    return app
