"""HTTP interface layer (FastAPI).

Exposes the control plane:

* POST /nodes/register        - register / re-register a simulated node
* POST /nodes/{node_id}/heartbeat
* POST /nodes/{node_id}/drain
* POST /tasks/submit          - submit one task (synchronous placement)
* POST /tasks/{task_id}/renew  - lease renewal (fencing token in body)
* POST /tasks/{task_id}/complete
* GET  /tasks                 - list tasks with state and placement
* GET  /reconcile             - force a reconciliation pass
* GET  /health

Rejections are returned with HTTP 409 and a stable machine-readable
``reason`` string drawn from :class:`RejectReason`.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .controlplane import ControlPlane
from .errors import (
    FencingError,
    LeaseExpiredError,
    NodeNotFoundError,
    TaskNotFoundError,
)
from .models import RejectReason, Resources, TaskSpec


class ResourcesBody(BaseModel):
    cpu: int = 0
    memory_mb: int = 0
    gpu: int = 0


class RegisterNodeBody(BaseModel):
    node_id: str
    capacity: ResourcesBody
    labels: dict[str, str] = Field(default_factory=dict)
    capabilities: dict[str, int] = Field(default_factory=dict)


class SubmitTaskBody(BaseModel):
    task_id: str
    image: str
    resources: ResourcesBody = Field(default_factory=ResourcesBody)
    required_labels: list[list[str]] = Field(default_factory=list)
    required_capabilities: list[str] = Field(default_factory=list)
    generation: int = 1


class FencingBody(BaseModel):
    fencing_token: int


def _resources(r: ResourcesBody) -> Resources:
    return Resources(cpu=r.cpu, memory_mb=r.memory_mb, gpu=r.gpu)


def create_app(control_plane: ControlPlane | None = None) -> FastAPI:
    cp = control_plane or ControlPlane()
    app = FastAPI(title="local-workload-orchestrator")

    def _spec(body: SubmitTaskBody) -> TaskSpec:
        return TaskSpec(
            image=body.image,
            resources=_resources(body.resources),
            required_labels=frozenset((k, v) for k, v in body.required_labels),
            required_capabilities=frozenset(body.required_capabilities),
            generation=body.generation,
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/nodes/register")
    def register_node(body: RegisterNodeBody) -> dict:
        node = cp.register_node(
            body.node_id,
            _resources(body.capacity),
            body.labels,
            body.capabilities,
        )
        return {"node_id": node.node_id, "state": node.state.value}

    @app.post("/nodes/{node_id}/heartbeat")
    def heartbeat(node_id: str) -> dict:
        try:
            ts = cp.heartbeat(node_id)
        except NodeNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"node_id": node_id, "last_heartbeat": ts}

    @app.post("/nodes/{node_id}/drain")
    def drain(node_id: str) -> dict:
        try:
            cp.mark_node_unavailable(node_id)
        except NodeNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"node_id": node_id, "state": "UNKNOWN"}

    @app.post("/tasks/submit")
    def submit_task(body: SubmitTaskBody) -> dict:
        decision = cp.submit_task(body.task_id, _spec(body))
        if not decision.accepted:
            raise HTTPException(
                status_code=409,
                detail={
                    "task_id": body.task_id,
                    "reason": decision.reject_reason.value
                    if decision.reject_reason
                    else RejectReason.NO_CANDIDATE_NODE.value,
                    "detail": decision.reject_detail,
                },
            )
        return {
            "task_id": body.task_id,
            "accepted": True,
            "node_id": decision.node_id,
            "fencing_token": decision.fencing_token,
        }

    @app.post("/tasks/{task_id}/renew")
    def renew(task_id: str, body: FencingBody) -> dict:
        try:
            lease = cp.renew_lease(task_id, body.fencing_token)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FencingError as exc:
            raise HTTPException(status_code=409, detail={"reason": "STALE_FENCING_TOKEN", "detail": str(exc)}) from exc
        except LeaseExpiredError as exc:
            raise HTTPException(status_code=409, detail={"reason": "LEASE_EXPIRED", "detail": str(exc)}) from exc
        return {
            "task_id": task_id,
            "fencing_token": lease.fencing_token,
            "expires_at": lease.expires_at,
        }

    @app.post("/tasks/{task_id}/complete")
    def complete(task_id: str, body: FencingBody) -> dict:
        try:
            cp.complete_task(task_id, body.fencing_token)
        except TaskNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except FencingError as exc:
            raise HTTPException(status_code=409, detail={"reason": "STALE_FENCING_TOKEN", "detail": str(exc)}) from exc
        return {"task_id": task_id, "state": "COMPLETED"}

    @app.get("/tasks")
    def list_tasks() -> dict:
        tasks = []
        for t in cp.list_tasks():
            tasks.append(
                {
                    "task_id": t.task_id,
                    "state": t.state.value,
                    "node_id": t.node_id,
                    "image": t.spec.image,
                    "generation": t.spec.generation,
                    "fencing_token": t.lease.fencing_token if t.lease else None,
                    "reject_reason": t.reject_reason.value if t.reject_reason else None,
                }
            )
        return {"tasks": tasks}

    @app.post("/reconcile")
    def reconcile() -> dict:
        events = cp.reconcile()
        return {"events": [{"kind": e.kind, "task_id": e.task_id, "node_id": e.node_id, "detail": e.detail} for e in events]}

    app.state.control_plane = cp
    return app
