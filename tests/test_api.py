"""HTTP interface tests: happy path + distinct rejection codes."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.clock import FakeClock
from orchestrator.config import Config
from orchestrator.controlplane import ControlPlane
from orchestrator.storage import Storage


@pytest.fixture
def client(clock, config, trace):
    storage = Storage(path=None)
    cp = ControlPlane(config=config, storage=storage, clock=clock, trace=trace)
    cp.start()
    return TestClient(create_app(cp))


def _register(client, node_id="n1", **over):
    body = {
        "node_id": node_id,
        "capacity": over.get("capacity", {"cpu": 4, "memory_mb": 4096, "gpu": 0}),
        "labels": over.get("labels", {}),
        "capabilities": over.get("capabilities", {}),
    }
    r = client.post("/nodes/register", json=body)
    assert r.status_code == 200, r.text


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_and_renew_flow(client):
    _register(client)
    r = client.post(
        "/tasks/submit",
        json={"task_id": "t1", "image": "img", "resources": {"cpu": 2, "memory_mb": 2048}},
    )
    assert r.status_code == 200
    token = r.json()["fencing_token"]
    r = client.post("/tasks/t1/renew", json={"fencing_token": token})
    assert r.status_code == 200
    assert r.json()["fencing_token"] == token


def test_insufficient_resources_returns_409_with_reason(client):
    _register(client, capacity={"cpu": 1, "memory_mb": 1024, "gpu": 0})
    r = client.post(
        "/tasks/submit",
        json={"task_id": "big", "image": "img", "resources": {"cpu": 99}},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "INSUFFICIENT_RESOURCES"


def test_label_conflict_distinct_reason(client):
    _register(client, labels={"zone": "b"})
    r = client.post(
        "/tasks/submit",
        json={"task_id": "t", "image": "img", "required_labels": [["zone", "a"]]},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "LABEL_CONSTRAINT_CONFLICT"


def test_capability_incompatible_distinct_reason(client):
    _register(client)
    r = client.post(
        "/tasks/submit",
        json={"task_id": "t", "image": "img", "required_capabilities": ["cuda"]},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "CAPABILITY_INCOMPATIBLE"


def test_empty_cluster_no_candidate_reason(client):
    r = client.post("/tasks/submit", json={"task_id": "t", "image": "img"})
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "NO_CANDIDATE_NODE"


def test_duplicate_submit_distinct_reason(client):
    _register(client)
    body = {"task_id": "t", "image": "img"}
    assert client.post("/tasks/submit", json=body).status_code == 200
    r = client.post("/tasks/submit", json=body)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "DUPLICATE_TASK"


def test_stale_fencing_token_409(client):
    _register(client)
    client.post("/tasks/submit", json={"task_id": "t", "image": "img"})
    r = client.post("/tasks/t/complete", json={"fencing_token": 424242})
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "STALE_FENCING_TOKEN"


def test_unknown_node_heartbeat_404(client):
    assert client.post("/nodes/ghost/heartbeat").status_code == 404


def test_drain_and_reconcile(client):
    _register(client)
    client.post("/tasks/submit", json={"task_id": "t", "image": "img"})
    r = client.post("/nodes/n1/drain")
    assert r.status_code == 200
    tasks = {t["task_id"]: t for t in client.get("/tasks").json()["tasks"]}
    assert tasks["t"]["state"] == "PENDING"
