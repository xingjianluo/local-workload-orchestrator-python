"""接口层测试：HTTP 状态码/错误码区分与完整提交流程。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.config import OrchestratorConfig
from orchestrator.models import TaskState
from orchestrator.service import OrchestratorService
from orchestrator.storage import JsonFileStore


def make_client(tmp_path=None) -> tuple[TestClient, OrchestratorService]:
    path = str(tmp_path / "state.json") if tmp_path else None
    cfg = OrchestratorConfig(lease_ttl_ms=100, heartbeat_timeout_ms=300, storage_path=path)
    svc = OrchestratorService(cfg, store=JsonFileStore(path) if tmp_path else None)
    return TestClient(create_app(svc)), svc


def register(client: TestClient, node_id: str, cpu: int, zone: str, versions=(1,)) -> None:
    r = client.post(
        "/nodes",
        json={
            "node_id": node_id,
            "capacity": {"cpu_millis": cpu, "memory_mib": 1024},
            "labels": {"zone": zone},
            "supported_versions": list(versions),
        },
    )
    assert r.status_code == 201, r.text


def test_health_and_submit_flow(tmp_path) -> None:
    client, _ = make_client(tmp_path)
    assert client.get("/health").json()["status"] == "ok"

    register(client, "n1", 1000, "a")
    r = client.post(
        "/tasks",
        json={"task_id": "t1", "request": {"cpu_millis": 500, "memory_mib": 64},
              "node_selector": {"zone": "a"}},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["state"] == TaskState.RUNNING.value
    assert body["assigned_node"] == "n1"
    assert body["spec_at_schedule"] == 1

    # 节点视图能看到 allocated/available
    n1 = client.get("/nodes/n1").json()
    assert n1["allocated"]["cpu_millis"] == 500
    assert n1["available"]["cpu_millis"] == 500


def test_rejection_status_codes_are_distinguishable() -> None:
    client, _ = make_client()
    register(client, "n1", 1000, "a")
    register(client, "n2", 1000, "b", versions=(1, 2))

    # 资源不足：有匹配节点但装不下 -> 409
    r = client.post("/tasks", json={
        "task_id": "cap", "request": {"cpu_millis": 9000, "memory_mib": 1},
        "node_selector": {"zone": "a"}})
    assert r.status_code == 409 and r.json()["code"] == "NO_CAPACITY"

    # 约束冲突：zone 不存在 / 版本不支持 -> 422
    r = client.post("/tasks", json={
        "task_id": "lbl", "request": {"cpu_millis": 1, "memory_mib": 1},
        "node_selector": {"zone": "zz"}})
    assert r.status_code == 422 and r.json()["code"] == "CONSTRAINT_CONFLICT"

    r = client.post("/tasks", json={
        "task_id": "ver", "request": {"cpu_millis": 1, "memory_mib": 1},
        "node_selector": {"zone": "a"}, "spec_version": 2})
    assert r.status_code == 422 and r.json()["code"] == "CONSTRAINT_CONFLICT"

    # 全部封锁 -> 503
    assert client.post("/nodes/n1/cordon").status_code == 200
    assert client.post("/nodes/n2/cordon").status_code == 200
    r = client.post("/tasks", json={
        "task_id": "none", "request": {"cpu_millis": 1, "memory_mib": 1}})
    assert r.status_code == 503 and r.json()["code"] == "NO_AVAILABLE_NODE"


def test_renew_complete_and_lease_errors() -> None:
    client, _ = make_client()
    register(client, "n1", 1000, "a")
    client.post("/tasks", json={
        "task_id": "t1", "request": {"cpu_millis": 100, "memory_mib": 10},
        "node_selector": {"zone": "a"}})

    assert client.post("/tasks/t1/renew", json={"holder": "exec-t1", "epoch": 1}).status_code == 200

    # 错误持有者 -> 409 LEASE_ALREADY_HELD
    r = client.post("/tasks/t1/renew", json={"holder": "x", "epoch": 1})
    assert r.status_code == 409 and r.json()["code"] == "LEASE_ALREADY_HELD"

    # 错误 epoch -> 410 LEASE_EXPIRED
    r = client.post("/tasks/t1/renew", json={"holder": "exec-t1", "epoch": 7})
    assert r.status_code == 410 and r.json()["code"] == "LEASE_EXPIRED"

    # 不存在 -> 404
    assert client.get("/tasks/missing").status_code == 404

    # 正常完成 -> 204，资源释放
    assert client.post("/tasks/t1/complete", json={"holder": "exec-t1", "epoch": 1}).status_code == 204
    assert client.get("/nodes/n1").json()["allocated"]["cpu_millis"] == 0


def test_reconcile_endpoint_reports(tmp_path) -> None:
    client, svc = make_client(tmp_path)
    register(client, "n1", 1000, "a")
    client.post("/tasks", json={
        "task_id": "t1", "request": {"cpu_millis": 100, "memory_mib": 10},
        "node_selector": {"zone": "a"}})

    # 心跳超时（不调用 heartbeat）使 n1 判 DOWN；无其他节点 -> LOST 且无法重调
    svc.advance_clock(400)
    r = client.post("/reconcile")
    assert r.status_code == 200
    assert r.json()["marked_lost"] == ["t1"]
    assert r.json()["rescheduled"] == []
    assert client.get("/health").json()["ledger_balances"] is True

    # 节点重新上线后再对账，任务恢复
    client.post("/nodes", json={
        "node_id": "n1", "capacity": {"cpu_millis": 1000, "memory_mib": 1024},
        "labels": {"zone": "a"}})
    r = client.post("/reconcile")
    assert r.json()["rescheduled"] == ["t1"]


def test_node_upgrade_endpoint_preserves_tasks() -> None:
    client, _ = make_client()
    register(client, "n1", 1000, "a")
    client.post("/tasks", json={
        "task_id": "old", "request": {"cpu_millis": 100, "memory_mib": 10},
        "node_selector": {"zone": "a"}})

    r = client.post("/nodes/n1/upgrade", json={
        "new_capacity": {"cpu_millis": 2000, "memory_mib": 2048},
        "add_supported_versions": [2]})
    assert r.status_code == 200
    body = r.json()
    assert body["generation"] == 1 and 2 in body["supported_versions"]
    assert body["allocated"]["cpu_millis"] == 100
