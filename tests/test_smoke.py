"""跨模块集成测试：完整生命周期 + 日志可观测性 + 守恒总断言。"""

from __future__ import annotations

import logging

import pytest

from orchestrator.config import OrchestratorConfig
from orchestrator.errors import NoCapacityError
from orchestrator.models import Node, Resources, Task, TaskState
from orchestrator.service import OrchestratorService
from orchestrator.storage import JsonFileStore


def test_full_lifecycle_with_failover_and_persistent_restart(tmp_path, caplog) -> None:
    caplog.set_level(logging.INFO)
    path = tmp_path / "state.json"
    cfg = OrchestratorConfig(
        lease_ttl_ms=100, heartbeat_timeout_ms=300, storage_path=str(path)
    )
    svc = OrchestratorService(cfg, store=JsonFileStore(str(path)))

    svc.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    svc.register_node(Node("n2", Resources(1000, 100), {"zone": "a"}, frozenset({1})))

    # 3 个任务（400cpu/个）：n1 放 2 个、n2 放 1 个（node_id 最小优先）
    for i in range(3):
        svc.submit_task(Task(f"t{i}", Resources(400, 10), {"zone": "a"}))
    assert svc.get_task("t0").assigned_node == "n1"
    assert svc.get_task("t1").assigned_node == "n1"
    assert svc.get_task("t2").assigned_node == "n2"
    assert svc.resource_ledger_balances()

    # 再来一个：n1 剩 200、n2 剩 600，能放进 n2
    svc.submit_task(Task("t3", Resources(400, 10), {"zone": "a"}))
    assert svc.get_task("t3").assigned_node == "n2"

    # 此时总量已用 1600/2000，再要 400 必然资源不足
    with pytest.raises(NoCapacityError):
        svc.submit_task(Task("t4", Resources(400, 10), {"zone": "a"}))
    assert svc.resource_ledger_balances()

    # 推进 400ms：超过 TTL(100) 与心跳超时(300)。只给 n2 心跳，n1 判 DOWN。
    svc.advance_clock(400)
    svc.heartbeat("n2")
    report = svc.reconcile()

    # n1 DOWN：t0/t1 执行权作废、LOST；n2 此时剩 200，装不下 400，无法重调
    assert set(report.marked_lost) == {"t0", "t1"}
    assert report.rescheduled == ()
    assert svc.get_node("n1").state.value == "DOWN"
    assert svc.get_task("t0").state == TaskState.LOST
    assert svc.get_task("t1").state == TaskState.LOST
    # n2 存活但其上 t2/t3 租约过期 -> 原位续约（holder/epoch 不变，不迁移、不抢占）
    assert set(report.reacquired) == {"t2", "t3"}
    assert svc.get_task("t2").assigned_node == "n2"
    assert svc.get_task("t3").assigned_node == "n2"
    assert svc.resource_ledger_balances()

    # 原位续约后原持有者仍可用 epoch=1 正常完成
    svc.complete_task("t2", "exec-t2", epoch=1)
    assert svc.get_task("t2").state == TaskState.COMPLETED

    # n1 恢复注册后再对账：t0/t1 重新调度并恢复 RUNNING（t2 已完成让出 400）
    svc.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    report2 = svc.reconcile()
    assert set(report2.rescheduled) == {"t0", "t1"}
    assert all(svc.get_task(t).state == TaskState.RUNNING for t in ("t0", "t1"))
    assert svc.resource_ledger_balances()

    # 模拟重启：新实例加载同一文件，账目一致，空对账不产生重复执行
    svc2 = OrchestratorService(cfg, store=JsonFileStore(str(path)))
    assert svc2.resource_ledger_balances()
    idle = svc2.reconcile()
    assert idle.marked_lost == () and idle.rescheduled == ()

    # 日志包含调度输入、判定依据、最终归属
    text = "\n".join(r.message for r in caplog.records)
    assert "调度输入" in text
    assert "判定依据" in text
    assert "最终归属" in text
