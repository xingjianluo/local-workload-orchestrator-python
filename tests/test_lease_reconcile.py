"""租约与对账测试：续约、心跳超时、重启恢复、epoch 防过期写入。"""

from __future__ import annotations

import logging

import pytest

from orchestrator.config import OrchestratorConfig
from orchestrator.errors import (
    LeaseAlreadyHeldError,
    LeaseExpiredError,
)
from orchestrator.models import Node, NodeState, Resources, Task, TaskState
from orchestrator.service import OrchestratorService
from orchestrator.storage import JsonFileStore


def make_service(tmp_path=None, **overrides) -> OrchestratorService:
    cfg = OrchestratorConfig(
        lease_ttl_ms=100,
        heartbeat_timeout_ms=300,
        storage_path=str(tmp_path / "state.json") if tmp_path else None,
        **overrides,
    )
    store = JsonFileStore(cfg.storage_path) if tmp_path else None
    return OrchestratorService(cfg, store=store)


def seed_two_nodes(svc: OrchestratorService) -> None:
    svc.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    svc.register_node(Node("n2", Resources(1000, 100), {"zone": "b"}, frozenset({1})))


def test_renew_happy_path_extends_expiry() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    svc.advance_clock(50)
    lease = svc.renew_lease("t1", "exec-t1", epoch=1)
    assert lease.expires_at_ms == 150
    assert svc.resource_ledger_balances()


def test_renew_rejected_after_ttl() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    svc.advance_clock(101)  # 租约 100ms 到期
    with pytest.raises(LeaseExpiredError):
        svc.renew_lease("t1", "exec-t1", epoch=1)


def test_renew_rejected_for_wrong_holder() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    with pytest.raises(LeaseAlreadyHeldError):
        svc.renew_lease("t1", "impostor", epoch=1)


def test_renew_rejected_for_stale_epoch_after_failover(caplog) -> None:
    """故障转移（节点 DOWN -> 在别处重授）后旧持有者用旧 epoch 续约/完成必须失败。"""
    caplog.set_level(logging.INFO, logger="orchestrator")
    svc = make_service()
    seed_two_nodes(svc)
    # 任务不带 zone 约束，n1 DOWN 后可迁移到 n2
    svc.submit_task(Task("t1", Resources(100, 10)))

    # 超过心跳超时，只给 n2 心跳：n1 判 DOWN，t1 执行权作废并迁移到 n2
    svc.advance_clock(400)
    svc.heartbeat("n2")
    report = svc.reconcile()
    assert report.marked_lost == ("t1",)
    assert report.rescheduled == ("t1",)
    assert svc.get_task("t1").assigned_node == "n2"

    # 新租约 epoch=2、持有者为 reconciler，旧持有者 epoch=1 的写操作全部失效
    with pytest.raises(LeaseExpiredError):
        svc.renew_lease("t1", "exec-t1", epoch=1)
    with pytest.raises(LeaseExpiredError):
        svc.complete_task("t1", "exec-t1", epoch=1)

    svc.renew_lease("t1", "reconciler", epoch=2)
    svc.complete_task("t1", "reconciler", epoch=2)
    assert svc.get_task("t1").state == TaskState.COMPLETED
    assert svc.resource_ledger_balances()


def test_expired_lease_on_healthy_node_is_renewed_in_place() -> None:
    """节点存活仅租约过期：原位续约，holder/epoch 不变，任务不丢失不迁移。"""
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    svc.advance_clock(101)
    svc.heartbeat("n1")
    svc.heartbeat("n2")
    report = svc.reconcile()

    assert report.marked_lost == () and report.rescheduled == ()
    assert report.reacquired == ("t1",)
    assert svc.get_task("t1").assigned_node == "n1"
    assert svc.get_task("t1").state == TaskState.RUNNING
    # 原位续约：原持有者仍可用相同 epoch 续约
    lease = svc.renew_lease("t1", "exec-t1", epoch=1)
    assert lease.epoch == 1 and lease.holder == "exec-t1"
    assert svc.resource_ledger_balances()


def test_heartbeat_timeout_marks_node_down_and_task_lost() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    # 两个节点都不心跳，推进到心跳超时；租约本身也早已过期
    svc.advance_clock(400)
    report = svc.reconcile()

    assert svc.get_node("n1").state == NodeState.DOWN
    assert svc.get_node("n2").state == NodeState.DOWN
    assert report.marked_lost == ("t1",)
    assert svc.get_task("t1").state == TaskState.LOST
    assert report.rescheduled == ()  # 没有可用节点，不能重调
    assert svc.resource_ledger_balances()


def test_failover_reschedules_to_healthy_node_and_frees_resources() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))

    # n1 心跳超时（DOWN），n2 持续存活
    svc.advance_clock(400)
    svc.heartbeat("n2")
    report = svc.reconcile()

    assert svc.get_node("n1").state == NodeState.DOWN
    assert report.marked_lost == ("t1",)
    # t1 选择器 zone=a 只匹配 DOWN 的 n1 -> 无法迁移，保持 LOST
    assert svc.get_task("t1").state == TaskState.LOST
    assert svc.resource_ledger_balances()

    # n1 恢复注册后再对账 -> 任务回到 n1，epoch 递增一次
    svc.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    report2 = svc.reconcile()
    assert report2.rescheduled == ("t1",)
    assert svc.get_task("t1").assigned_node == "n1"
    assert svc.get_task("t1").state == TaskState.RUNNING
    assert svc.resource_ledger_balances()


def test_reconcile_is_idempotent() -> None:
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))
    svc.advance_clock(400)
    svc.heartbeat("n1")
    svc.heartbeat("n2")

    first = svc.reconcile()
    snapshot_after_first = (
        {n.node_id: dict(n.allocations) for n in svc.list_nodes()},
        [(t.task_id, t.state, t.assigned_node) for t in svc.list_tasks()],
    )
    second = svc.reconcile()

    # 第一轮：租约过期但节点存活 -> 原位续约；第二轮：无任何动作
    assert first.reacquired == ("t1",)
    assert second.marked_lost == () and second.rescheduled == () and second.expired_leases == ()
    assert second.reacquired == ()
    snapshot_after_second = (
        {n.node_id: dict(n.allocations) for n in svc.list_nodes()},
        [(t.task_id, t.state, t.assigned_node) for t in svc.list_tasks()],
    )
    assert snapshot_after_first == snapshot_after_second
    assert svc.resource_ledger_balances()


def test_restart_recovery_rebuilds_leases_from_persisted_state(tmp_path) -> None:
    """模拟控制面重启：新进程从文件加载，启动即对账，不重复执行/不丢账。"""
    svc = make_service(tmp_path)
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10), {"zone": "a"}))
    svc.submit_task(Task("t2", Resources(200, 20), {"zone": "b"}))
    assert svc.resource_ledger_balances()

    # 控制面“重启”：全新 service 实例从同一文件加载
    svc2 = make_service(tmp_path)
    assert {n.node_id for n in svc2.list_nodes()} == {"n1", "n2"}
    assert svc2.get_task("t1").state == TaskState.RUNNING

    # 重启不推进时钟：租约仍有效，对账为空操作，任务不重复执行
    report = svc2.reconcile()
    assert report.expired_leases == () and report.marked_lost == ()
    assert svc2.resource_ledger_balances()

    # 重启后时钟越过 TTL，两节点仍心跳：存活节点上的过期租约原位续约，
    # holder/epoch 不变，任务不丢失不迁移、无重复执行
    svc2.advance_clock(101)
    svc2.heartbeat("n1")
    svc2.heartbeat("n2")
    report2 = svc2.reconcile()
    assert report2.marked_lost == () and report2.rescheduled == ()
    assert set(report2.reacquired) == {"t1", "t2"}
    assert svc2.get_task("t1").assigned_node == "n1"
    assert svc2.get_task("t2").assigned_node == "n2"
    assert svc2.resource_ledger_balances()
    for tid in ("t1", "t2"):
        assert svc2.get_task(tid).state == TaskState.RUNNING

    # 再推进越过心跳超时（n1 最后心跳在 101）且只保活 n2：
    # t1 带 zone=a 约束不能迁到 zone=b 的 n2，保持 LOST；t2 在 n2 原位续约
    svc2.advance_clock(301)
    svc2.heartbeat("n2")
    report3 = svc2.reconcile()
    assert report3.marked_lost == ("t1",)
    assert report3.rescheduled == ()
    assert report3.reacquired == ("t2",)
    assert svc2.get_task("t1").state == TaskState.LOST
    assert svc2.resource_ledger_balances()

    # n1 重新注册后再对账：t1 回到 n1，epoch 已递增
    svc2.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    report4 = svc2.reconcile()
    assert report4.rescheduled == ("t1",)
    assert svc2.get_task("t1").assigned_node == "n1"
    assert svc2.resource_ledger_balances()

    # 再次“重启”后账目仍然一致
    svc3 = make_service(tmp_path)
    assert svc3.resource_ledger_balances()


def test_no_double_execution_after_lease_expiry_and_regrant() -> None:
    """资源账视角验证：节点 DOWN 后重授，旧占用清除、新占用唯一。"""
    svc = make_service()
    seed_two_nodes(svc)
    svc.submit_task(Task("t1", Resources(100, 10)))  # 无 zone 约束，可迁至 n2
    svc.advance_clock(400)
    svc.heartbeat("n2")  # n1 心跳超时 DOWN
    svc.reconcile()

    n1, n2 = svc.get_node("n1"), svc.get_node("n2")
    assert "t1" not in n1.allocations
    assert list(n2.allocations) == ["t1"]  # 仅一笔账，无重复执行
    assert n2.allocations["t1"].cpu_millis == 100


def test_healthy_tasks_not_preempted_when_other_node_fails() -> None:
    """关键不抢占语义：n1 DOWN 后，LOST 任务不得挤占 n2 上存活任务的资源。"""
    svc = make_service()
    seed_two_nodes(svc)
    # n1 放一个大任务（zone=a），n2 放满两个任务（无约束）
    svc.submit_task(Task("a-task", Resources(900, 10), {"zone": "a"}))
    svc.submit_task(Task("b1", Resources(500, 10)))
    svc.submit_task(Task("b2", Resources(500, 10)))
    assert svc.get_task("b1").assigned_node == "n2"
    assert svc.get_task("b2").assigned_node == "n2"

    svc.advance_clock(400)
    svc.heartbeat("n2")  # n1 DOWN；n2 上 b1/b2 租约也过期，但节点存活 -> 原位续约保留
    report = svc.reconcile()

    assert report.marked_lost == ("a-task",)
    assert report.rescheduled == ()  # n2 已被存活任务占满，LOST 任务不能抢占
    assert set(svc.get_node("n2").allocations) == {"b1", "b2"}
    assert svc.get_task("b1").state == TaskState.RUNNING
    assert svc.get_task("b2").state == TaskState.RUNNING
    assert svc.get_task("a-task").state == TaskState.LOST
    assert svc.resource_ledger_balances()
