"""升级兼容测试：节点/任务规格升级后的兼容判定与老任务保留。"""

from __future__ import annotations

import pytest

from orchestrator.compatibility import check_spec_compatibility
from orchestrator.config import OrchestratorConfig
from orchestrator.errors import ConstraintConflictError
from orchestrator.models import (
    Node,
    Resources,
    Spec,
    Task,
    TaskState,
)
from orchestrator.service import OrchestratorService


# ----- 纯判定函数 -----
def test_new_task_requires_explicit_version_support() -> None:
    v = check_spec_compatibility(frozenset({1}), spec_version=2,
                                 node_generation=0, task_locked_version=None)
    assert not v.compatible
    assert "v2" in v.detail and "新调度" in v.detail


def test_running_task_keeps_locked_version_after_task_spec_upgrade() -> None:
    # 任务声明已升到 v2，但运行时锁定 v1；节点仍只支持 v1 -> 兼容（不驱逐、不漂移）
    v = check_spec_compatibility(frozenset({1}), spec_version=2,
                                 node_generation=5, task_locked_version=1)
    assert v.compatible
    assert "锁定版本 v1" in v.detail


def test_running_task_on_node_that_drops_version_still_reconciles_by_rule() -> None:
    """判定函数对“运行中老任务”只认锁定版本，节点世代变化不改变结论。"""
    v_old = check_spec_compatibility(frozenset({1}), 1, node_generation=0, task_locked_version=1)
    v_new = check_spec_compatibility(frozenset({1}), 3, node_generation=9, task_locked_version=1)
    assert v_old.compatible and v_new.compatible


# ----- service 层升级语义 -----
def make_service() -> OrchestratorService:
    svc = OrchestratorService(OrchestratorConfig(lease_ttl_ms=1_000_000))
    svc.register_node(Node("n1", Resources(1000, 100), {"zone": "a"}, frozenset({1})))
    return svc


def test_node_capacity_upgrade_preserves_running_tasks() -> None:
    svc = make_service()
    svc.submit_task(Task("t1", Resources(800, 10), {"zone": "a"}))

    svc.upgrade_node("n1", new_capacity=Resources(2000, 200))

    node = svc.get_node("n1")
    assert node.generation == 1
    # 老任务未被清掉，资源占用保留
    assert node.allocations["t1"].cpu_millis == 800
    assert svc.get_task("t1").state == TaskState.RUNNING
    assert svc.get_task("t1").spec_at_schedule == 1
    assert svc.resource_ledger_balances()


def test_node_version_upgrade_enables_new_spec_but_old_task_untouched() -> None:
    svc = make_service()
    svc.submit_task(Task("old", Resources(100, 10), {"zone": "a"}, spec=Spec(1)))

    # 升级前 v2 任务必须被明确拒绝，不能悄悄跑上去
    with pytest.raises(ConstraintConflictError):
        svc.submit_task(Task("new", Resources(100, 10), {"zone": "a"}, spec=Spec(2)))
    assert svc.get_task("new").reject_detail and "v2" in svc.get_task("new").reject_detail

    svc.upgrade_node("n1", add_supported_versions=frozenset({2}))

    # 升级后 v2 任务可以调度
    svc.submit_task(Task("new2", Resources(100, 10), {"zone": "a"}, spec=Spec(2)))
    assert svc.get_task("new2").assigned_node == "n1"
    assert svc.get_task("new2").spec_at_schedule == 2

    # 老任务仍按 v1 运行，占用不变
    old = svc.get_task("old")
    assert old.state == TaskState.RUNNING and old.spec_at_schedule == 1
    assert svc.resource_ledger_balances()


def test_task_spec_upgrade_does_not_migrate_or_reject_running_task() -> None:
    """运行中任务规格升级到 v2：继续按锁定的 v1 跑，且对账后仍不漂移。"""
    svc = make_service()
    task = Task("t1", Resources(100, 10), {"zone": "a"}, spec=Spec(1))
    svc.submit_task(task)

    # 模拟任务侧把声明规格升到 v2（节点并不支持 v2）
    task.spec = Spec(2)

    # 对账不驱逐：锁定版本 v1 仍兼容 n1
    report = svc.reconcile()
    assert report.marked_lost == () and report.rescheduled == ()
    assert svc.get_task("t1").state == TaskState.RUNNING
    assert svc.get_task("t1").spec_at_schedule == 1
    assert svc.get_task("t1").assigned_node == "n1"

    # 但把它当作“新任务”重新选择（LOST 后无锁定版本）时必须被拒绝，
    # 不能静默落到只支持 v1 的节点
    from orchestrator.scheduler import Scheduler

    fresh = Task("t2", Resources(100, 10), {"zone": "a"}, spec=Spec(2))
    decision = Scheduler().evaluate(fresh, [svc.get_node("n1")])
    assert not decision.accepted
    assert "v2" in decision.detail


def test_reconcile_after_node_upgrade_keeps_old_allocations() -> None:
    svc = make_service()
    svc.submit_task(Task("t1", Resources(800, 10), {"zone": "a"}, spec=Spec(1)))
    svc.upgrade_node("n1", new_capacity=Resources(500, 100))  # 容量收缩但不驱逐

    report = svc.reconcile()
    assert report.marked_lost == ()
    node = svc.get_node("n1")
    assert node.allocations.get("t1") is not None
    # 超配状态如实暴露（已分配 800 > 新容量 500），但老任务保留——这是显式选择而非静默迁移
    assert node.allocated().cpu_millis == 800
    assert svc.capacity_violations() == {"n1": Resources(cpu_millis=300, memory_mib=0)}
    assert svc.resource_ledger_balances()  # 账目仍然一一对应

    # 新任务在此超配节点上必然资源不足（剩余为 0 / 负按 0 处理：此处直接不足）
    decision_task = Task("t2", Resources(1, 1), {"zone": "a"})
    from orchestrator.errors import NoCapacityError

    with pytest.raises(NoCapacityError):
        svc.submit_task(decision_task)
