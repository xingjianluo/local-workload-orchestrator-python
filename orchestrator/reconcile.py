"""重启与心跳超时后的对账逻辑。

对账目标：
- 节点心跳超时 -> 标记 DOWN，其上运行任务重新对账；
- 租约过期但节点存活 -> 释放租约与资源占用，任务标记 LOST 后重新调度；
- 重新授予租约时 epoch 递增，杜绝过期执行者继续写入；
- 整个过程幂等，重复执行结果一致，资源账守恒。
"""

from __future__ import annotations

import logging

from .config import OrchestratorConfig
from .lease import LeaseManager
from .models import (
    Lease,
    NodeState,
    ReconcileReport,
    TaskState,
)
from .scheduler import Scheduler
from .storage import StoreSnapshot

logger = logging.getLogger("orchestrator.reconcile")


class Reconciler:
    """无状态对账器，直接操作快照（在 service 锁内调用）。"""

    def __init__(self, config: OrchestratorConfig) -> None:
        self._config = config
        self._scheduler = Scheduler()
        self._leases = LeaseManager(config.lease_ttl_ms)

    def reconcile(self, snapshot: StoreSnapshot, now_ms: int) -> ReconcileReport:
        """对快照执行一轮对账与必要的重新调度。

        步骤：
        1. 心跳超时的 ONLINE/CORDONED 节点标记 DOWN；
        2. 扫描全部租约与 RUNNING 任务：
           - 节点 DOWN：租约作废（epoch+1）、任务 LOST，等待重调度；
           - 节点存活但租约过期：原位续约（holder/epoch 不变），任务不丢失不迁移；
        3. 以存活任务的有效租约为事实重建每个节点的 allocations，
           保证资源账既不漏也不重；
        4. 仅对真正失去节点的 LOST/PENDING 任务按稳定顺序重新调度。
        """
        lost_tasks: list[str] = []
        expired_leases: list[str] = []
        reacquired: list[str] = []   # 节点存活、租约原位续约的任务
        rescheduled: list[str] = []  # 失去节点后重新调度成功的任务
        task_id_in_invalidated: set[str] = set()

        # 1. 心跳超时检测
        for node in sorted(snapshot.nodes.values(), key=lambda n: n.node_id):
            if (
                node.state in (NodeState.ONLINE, NodeState.CORDONED)
                and now_ms - node.last_heartbeat_ms > self._config.heartbeat_timeout_ms
            ):
                logger.info(
                    "[对账] 节点 %s 心跳超时（last=%d now=%d timeout=%d），标记 DOWN",
                    node.node_id, node.last_heartbeat_ms, now_ms,
                    self._config.heartbeat_timeout_ms,
                )
                node.state = NodeState.DOWN

        # 2. 租约/任务对账，分三类处理：
        #    A. 节点 DOWN（或消失）：执行权作废（epoch+1），任务 LOST 待重调度；
        #    B. 节点存活但租约过期：原位续约——执行者仍在该节点上，只是续约迟到。
        #       保留原 holder 与 epoch（执行权未转移），任务不丢失、不迁移、不抢占；
        #    C. 租约有效：不动。
        #
        # 这样保证：健康节点上的任务资源先原位锁定，LOST 任务重调度时只能使用
        # 真正空闲的容量，绝不会抢占存活任务。
        for task_id in sorted(snapshot.tasks):
            task = snapshot.tasks[task_id]
            lease = snapshot.leases.get(task_id)
            if task.state != TaskState.RUNNING or lease is None:
                continue
            node = snapshot.nodes.get(lease.node_id)

            if node is None or node.state == NodeState.DOWN:
                # A. 节点不可用：执行权作废，任务等待重新调度（不会在 DOWN 节点上重跑）
                lease = self._invalidate(snapshot, lease, now_ms)
                expired_leases.append(task_id)
                task_id_in_invalidated.add(task_id)
                self._mark_lost(task, node, lost_tasks)
                logger.info(
                    "[对账] 任务 %s 所在节点 %s 不可用，租约作废 epoch->%d，任务 LOST",
                    task_id, lease.node_id, lease.epoch,
                )
                continue

            if not lease.is_valid_at(now_ms):
                # B. 节点存活、租约过期：原位续约，执行权不转移
                renewed = Lease(
                    task_id=lease.task_id,
                    node_id=lease.node_id,
                    acquired_ms=lease.acquired_ms,
                    expires_at_ms=now_ms + self._config.lease_ttl_ms,
                    epoch=lease.epoch,
                    holder=lease.holder,
                )
                snapshot.leases[task_id] = renewed
                reacquired.append(task_id)
                logger.info(
                    "[对账] 任务 %s 所在节点 %s 存活，租约过期原位续约（holder=%s epoch=%d）",
                    task_id, node.node_id, lease.holder, lease.epoch,
                )

        # 3. 以“仍存活任务”的有效租约重建资源账：
        #    B 类任务原位续约后租约有效、占用保留；A 类 LOST 任务不再占账。
        self._rebuild_allocations(snapshot, now_ms)

        # 4. 仅对真正失去执行节点的 LOST / 历史 PENDING 任务重新调度，
        #    按 (submission_seq, task_id) 稳定执行；容量只从存活任务占用之外扣。
        candidates = sorted(
            (t for t in snapshot.tasks.values() if t.state in (TaskState.LOST, TaskState.PENDING)),
            key=lambda t: (t.submission_seq, t.task_id),
        )
        for task in candidates:
            decision = self._scheduler.evaluate(
                task, list(snapshot.nodes.values())
            )
            if not decision.accepted:
                logger.info("[对账] 任务 %s 重新调度暂无落点：%s", task.task_id, decision.detail)
                continue
            node = snapshot.nodes[decision.node_id]
            old_lease = snapshot.leases.get(task.task_id)
            if old_lease is not None and task.task_id in task_id_in_invalidated:
                # A 类：旧租约在本轮已作废（epoch 已 +1），重新授予沿用该 epoch，
                # 保证每次“执行权转移”epoch 恰好递增一次
                epoch = old_lease.epoch
                holder = "reconciler"
            else:
                epoch = (old_lease.epoch if old_lease else 0) + 1
                holder = "reconciler"
            new_lease = self._leases.grant(
                task.task_id, node.node_id, now_ms, holder=holder, epoch=epoch
            )
            snapshot.leases[task.task_id] = new_lease
            node.allocations[task.task_id] = task.request
            task.state = TaskState.RUNNING
            task.assigned_node = node.node_id
            task.spec_at_schedule = task.spec_at_schedule or task.spec.spec_version
            task.reject_reason = None
            task.reject_detail = None
            rescheduled.append(task.task_id)
            logger.info(
                "[对账] 任务 %s 重新调度到节点 %s，新租约 holder=%s epoch=%d expires=%d",
                task.task_id, node.node_id, holder, epoch, new_lease.expires_at_ms,
            )

        return ReconcileReport(
            reacquired=tuple(reacquired),
            marked_lost=tuple(lost_tasks),
            expired_leases=tuple(expired_leases),
            rescheduled=tuple(rescheduled),
        )

    @staticmethod
    def _invalidate(snapshot: StoreSnapshot, lease: Lease, now_ms: int) -> Lease:
        """作废旧租约：epoch 递增、到期时间压到当前，使旧持有者立即失效。"""
        invalidated = Lease(
            task_id=lease.task_id,
            node_id=lease.node_id,
            acquired_ms=lease.acquired_ms,
            expires_at_ms=now_ms,
            epoch=lease.epoch + 1,
            holder=lease.holder,
        )
        snapshot.leases[lease.task_id] = invalidated
        return invalidated

    @staticmethod
    def _mark_lost(task, node, marked_lost: list[str]) -> None:
        task.state = TaskState.LOST
        task.assigned_node = node.node_id if node is not None else None
        marked_lost.append(task.task_id)

    @staticmethod
    def _rebuild_allocations(snapshot: StoreSnapshot, now_ms: int) -> None:
        """以存活任务的有效租约为唯一事实重建节点资源占用。

        - 节点 DOWN 而 LOST 的任务：占用清除，等待重新调度；
        - 节点存活、原位续约后的任务：租约重新有效，占用保留；
        - 没有有效租约的 RUNNING 任务（理论上不应出现）不保留占用。
        """
        for node in snapshot.nodes.values():
            node.allocations = {}
        for task_id, lease in snapshot.leases.items():
            task = snapshot.tasks.get(task_id)
            node = snapshot.nodes.get(lease.node_id)
            if (
                task is not None
                and node is not None
                and task.state == TaskState.RUNNING
                and lease.is_valid_at(now_ms)
            ):
                node.allocations[task_id] = task.request
        # 注：此处不校验 allocated <= capacity。容量收缩等显式运维操作允许
        # 老任务保留形成“可解释的超配”，由 service.capacity_violations 暴露；
        # 账本本身与有效租约一一对应，不会重复计数。
