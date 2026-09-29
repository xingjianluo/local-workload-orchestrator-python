"""编排控制面门面：串行化所有状态变更，维护资源账与租约。

线程模型：一把全局 RLock 保护提交/调度/续约/对账等所有写操作，
保证并发提交与并发续约不会超卖；所有变更先持久化再返回。
"""

from __future__ import annotations

import logging
import threading

from .config import OrchestratorConfig
from .errors import (
    ConstraintConflictError,
    LeaseAlreadyHeldError,
    LeaseExpiredError,
    NoAvailableNodeError,
    NoCapacityError,
    TaskNotFoundError,
)
from .lease import LeaseManager
from .models import (
    Lease,
    Node,
    NodeState,
    ReconcileReport,
    RejectReason,
    Resources,
    Task,
    TaskState,
)
from .reconcile import Reconciler
from .scheduler import Scheduler
from .storage import MemoryStore, Store, StoreSnapshot

logger = logging.getLogger("orchestrator")

_REASON_TO_EXCEPTION = {
    RejectReason.NO_CAPACITY: NoCapacityError,
    RejectReason.NO_AVAILABLE_NODE: NoAvailableNodeError,
    RejectReason.CONSTRAINT_CONFLICT: ConstraintConflictError,
}


class OrchestratorService:
    """控制面对外的主入口。"""

    def __init__(self, config: OrchestratorConfig | None = None, store: Store | None = None) -> None:
        self._config = config or OrchestratorConfig()
        self._store: Store = store or MemoryStore()
        self._lock = threading.RLock()
        self._scheduler = Scheduler()
        self._leases = LeaseManager(self._config.lease_ttl_ms)
        self._reconciler = Reconciler(self._config)
        self._snapshot: StoreSnapshot = self._store.load()
        self._clock_ms: int = self._snapshot.clock_ms

    # ----- 内部工具 -----
    def _persist_locked(self) -> None:
        """同步逻辑时钟、修订号递增并原子落盘。"""
        self._snapshot.clock_ms = self._clock_ms
        self._snapshot.revision += 1
        self._store.persist(self._snapshot)

    # ----- 时间（测试可注入） -----
    def now_ms(self) -> int:
        return self._clock_ms

    def advance_clock(self, delta_ms: int) -> int:
        """推进逻辑时钟并返回新时间。"""
        if delta_ms < 0:
            raise ValueError("时钟不能回拨")
        with self._lock:
            self._clock_ms += delta_ms
            self._snapshot.clock_ms = self._clock_ms
            self._persist_locked()
            return self._clock_ms

    # ----- 节点管理 -----
    def register_node(self, node: Node) -> None:
        """注册（或重新上线）节点。

        重新注册一个已有 node_id 视为节点进程重启：状态置为 ONLINE 并刷新
        心跳；其上的 RUNNING 任务不会被直接清掉，而是交给对账按租约裁决，
        避免“节点重启 = 任务丢失/重复执行”。
        """
        with self._lock:
            existing = self._snapshot.nodes.get(node.node_id)
            if existing is None:
                node.last_heartbeat_ms = self._clock_ms
                self._snapshot.nodes[node.node_id] = node
            else:
                existing.state = NodeState.ONLINE
                existing.last_heartbeat_ms = self._clock_ms
            logger.info("[节点注册] node=%s now=%d", node.node_id, self._clock_ms)
            self._persist_locked()

    def heartbeat(self, node_id: str) -> None:
        """节点心跳：刷新存活时间。DOWN 节点需要先重新注册才能恢复。"""
        with self._lock:
            node = self._snapshot.nodes.get(node_id)
            if node is None:
                raise TaskNotFoundError(f"节点 {node_id} 不存在")
            if node.state == NodeState.DOWN:
                raise TaskNotFoundError(f"节点 {node_id} 已处于 DOWN，需重新注册")
            node.last_heartbeat_ms = self._clock_ms
            self._persist_locked()

    def cordon_node(self, node_id: str) -> None:
        """封锁节点：不再接收新调度，已有任务与租约原样保留。"""
        with self._lock:
            node = self._snapshot.nodes.get(node_id)
            if node is None:
                raise TaskNotFoundError(f"节点 {node_id} 不存在")
            node.state = NodeState.CORDONED
            self._persist_locked()

    def upgrade_node(
        self,
        node_id: str,
        new_capacity: Resources | None = None,
        add_supported_versions: frozenset[int] = frozenset(),
    ) -> None:
        """升级节点能力：扩展容量/支持版本，generation 递增。

        升级只影响后续调度：已有任务的资源占用全部保留，老任务继续按其
        锁定的规格版本运行；任务规格升级后是否能落到该节点仍由兼容模块
        显式判定，不会因为节点世代变化而静默漂移。
        """
        with self._lock:
            node = self._snapshot.nodes.get(node_id)
            if node is None:
                raise TaskNotFoundError(f"节点 {node_id} 不存在")
            before_caps = node.capacity
            if new_capacity is not None:
                node.capacity = new_capacity
            node.supported_versions = node.supported_versions | frozenset(add_supported_versions)
            node.generation += 1
            logger.info(
                "[节点升级] node=%s gen->%d capacity %s->%s versions=%s；保留占用任务 %d 个",
                node_id, node.generation, before_caps, node.capacity,
                sorted(node.supported_versions), len(node.allocations),
            )
            self._persist_locked()

    # ----- 任务生命周期 -----
    def submit_task(self, task: Task) -> Task:
        """提交并立即尝试调度；返回最终状态（RUNNING 或 REJECTED）。

        拒绝时任务以 REJECTED 落盘并带可区分的 reject_reason，同时抛出
        对应异常类型，调用方既能捕获异常也能事后查询拒绝原因。
        """
        with self._lock:
            if task.task_id in self._snapshot.tasks:
                raise ConstraintConflictError(f"任务 {task.task_id} 已存在（task_id 冲突）")
            self._snapshot.submission_counter += 1
            task.submission_seq = self._snapshot.submission_counter
            self._snapshot.tasks[task.task_id] = task

            decision = self._scheduler.evaluate(task, list(self._snapshot.nodes.values()))
            if not decision.accepted:
                task.state = TaskState.REJECTED
                task.reject_reason = decision.reason
                task.reject_detail = decision.detail
                logger.info(
                    "[提交结果] task=%s 被拒绝 reason=%s detail=%s",
                    task.task_id,
                    decision.reason.value if decision.reason else None,
                    decision.detail,
                )
                self._persist_locked()
                exc_cls = _REASON_TO_EXCEPTION[decision.reason]
                raise exc_cls(
                    f"任务 {task.task_id} 调度被拒绝（{decision.reason.value}）：{decision.detail}"
                )

            node = self._snapshot.nodes[decision.node_id]
            lease = self._leases.grant(
                task.task_id, node.node_id, self._clock_ms,
                holder=f"exec-{task.task_id}", epoch=1,
            )
            self._snapshot.leases[task.task_id] = lease
            node.allocations[task.task_id] = task.request
            task.state = TaskState.RUNNING
            task.assigned_node = node.node_id
            task.spec_at_schedule = task.spec.spec_version
            logger.info(
                "[提交结果] task=%s 归属节点=%s lease_epoch=1 expires=%d",
                task.task_id, node.node_id, lease.expires_at_ms,
            )
            self._persist_locked()
            return task

    def renew_lease(self, task_id: str, holder: str, epoch: int) -> Lease:
        """任务执行者续约；任务状态/持有者/epoch/过期时间任一不符即明确拒绝。

        多重校验保证过期执行者或重复执行者无法写入：任务必须 RUNNING、
        持有者标识一致、epoch 一致且租约未过期。
        """
        with self._lock:
            task = self._snapshot.tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(f"任务 {task_id} 不存在")
            lease = self._snapshot.leases.get(task_id)
            if lease is None or task.state != TaskState.RUNNING:
                raise LeaseExpiredError(f"任务 {task_id} 不在运行中，无有效租约")
            # epoch 先于持有者校验：执行权已转移到新 epoch 时，
            # 即便旧持有者恰好同名，也必须按“过期执行权”拒绝
            if lease.epoch != epoch:
                raise LeaseExpiredError(
                    f"任务 {task_id} 续约 epoch={epoch} 与当前 epoch={lease.epoch} 不符，"
                    f"执行权已转移，拒绝旧持有者操作"
                )
            if lease.holder != holder:
                raise LeaseAlreadyHeldError(
                    f"任务 {task_id} 租约持有者为 {lease.holder!r}，拒绝 {holder!r} 续约"
                )
            new_lease = self._leases.renew(lease, self._clock_ms)
            self._snapshot.leases[task_id] = new_lease
            logger.info(
                "[续约] task=%s holder=%s epoch=%d new_expires=%d",
                task_id, holder, epoch, new_lease.expires_at_ms,
            )
            self._persist_locked()
            return new_lease

    def complete_task(self, task_id: str, holder: str, epoch: int) -> None:
        """任务正常完成：校验持有者与 epoch 后释放租约与资源。"""
        with self._lock:
            task = self._snapshot.tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(f"任务 {task_id} 不存在")
            lease = self._snapshot.leases.get(task_id)
            if lease is None:
                raise LeaseExpiredError(f"任务 {task_id} 无租约记录")
            if lease.epoch != epoch:
                raise LeaseExpiredError(
                    f"任务 {task_id} 完成 epoch={epoch} 已过期，当前 epoch={lease.epoch}"
                )
            if lease.holder != holder:
                raise LeaseAlreadyHeldError(
                    f"任务 {task_id} 持有者为 {lease.holder!r}，{holder!r} 无权完成"
                )
            node = self._snapshot.nodes.get(lease.node_id)
            if node is not None:
                node.allocations.pop(task_id, None)
            task.state = TaskState.COMPLETED
            self._snapshot.leases.pop(task_id, None)
            logger.info("[完成] task=%s 释放节点 %s 上的资源", task_id, lease.node_id)
            self._persist_locked()

    def reconcile(self, now_ms: int | None = None) -> ReconcileReport:
        """触发一轮对账（重启恢复 / 心跳超时）。

        控制面启动时也应调用一次：持久化的 RUNNING 状态以租约为准重新裁决，
        避免重启造成任务重复执行或资源重复占用。
        """
        with self._lock:
            if now_ms is not None:
                if now_ms < self._clock_ms:
                    raise ValueError("对账时间不能早于当前时钟")
                self._clock_ms = now_ms
            report = self._reconciler.reconcile(self._snapshot, self._clock_ms)
            logger.info(
                "[对账完成] expired=%s lost=%s rescheduled=%s reacquired=%s",
                report.expired_leases, report.marked_lost,
                report.rescheduled, report.reacquired,
            )
            self._persist_locked()
            return report

    # ----- 查询（用于断言资源守恒） -----
    def get_task(self, task_id: str) -> Task:
        with self._lock:
            task = self._snapshot.tasks.get(task_id)
            if task is None:
                raise TaskNotFoundError(f"任务 {task_id} 不存在")
            return task

    def get_node(self, node_id: str) -> Node:
        with self._lock:
            node = self._snapshot.nodes.get(node_id)
            if node is None:
                raise TaskNotFoundError(f"节点 {node_id} 不存在")
            return node

    def list_tasks(self) -> list[Task]:
        with self._lock:
            return sorted(
                self._snapshot.tasks.values(), key=lambda t: (t.submission_seq, t.task_id)
            )

    def list_nodes(self) -> list[Node]:
        with self._lock:
            return sorted(self._snapshot.nodes.values(), key=lambda n: n.node_id)

    def resource_ledger_balances(self) -> bool:
        """校验资源账：节点占用与 RUNNING 任务严格一一对应（账目守恒）。

        守恒的两层含义：
        1. 节点维度：allocations 必须恰好等于该节点上 RUNNING 任务的请求
           （无超卖式的重复计数、无幽灵占用）；
        2. 任务维度：每个 RUNNING 任务必须指向一个存在的节点。

        注意：该方法只验证“账实相符”。容量收缩后老任务保留导致的
        allocated > capacity 属于可解释的显式超配，由
        :meth:`capacity_violations` 单独暴露。
        """
        with self._lock:
            expected_by_node: dict[str, dict[str, Resources]] = {}
            for task in self._snapshot.tasks.values():
                if task.state != TaskState.RUNNING:
                    continue
                if task.assigned_node is None:
                    return False
                expected_by_node.setdefault(task.assigned_node, {})[task.task_id] = task.request
            for node_id, node in self._snapshot.nodes.items():
                if dict(node.allocations) != expected_by_node.get(node_id, {}):
                    return False
            for node_id in expected_by_node:
                if node_id not in self._snapshot.nodes:
                    return False
            return True

    def capacity_violations(self) -> dict[str, Resources]:
        """返回显式超配节点及其超出量（例如容量收缩后保留老任务所致）。"""
        with self._lock:
            violations: dict[str, Resources] = {}
            for node in self._snapshot.nodes.values():
                used = node.allocated()
                over_cpu = max(0, used.cpu_millis - node.capacity.cpu_millis)
                over_mem = max(0, used.memory_mib - node.capacity.memory_mib)
                if over_cpu or over_mem:
                    violations[node.node_id] = Resources(over_cpu, over_mem)
            return violations
