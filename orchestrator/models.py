"""领域模型：资源、节点、任务、租约与调度结果。

本文件只包含数据结构定义与少量纯数据辅助方法，
调度/兼容判定等业务逻辑放在各自模块。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskState(str, enum.Enum):
    """任务生命周期状态。"""

    PENDING = "PENDING"          # 已提交，尚未成功调度
    RUNNING = "RUNNING"          # 已绑定节点并持有有效租约
    REJECTED = "REJECTED"        # 调度被明确拒绝（带原因）
    COMPLETED = "COMPLETED"      # 正常结束
    LOST = "LOST"                # 对账后判定丢失，等待重新调度


class NodeState(str, enum.Enum):
    """节点生命周期状态。"""

    ONLINE = "ONLINE"            # 可接收调度
    CORDONED = "CORDONED"        # 不可新调度，但已有任务保留
    DOWN = "DOWN"                # 心跳超时/宕机


class RejectReason(str, enum.Enum):
    """拒绝原因枚举，保证不同拒绝路径可区分、可断言。"""

    NO_CAPACITY = "NO_CAPACITY"                 # 资源不足
    NO_AVAILABLE_NODE = "NO_AVAILABLE_NODE"     # 没有可用节点
    CONSTRAINT_CONFLICT = "CONSTRAINT_CONFLICT"  # 标签/规格等约束冲突


@dataclass(frozen=True)
class Resources:
    """可分配资源向量。资源账目一律使用整数，避免浮点误差。"""

    cpu_millis: int = 0
    memory_mib: int = 0

    def fits_in(self, other: Resources) -> bool:
        """self 的需求是否能放入 other 的容量。"""
        return self.cpu_millis <= other.cpu_millis and self.memory_mib <= other.memory_mib

    def __add__(self, other: Resources) -> Resources:
        return Resources(
            cpu_millis=self.cpu_millis + other.cpu_millis,
            memory_mib=self.memory_mib + other.memory_mib,
        )

    def __sub__(self, other: Resources) -> Resources:
        return Resources(
            cpu_millis=self.cpu_millis - other.cpu_millis,
            memory_mib=self.memory_mib - other.memory_mib,
        )


@dataclass(frozen=True)
class Spec:
    """任务规格（含版本），用于升级时的兼容判定。

    老任务保留其提交时的 spec_version；节点声明自己支持的版本集合，
    兼容规则由 compatibility 模块解释，保证判定可解释。
    """

    spec_version: int
    required_caps: frozenset[str] = frozenset()


@dataclass
class Node:
    """本地模拟的工作节点。"""

    node_id: str
    capacity: Resources
    labels: dict[str, str] = field(default_factory=dict)
    supported_versions: frozenset[int] = frozenset({1})
    state: NodeState = NodeState.ONLINE
    last_heartbeat_ms: int = 0          # 最近一次心跳的单调时间戳（毫秒）
    allocations: dict[str, Resources] = field(default_factory=dict)
    generation: int = 0                 # 节点能力版本，能力变更时递增

    def allocated(self) -> Resources:
        """当前已分配资源合计。"""
        total = Resources()
        for amount in self.allocations.values():
            total = total + amount
        return total

    def available(self) -> Resources:
        """当前剩余可分配资源。"""
        return self.capacity - self.allocated()


@dataclass
class Task:
    """待编排的工作负载任务。"""

    task_id: str
    request: Resources
    node_selector: dict[str, str] = field(default_factory=dict)
    spec: Spec = Spec(spec_version=1)
    state: TaskState = TaskState.PENDING
    assigned_node: str | None = None
    spec_at_schedule: int | None = None  # 调度时锁定的规格版本
    reject_reason: RejectReason | None = None
    reject_detail: str | None = None
    submission_seq: int = 0              # 提交序号，保证稳定调度顺序


@dataclass
class Lease:
    """任务在节点上的运行租约。"""

    task_id: str
    node_id: str
    acquired_ms: int
    expires_at_ms: int
    epoch: int = 1                       # 每次重新授予递增，防止过期执行者写入
    holder: str = ""                      # 持有者标识（模拟执行者）

    def is_valid_at(self, now_ms: int) -> bool:
        """租约在给定时刻是否仍然有效（边界：到期时刻即失效）。"""
        return now_ms < self.expires_at_ms


@dataclass(frozen=True)
class SchedulingDecision:
    """一次调度判定的可解释结果。"""

    task_id: str
    accepted: bool
    node_id: str | None
    reason: RejectReason | None
    detail: str
    evaluated_nodes: tuple[str, ...]    # 参与评估的节点（稳定顺序）


@dataclass(frozen=True)
class ReconcileReport:
    """一次对账的结果汇总。"""

    reacquired: tuple[str, ...] = ()
    marked_lost: tuple[str, ...] = ()
    expired_leases: tuple[str, ...] = ()
    rescheduled: tuple[str, ...] = ()
