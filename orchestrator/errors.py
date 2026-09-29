"""控制面错误类型：不同拒绝原因可区分。"""


class OrchestratorError(Exception):
    """所有编排错误的基类。"""


class RejectionError(OrchestratorError):
    """任务被调度拒绝的基类。"""


class NoCapacityError(RejectionError):
    """资源不足：没有任何节点能满足任务资源需求。"""


class NoAvailableNodeError(RejectionError):
    """节点不可用：没有处于可用状态的候选节点。"""


class ConstraintConflictError(RejectionError):
    """约束冲突：标签选择器/规格版本等硬性约束无法满足。"""


class TaskNotFoundError(OrchestratorError):
    """任务不存在。"""


class LeaseExpiredError(OrchestratorError):
    """租约已过期，操作（如续约/心跳）被拒绝。"""


class LeaseAlreadyHeldError(OrchestratorError):
    """租约已被其他执行者持有。"""


class ConflictError(OrchestratorError):
    """并发冲突（乐观锁版本不匹配等）。"""
