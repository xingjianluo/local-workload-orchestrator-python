"""租约管理：授予、续约与过期判定。"""

from __future__ import annotations

from .errors import LeaseExpiredError
from .models import Lease


class LeaseManager:
    """根据 TTL 管理租约生命周期。"""

    def __init__(self, ttl_ms: int) -> None:
        if ttl_ms <= 0:
            raise ValueError("lease ttl 必须为正数")
        self._ttl_ms = ttl_ms

    @property
    def ttl_ms(self) -> int:
        return self._ttl_ms

    def grant(self, task_id: str, node_id: str, now_ms: int, holder: str, epoch: int = 1) -> Lease:
        """为任务在节点上签发新租约。"""
        return Lease(
            task_id=task_id,
            node_id=node_id,
            acquired_ms=now_ms,
            expires_at_ms=now_ms + self._ttl_ms,
            epoch=epoch,
            holder=holder,
        )

    def renew(self, lease: Lease, now_ms: int) -> Lease:
        """续约；租约已过期则抛出 :class:`LeaseExpiredError`。

        续约只延长到期时间，不改变持有者与 epoch —— epoch 由对账层在
        重新授予时递增，用于让旧持有者的后续操作失效。
        """
        if not lease.is_valid_at(now_ms):
            raise LeaseExpiredError(
                f"任务 {lease.task_id} 的租约已于 {lease.expires_at_ms}ms 过期"
                f"（当前 {now_ms}ms，epoch={lease.epoch}），拒绝续约"
            )
        return Lease(
            task_id=lease.task_id,
            node_id=lease.node_id,
            acquired_ms=lease.acquired_ms,
            expires_at_ms=now_ms + self._ttl_ms,
            epoch=lease.epoch,
            holder=lease.holder,
        )
