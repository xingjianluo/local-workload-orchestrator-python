"""Lease lifecycle: acquisition, renewal and expiry.

Each lease carries a monotonically increasing *fencing token*. Only a
renewal presenting the current token is accepted; an expired or older
token is rejected. Workers embed the token in their operations, so after
a lease is revoked and reissued a stale worker cannot double-execute or
double-free resources (fencing).
"""

from __future__ import annotations

from .clock import Clock
from .config import Config
from .errors import FencingError, LeaseExpiredError
from .models import Lease


class LeaseManager:
    def __init__(self, clock: Clock, config: Config) -> None:
        self._clock = clock
        self._ttl = config.lease_ttl_seconds

    def issue(self, task_id: str, node_id: str, next_token: int, generation: int = 1) -> Lease:
        now = self._clock.now()
        return Lease(
            task_id=task_id,
            node_id=node_id,
            fencing_token=next_token,
            acquired_at=now,
            expires_at=now + self._ttl,
            generation=generation,
        )

    def renew(self, lease: Lease, fencing_token: int) -> Lease:
        now = self._clock.now()
        if fencing_token != lease.fencing_token:
            raise FencingError(
                f"task {lease.task_id}: presented token {fencing_token}, "
                f"current token {lease.fencing_token}"
            )
        if now > lease.expires_at:
            raise LeaseExpiredError(
                f"task {lease.task_id}: lease expired at {lease.expires_at}, "
                f"now={now}"
            )
        return Lease(
            task_id=lease.task_id,
            node_id=lease.node_id,
            fencing_token=lease.fencing_token,
            acquired_at=lease.acquired_at,
            expires_at=now + self._ttl,
            generation=lease.generation,
        )

    def is_expired(self, lease: Lease, now: float | None = None) -> bool:
        point = self._clock.now() if now is None else now
        return point > lease.expires_at
