"""Configuration for the orchestrator control plane."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    state_path: str = "data/state.json"
    lease_ttl_seconds: float = 30.0
    heartbeat_timeout_seconds: float = 45.0
    reconcile_interval_seconds: float = 5.0
    storage_dir: str = "data"

    @staticmethod
    def from_env() -> "Config":
        def _f(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw is not None else default

        return Config(
            state_path=os.environ.get("ORCH_STATE_PATH", "data/state.json"),
            lease_ttl_seconds=_f("ORCH_LEASE_TTL_SECONDS", 30.0),
            heartbeat_timeout_seconds=_f("ORCH_HEARTBEAT_TIMEOUT_SECONDS", 45.0),
            reconcile_interval_seconds=_f("ORCH_RECONCILE_INTERVAL_SECONDS", 5.0),
            storage_dir=os.environ.get("ORCH_STORAGE_DIR", "data"),
        )
