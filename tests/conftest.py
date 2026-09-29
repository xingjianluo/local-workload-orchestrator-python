"""Shared pytest fixtures."""

from __future__ import annotations

import logging

import pytest

from orchestrator.clock import FakeClock
from orchestrator.config import Config
from orchestrator.controlplane import ControlPlane
from orchestrator.storage import Storage
from orchestrator.trace import TraceRecorder


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def config() -> Config:
    return Config(state_path="", lease_ttl_seconds=30.0, heartbeat_timeout_seconds=45.0)


@pytest.fixture
def trace() -> TraceRecorder:
    return TraceRecorder()


@pytest.fixture
def cp(clock: FakeClock, config: Config, trace: TraceRecorder) -> ControlPlane:
    storage = Storage(path=None)
    plane = ControlPlane(config=config, storage=storage, clock=clock, trace=trace)
    plane.start()
    return plane
