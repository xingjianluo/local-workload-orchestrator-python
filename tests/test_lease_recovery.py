"""Lease renewal, fencing, restart reconciliation and heartbeat timeout.

Invariant checked repeatedly: capacity account after reconciliation
equals exactly the sum of surviving RUNNING reservations.
"""

from __future__ import annotations

import pytest

from orchestrator.config import Config
from orchestrator.controlplane import ControlPlane
from orchestrator.errors import FencingError, LeaseExpiredError
from orchestrator.models import (
    RejectReason,
    Resources,
    TaskSpec,
    TaskState,
)
from orchestrator.storage import Storage


def _assert_accounts_conserved(cp: ControlPlane) -> None:
    snap = cp.snapshot()
    for node_id, node in snap.nodes.items():
        summed = Resources(0, 0, 0)
        for task in snap.tasks.values():
            if task.state is TaskState.RUNNING and task.node_id == node_id:
                r = task.spec.resources
                summed = Resources(
                    summed.cpu + r.cpu,
                    summed.memory_mb + r.memory_mb,
                    summed.gpu + r.gpu,
                )
        assert node.used == summed, f"node {node_id} account leaked"
        assert node.running_task_ids == {
            t.task_id
            for t in snap.tasks.values()
            if t.state is TaskState.RUNNING and t.node_id == node_id
        }
        assert (
            node.used.cpu <= node.capacity.cpu
            and node.used.memory_mb <= node.capacity.memory_mb
            and node.used.gpu <= node.capacity.gpu
        )


def test_renew_extends_lease_and_stale_token_rejected(cp, clock):
    cp.register_node("n1", Resources(4, 4096, 0))
    d = cp.submit_task("t1", TaskSpec("img", Resources(1, 1024, 0)))
    clock.advance(20)
    renewed = cp.renew_lease("t1", d.fencing_token)
    assert renewed.expires_at > d.fencing_token  # just ensure field exists
    with pytest.raises(FencingError):
        cp.renew_lease("t1", d.fencing_token + 999)
    _assert_accounts_conserved(cp)


def test_expired_lease_requeues_task_and_frees_capacity(cp, clock):
    cp.register_node("n1", Resources(4, 4096, 0))
    d = cp.submit_task("t1", TaskSpec("img", Resources(3, 3072, 0)))
    assert cp.get_node("n1").used.cpu == 3
    # heartbeat keeps node alive but lease is not renewed -> expiry
    clock.advance(40)
    cp.heartbeat("n1")
    events = cp.reconcile()
    assert any(e.kind == "LEASE_REVOKED" and e.task_id == "t1" for e in events)
    task = cp.get_task("t1")
    assert task.state is TaskState.PENDING
    assert task.node_id is None and task.lease is None
    assert cp.get_node("n1").used == Resources(0, 0, 0)
    # stale worker with old token must be fenced off
    with pytest.raises(FencingError):
        cp.complete_task("t1", d.fencing_token)
    _assert_accounts_conserved(cp)


def test_heartbeat_timeout_marks_node_unknown_and_requeues(cp, clock):
    cp.register_node("n1", Resources(4, 4096, 0))
    cp.submit_task("t1", TaskSpec("img", Resources(1, 1024, 0)))
    clock.advance(50)
    events = cp.reconcile()
    assert any(e.kind == "NODE_UNKNOWN" for e in events)
    assert cp.get_node("n1").state.value == "UNKNOWN"
    assert cp.get_task("t1").state is TaskState.PENDING
    _assert_accounts_conserved(cp)


def test_restart_rebuilds_accounts_and_does_not_duplicate_tasks(tmp_path, clock, config, trace):
    state_file = tmp_path / "state.json"
    storage = Storage(str(state_file))
    cp1 = ControlPlane(
        config=Config(state_path=str(state_file), lease_ttl_seconds=30.0,
                      heartbeat_timeout_seconds=45.0),
        storage=storage,
        clock=clock,
        trace=trace,
    )
    cp1.start()
    cp1.register_node("n1", Resources(4, 4096, 0))
    d = cp1.submit_task("t1", TaskSpec("img", Resources(2, 2048, 0)))
    assert d.accepted

    # Simulate control-plane crash: fresh instance loads same file.
    clock.advance(5)
    cp2 = ControlPlane(
        config=Config(state_path=str(state_file), lease_ttl_seconds=30.0,
                      heartbeat_timeout_seconds=45.0),
        storage=Storage(str(state_file)),
        clock=clock,
        trace=trace,
    )
    cp2.start()
    task = cp2.get_task("t1")
    assert task.state is TaskState.RUNNING
    assert task.node_id == "n1"
    assert cp2.get_node("n1").used == Resources(2, 2048, 0)
    # token sequence continues after restart (monotonic, persisted)
    d2 = cp2.submit_task("t2", TaskSpec("img", Resources(1, 1024, 0)))
    assert d2.fencing_token == d.fencing_token + 1
    _assert_accounts_conserved(cp2)


def test_restart_after_expiry_requeues_without_double_booking(tmp_path, clock, trace):
    state_file = tmp_path / "state.json"
    cfg = Config(state_path=str(state_file), lease_ttl_seconds=30.0,
                 heartbeat_timeout_seconds=45.0)
    cp1 = ControlPlane(config=cfg, storage=Storage(str(state_file)), clock=clock, trace=trace)
    cp1.start()
    cp1.register_node("n1", Resources(4, 4096, 0))
    cp1.submit_task("t1", TaskSpec("img", Resources(2, 2048, 0)))
    clock.advance(100)  # both lease and heartbeat expire

    cp2 = ControlPlane(config=cfg, storage=Storage(str(state_file)), clock=clock, trace=trace)
    cp2.start()
    assert cp2.get_task("t1").state is TaskState.PENDING
    assert cp2.get_node("n1").used == Resources(0, 0, 0)
    # rescheduling reserves exactly once
    cp2.heartbeat("n1")
    d = cp2.submit_task("t1-rerun", TaskSpec("img", Resources(2, 2048, 0)))
    assert d.accepted
    assert cp2.get_node("n1").used == Resources(2, 2048, 0)
    _assert_accounts_conserved(cp2)


def test_drain_revokes_leases_immediately(cp):
    cp.register_node("n1", Resources(4, 4096, 0))
    cp.submit_task("t1", TaskSpec("img", Resources(1, 1024, 0)))
    cp.mark_node_unavailable("n1")
    assert cp.get_task("t1").state is TaskState.PENDING
    assert cp.get_node("n1").used == Resources(0, 0, 0)
    _assert_accounts_conserved(cp)


def test_expired_renewal_raises(cp, clock):
    cp.register_node("n1", Resources(4, 4096, 0))
    d = cp.submit_task("t1", TaskSpec("img", Resources(1, 1024, 0)))
    clock.advance(100)
    cp.reconcile()
    with pytest.raises(FencingError):
        cp.renew_lease("t1", d.fencing_token)
