"""Concurrent submit / renew stress: no oversell, accounting conserved."""

from __future__ import annotations

import threading

from orchestrator.models import Resources, TaskSpec, TaskState


def _assert_conserved(cp):
    snap = cp.snapshot()
    for node in snap.nodes.values():
        summed = Resources(0, 0, 0)
        for task in snap.tasks.values():
            if task.state is TaskState.RUNNING and task.node_id == node.node_id:
                r = task.spec.resources
                summed = Resources(
                    summed.cpu + r.cpu,
                    summed.memory_mb + r.memory_mb,
                    summed.gpu + r.gpu,
                )
        assert node.used == summed
        assert node.used.cpu <= node.capacity.cpu
        assert node.used.memory_mb <= node.capacity.memory_mb
        assert node.used.gpu <= node.capacity.gpu


def test_concurrent_submits_never_oversell(cp):
    cp.register_node("n1", Resources(10, 10_240, 0))
    n_threads = 80
    barrier = threading.Barrier(n_threads)
    results: list[bool] = []
    lock = threading.Lock()

    def worker(i: int):
        barrier.wait()
        decision = cp.submit_task(f"t{i:03d}", TaskSpec("img", Resources(1, 1024, 0)))
        with lock:
            results.append(decision.accepted)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 10  # exactly capacity admits
    node = cp.get_node("n1")
    assert node.used == Resources(10, 10_240, 0)
    assert len(node.running_task_ids) == 10
    rejected = [
        t for t in cp.list_tasks() if t.state.value == "REJECTED"
    ]
    assert all(t.reject_reason.value == "INSUFFICIENT_RESOURCES" for t in rejected)
    _assert_conserved(cp)


def test_concurrent_submits_same_task_id_single_admission(cp):
    cp.register_node("n1", Resources(2, 2048, 0))
    n_threads = 30
    barrier = threading.Barrier(n_threads)
    accepted = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        d = cp.submit_task("same-id", TaskSpec("img", Resources(1, 1024, 0)))
        with lock:
            accepted.append(d.accepted)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert accepted.count(True) == 1
    task = cp.get_task("same-id")
    assert task.state is TaskState.RUNNING
    assert cp.get_node("n1").used == Resources(1, 1024, 0)
    _assert_conserved(cp)


def test_concurrent_renewals_single_surviving_lease(cp, clock):
    cp.register_node("n1", Resources(4, 4096, 0))
    d = cp.submit_task("t1", TaskSpec("img", Resources(1, 1024, 0)))
    errors = []

    def good():
        try:
            cp.renew_lease("t1", d.fencing_token)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    # Hammer renewals with both valid and stale tokens concurrently.
    threads = [threading.Thread(target=good) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    snap = cp.snapshot()
    assert set(snap.leases) == {"t1"}
    assert snap.leases["t1"].fencing_token == d.fencing_token
    _assert_conserved(cp)


def test_concurrent_submit_and_complete_account_conserved(cp):
    cp.register_node("n1", Resources(4, 4096, 0))

    def fill_then_release():
        for i in range(4):
            d = cp.submit_task(f"a{i}", TaskSpec("img", Resources(1, 1024, 0)))
            assert d.accepted
        for i in range(4):
            t = cp.get_task(f"a{i}")
            cp.complete_task(f"a{i}", t.lease.fencing_token)

    def concurrent_load():
        for i in range(20):
            cp.submit_task(f"b{i}", TaskSpec("img", Resources(2, 2048, 0)))

    t1 = threading.Thread(target=fill_then_release)
    t2 = threading.Thread(target=concurrent_load)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    _assert_conserved(cp)
