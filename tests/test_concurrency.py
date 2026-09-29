"""并发测试：并发提交与并发续约后资源账守恒、无超卖。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from orchestrator.config import OrchestratorConfig
from orchestrator.errors import OrchestratorError, RejectionError
from orchestrator.models import Node, RejectReason, Resources, Task, TaskState
from orchestrator.service import OrchestratorService


def make_service() -> OrchestratorService:
    svc = OrchestratorService(OrchestratorConfig(lease_ttl_ms=1_000_000))
    # 单节点容量恰好能容纳 10 个 100cpu 的任务
    svc.register_node(Node("n1", Resources(1000, 10_000), {"zone": "a"}, frozenset({1})))
    return svc


def test_concurrent_submissions_never_oversell() -> None:
    svc = make_service()
    n_threads = 40

    def submit(i: int) -> tuple[str, bool, RejectReason | None]:
        try:
            svc.submit_task(Task(f"t{i:03d}", Resources(100, 10), {"zone": "a"}))
            return (f"t{i:03d}", True, None)
        except RejectionError:
            task = svc.get_task(f"t{i:03d}")
            return (f"t{i:03d}", False, task.reject_reason)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(n_threads)))

    accepted = [r for r in results if r[1]]
    rejected = [r for r in results if not r[1]]

    # 容量 1000 / 每个 100 -> 恰好 10 个成功，其余全部因资源不足被拒
    assert len(accepted) == 10
    assert len(rejected) == 30
    assert {r[2] for r in rejected} == {RejectReason.NO_CAPACITY}

    node = svc.get_node("n1")
    assert node.allocated().cpu_millis == 1000
    assert node.allocated().cpu_millis <= node.capacity.cpu_millis
    assert len(node.allocations) == 10
    assert svc.resource_ledger_balances()

    running = [t for t in svc.list_tasks() if t.state == TaskState.RUNNING]
    rejected_tasks = [t for t in svc.list_tasks() if t.state == TaskState.REJECTED]
    assert len(running) == 10 and len(rejected_tasks) == 30
    # 每个成功任务都有独立 task_id 与一笔账目，无重复执行
    assert len({t.task_id for t in running}) == 10


def test_concurrent_submissions_across_two_nodes_stable_total() -> None:
    svc = OrchestratorService(OrchestratorConfig(lease_ttl_ms=1_000_000))
    svc.register_node(Node("a", Resources(600, 10_000), {"zone": "x"}, frozenset({1})))
    svc.register_node(Node("b", Resources(600, 10_000), {"zone": "x"}, frozenset({1})))

    def submit(i: int) -> bool:
        try:
            svc.submit_task(Task(f"t{i:03d}", Resources(150, 10), {"zone": "x"}))
            return True
        except RejectionError:
            return False

    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = list(pool.map(submit, range(20)))

    assert sum(outcomes) == 8  # 总容量 1200 / 150 = 8
    total_allocated = sum(n.allocated().cpu_millis for n in svc.list_nodes())
    assert total_allocated == 1200
    assert svc.resource_ledger_balances()


def test_concurrent_renewals_keep_single_lease_each() -> None:
    svc = make_service()
    for i in range(10):
        svc.submit_task(Task(f"t{i:02d}", Resources(100, 10), {"zone": "a"}))

    barrier = threading.Barrier(10)

    def renew(i: int) -> int:
        barrier.wait()  # 尽量同时打进续约
        lease = svc.renew_lease(f"t{i:02d}", f"exec-t{i:02d}", epoch=1)
        return lease.epoch

    with ThreadPoolExecutor(max_workers=10) as pool:
        epochs = list(pool.map(renew, range(10)))

    # 所有续约成功但 epoch 均保持 1（续约不改 epoch），每任务仍只有一笔账
    assert epochs == [1] * 10
    assert len(svc.get_node("n1").allocations) == 10
    assert svc.resource_ledger_balances()


def test_mixed_submit_renew_complete_preserves_ledger() -> None:
    svc = make_service()
    for i in range(10):
        svc.submit_task(Task(f"init-{i}", Resources(100, 10), {"zone": "a"}))

    stop = threading.Event()

    def churn() -> None:
        i = 0
        while not stop.is_set():
            try:
                svc.submit_task(Task(f"extra-{threading.get_ident()}-{i}",
                                     Resources(100, 10), {"zone": "a"}))
            except RejectionError:
                pass
            i += 1

    def keepalive() -> None:
        while not stop.is_set():
            for j in range(10):
                # 压力混合场景下续约可能因任务被完成而过期失败，属于预期噪声；
                # 本测试只断言最终资源账守恒，故刻意吞掉异常
                try:
                    svc.renew_lease(f"init-{j}", f"exec-init-{j}", epoch=1)
                except OrchestratorError:  # 租约过期/持有者不符等预期失败
                    pass

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(churn) for _ in range(2)]
        futures += [pool.submit(keepalive) for _ in range(2)]
        # 完成部分初始任务腾出资源，与新提交并发交错
        for j in range(0, 10, 2):
            svc.complete_task(f"init-{j}", f"exec-init-{j}", epoch=1)
        stop.set()
        for f in futures:
            f.result()

    assert svc.resource_ledger_balances()
    node = svc.get_node("n1")
    assert node.allocated().cpu_millis <= node.capacity.cpu_millis
    # 账目条目数与 RUNNING 任务数一致
    running_ids = {t.task_id for t in svc.list_tasks() if t.state == TaskState.RUNNING}
    assert set(node.allocations) == running_ids
