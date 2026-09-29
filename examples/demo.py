"""End-to-end local simulation: nodes, tasks, lease expiry and recovery.

Run:  uv run python examples/demo.py

Deterministically:
  1. registers two simulated nodes;
  2. submits a batch (one oversized / label-conflicting / duplicate are
     rejected with distinct reasons);
  3. renews a lease, lets another time out and reconciles;
  4. prints the final resource accounts (always conserved).
"""

from __future__ import annotations

from orchestrator.clock import FakeClock
from orchestrator.config import Config
from orchestrator.controlplane import ControlPlane
from orchestrator.models import Resources, TaskSpec
from orchestrator.storage import Storage


def main() -> None:
    clock = FakeClock()
    cp = ControlPlane(
        config=Config(state_path="", lease_ttl_seconds=30.0,
                      heartbeat_timeout_seconds=45.0),
        storage=Storage(path=None),
        clock=clock,
    )
    cp.start()
    cp.register_node("node-a", Resources(4, 4096, 0), labels={"zone": "a"})
    cp.register_node("node-gpu", Resources(8, 8192, 2), labels={"zone": "b"},
                     capabilities={"cuda": 1})

    batch = [
        ("web-1", TaskSpec("registry/web:1", Resources(1, 1024, 0))),
        ("web-2", TaskSpec("registry/web:1", Resources(1, 1024, 0))),
        ("gpu-1", TaskSpec("registry/train:1", Resources(2, 2048, 1),
                           required_capabilities=frozenset({"cuda"}))),
        ("big-1", TaskSpec("registry/web:1", Resources(64, 65536, 0))),
        ("zone-x", TaskSpec("registry/web:1",
                            required_labels=frozenset({("zone", "x")}))),
    ]
    decisions = {}
    for task_id, spec in batch:
        decisions[task_id] = cp.submit_task(task_id, spec)
    # duplicate of web-1
    dup = cp.submit_task("web-1", TaskSpec("registry/web:1"))

    print("== placement ==")
    for tid, d in decisions.items():
        if d.accepted:
            print(f"{tid}: RUNNING on {d.node_id} token={d.fencing_token}")
        else:
            print(f"{tid}: REJECTED {d.reject_reason.value} :: {d.reject_detail}")
    print(f"web-1 duplicate: REJECTED {dup.reject_reason.value}")

    print("\n== lease lifecycle ==")
    cp.renew_lease("web-1", decisions["web-1"].fencing_token)
    print("web-1 renewed")
    clock.advance(100)  # web-2 lease and node heartbeats expire
    events = cp.reconcile()
    for e in events:
        print(f"event {e.kind}: task={e.task_id} node={e.node_id} {e.detail}")

    print("\n== final accounts (conserved) ==")
    for node_id in sorted(cp.snapshot().nodes):
        node = cp.get_node(node_id)
        print(f"{node_id}: used cpu={node.used.cpu} mem={node.used.memory_mb} "
              f"gpu={node.used.gpu} / cap cpu={node.capacity.cpu} "
              f"state={node.state.value} running={sorted(node.running_task_ids)}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.WARNING)
    main()
