"""调度器：按资源、标签与规格版本为任务选择节点。

关键语义：
- 明确区分三类拒绝：资源不足 / 无可用节点 / 约束冲突；
- 确定性：相同输入（节点集合 + 任务集合）多次调度结果一致，
  节点按 node_id 排序评估，任务按 (submission_seq, task_id) 排序；
- 每个任务的调度返回可解释的 SchedulingDecision。
"""

from __future__ import annotations

import logging

from .compatibility import check_spec_compatibility
from .models import Node, NodeState, RejectReason, SchedulingDecision, Task

logger = logging.getLogger("orchestrator.scheduler")


class Scheduler:
    """无状态的节点选择逻辑；状态变更由 service 层负责。"""

    def evaluate(self, task: Task, nodes: list[Node]) -> SchedulingDecision:
        """评估单个任务在给定节点集合上的调度结果。

        判定依据（按顺序）：
        1. 没有 ONLINE 节点 -> NO_AVAILABLE_NODE；
        2. 对节点按 node_id 排序后逐个评估：
           - 标签选择器不匹配 / 规格版本不支持 -> 记为约束冲突依据；
           - 剩余资源不足 -> 记为资源不足依据；
        3. 选第一个完全满足的节点（node_id 最小），保证同输入同结果；
        4. 全部不满足时：出现过约束冲突 -> CONSTRAINT_CONFLICT；
           否则（仅因资源不足）-> NO_CAPACITY。
        """
        ordered = sorted(nodes, key=lambda n: n.node_id)
        evaluated = tuple(n.node_id for n in ordered)

        logger.info(
            "[调度输入] task=%s 需要 cpu=%dms mem=%dMiB selector=%s spec=v%s; "
            "候选节点=%s",
            task.task_id,
            task.request.cpu_millis,
            task.request.memory_mib,
            task.node_selector,
            task.spec.spec_version,
            [
                (n.node_id, n.state.value, f"avail={n.available()}", f"labels={n.labels}",
                 f"versions={sorted(n.supported_versions)}", f"gen={n.generation}")
                for n in ordered
            ],
        )

        online = [n for n in ordered if n.state == NodeState.ONLINE]
        if not online:
            decision = SchedulingDecision(
                task_id=task.task_id,
                accepted=False,
                node_id=None,
                reason=RejectReason.NO_AVAILABLE_NODE,
                detail=f"没有 ONLINE 节点可参与调度（评估节点：{', '.join(evaluated) or '空'}）",
                evaluated_nodes=evaluated,
            )
            logger.info("[判定依据] %s；[最终归属] 拒绝", decision.detail)
            return decision

        saw_constraint = False   # 是否有节点因标签/规格被排除
        saw_capacity = False      # 是否有节点通过硬约束但资源不足
        any_hard_match = False    # 是否至少有一个节点满足标签+规格硬约束
        bases: list[str] = []

        for node in online:
            # 标签选择器：硬约束
            mismatched = {
                k: f"要求 {k}={v}，实际 {node.labels.get(k)!r}"
                for k, v in sorted(task.node_selector.items())
                if node.labels.get(k) != v
            }
            if mismatched:
                saw_constraint = True
                bases.append(f"节点 {node.node_id} 标签冲突: {'; '.join(mismatched.values())}")
                continue

            # 规格版本：硬约束（老任务沿用锁定版本）
            verdict = check_spec_compatibility(
                node.supported_versions,
                task.spec.spec_version,
                node.generation,
                task.spec_at_schedule,
            )
            if not verdict.compatible:
                saw_constraint = True
                bases.append(f"节点 {node.node_id} {verdict.detail}")
                continue

            # 通过全部硬约束
            any_hard_match = True
            available = node.available()
            if not task.request.fits_in(available):
                saw_capacity = True
                bases.append(
                    f"节点 {node.node_id} 资源不足: 需要 cpu={task.request.cpu_millis}ms "
                    f"mem={task.request.memory_mib}MiB，剩余 cpu={available.cpu_millis}ms "
                    f"mem={available.memory_mib}MiB"
                )
                continue

            decision = SchedulingDecision(
                task_id=task.task_id,
                accepted=True,
                node_id=node.node_id,
                reason=None,
                detail=(
                    f"节点 {node.node_id} 满足全部约束：标签匹配、规格 v"
                    f"{task.spec_at_schedule or task.spec.spec_version} 受支持、"
                    f"剩余资源 cpu={available.cpu_millis}ms mem={available.memory_mib}MiB 充足"
                ),
                evaluated_nodes=evaluated,
            )
            logger.info("[判定依据] 候选评估 -> %s；[最终归属] 节点 %s", "; ".join(bases + [decision.detail]) if bases else decision.detail, node.node_id)
            return decision

        # 聚合拒绝原因：
        # - 存在通过标签+规格硬约束的节点（哪怕都装不下）-> 资源不足，
        #   因为约束本身可满足，扩容/等腾挪即可解决；
        # - 没有任何节点满足硬约束 -> 约束冲突。
        reason = (
            RejectReason.NO_CAPACITY
            if any_hard_match
            else RejectReason.CONSTRAINT_CONFLICT
        )
        detail = "所有可用节点均不满足: " + "；".join(bases)
        decision = SchedulingDecision(
            task_id=task.task_id,
            accepted=False,
            node_id=None,
            reason=reason,
            detail=detail,
            evaluated_nodes=evaluated,
        )
        logger.info(
            "[判定依据] %s（约束冲突=%s，资源不足=%s）；[最终归属] 拒绝 reason=%s",
            detail, saw_constraint, saw_capacity, reason.value,
        )
        return decision
