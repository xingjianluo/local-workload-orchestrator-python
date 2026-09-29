"""规格版本与能力的兼容判定。

升级场景：
- 节点能力升级（supported_versions 增加新版本）不影响已运行的老任务；
- 任务规格升级（spec_version 变大）后，只能调度到明确支持该版本的节点，
  绝不悄悄落到不支持的节点上；
- 判定结果必须带解释，便于日志与测试断言。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompatibilityVerdict:
    """节点-任务规格兼容判定结果。"""

    compatible: bool
    detail: str


def check_spec_compatibility(
    node_supported_versions: frozenset[int],
    spec_version: int,
    node_generation: int,
    task_locked_version: int | None,
) -> CompatibilityVerdict:
    """判定节点能否承载该任务的规格版本。

    生效版本（effective version）规则：
    - 任务已经在运行（``task_locked_version`` 非空）时，始终按调度时
      锁定的版本判定——节点能力升级不会清掉老任务，任务规格升级也不会
      让在跑的任务悄悄切换到新版本；
    - 新调度/重新调度时按任务当前 ``spec_version`` 判定，节点必须显式
      声明支持该版本，绝不静默落到不支持的节点上。

    ``node_generation`` 仅进入解释文本，用于追溯判定时的节点能力世代。
    """
    if task_locked_version is not None:
        effective = task_locked_version
        basis = f"运行中任务沿用调度时锁定版本 v{task_locked_version}，忽略当前声明版本 v{spec_version}"
    else:
        effective = spec_version
        basis = f"新调度按任务当前规格版本 v{spec_version} 判定"

    if effective in node_supported_versions:
        return CompatibilityVerdict(
            compatible=True,
            detail=(
                f"兼容：生效版本 v{effective} 在节点支持集合 "
                f"{sorted(node_supported_versions)} 内（节点世代 gen={node_generation}；{basis}）"
            ),
        )
    return CompatibilityVerdict(
        compatible=False,
        detail=(
            f"不兼容：生效版本 v{effective} 不在节点支持集合 "
            f"{sorted(node_supported_versions)} 内（节点世代 gen={node_generation}；{basis}）"
        ),
    )
