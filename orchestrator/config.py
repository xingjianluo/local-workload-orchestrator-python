"""控制面配置项。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrchestratorConfig:
    """编排控制面可调参数。

    - lease_ttl_ms: 租约有效期。执行者必须在此周期内续约，
      超过该时长未续约的租约在对账时判定过期。
    - heartbeat_timeout_ms: 节点心跳超时。超过该时长未心跳的节点
      在对账时标记为 DOWN，其上任务进入重新对账流程。
    - reconcile_interval_ms: 建议的后台对账周期（仅文档/驱动层使用）。
    - storage_path: 持久化文件路径；None 表示纯内存（测试用）。
    """

    lease_ttl_ms: int = 10_000
    heartbeat_timeout_ms: int = 30_000
    reconcile_interval_ms: int = 5_000
    storage_path: str | None = None
