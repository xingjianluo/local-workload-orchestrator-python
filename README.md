# 本地工作负载编排控制面（Local Workload Orchestrator）

用本地数据模拟工作节点与任务的一套最小编排控制面，跑通三件核心事情：

1. **调度**：按资源余量、标签选择器、规格版本为任务选择节点；不满足时给出
   可区分的拒绝原因；相同输入多次调度结果稳定可复现。
2. **租约与故障恢复**：运行中任务持有带 TTL 与 epoch 的租约；控制面重启、
   节点心跳超时后以租约为唯一事实对账，任务不重复执行、不丢失、资源不重复占用。
3. **升级兼容**：节点能力或任务规格升级时，老任务不被清退、不会静默漂移到
   不合适的节点，兼容判定结果带解释。

## 目录结构

```
orchestrator/
  models.py         领域模型（节点/任务/租约/资源/状态/拒绝原因）
  errors.py         可区分的错误类型
  config.py         配置项（TTL、心跳超时、存储路径等）
  compatibility.py  规格版本兼容判定（可解释）
  storage.py        内存 / JSON 文件持久化（原子写）
  scheduler.py      调度器（资源 + 标签 + 规格，确定性排序）
  lease.py          租约授予 / 续约
  reconcile.py      心跳超时与重启后的对账
  service.py        控制面门面（全局锁、资源账、时钟、对外操作）
  api.py            FastAPI 接口层
tests/              调度、租约/对账、并发、升级兼容、接口、集成测试
docs/
  SEMANTICS.md      调度规则、租约与对账语义的详细说明
```

## 快速开始

```bash
uv sync
uv run python main.py            # 启动 HTTP 服务（端口 0 = 系统分配）
uv run pytest                     # 运行全部测试（默认打印调度日志）
```

## 调度规则（摘要）

提交任务时，调度器对 **ONLINE** 节点按 `node_id` 升序逐个评估，判定顺序为：

1. **节点可用性**：没有任何 ONLINE 节点 → `NO_AVAILABLE_NODE`；
2. **标签选择器**（硬约束）：节点 labels 必须包含 selector 的全部键值；
3. **规格版本**（硬约束）：节点必须显式支持任务的 `spec_version`；
4. **资源余量**：任务请求必须放入节点剩余容量。

- 选择第一个完全满足的节点（`node_id` 最小），因此**同输入同结果**。
- 全部不满足时的拒绝原因聚合规则：
  - 至少有一个节点通过标签+规格硬约束（只是装不下）→ **`NO_CAPACITY`（资源不足）**；
  - 没有任何节点通过硬约束 → **`CONSTRAINT_CONFLICT`（约束冲突）**；
  - 没有 ONLINE 节点 → **`NO_AVAILABLE_NODE`（节点不可用）**。
- 任务按 `(submission_seq, task_id)` 稳定排序，`submission_seq` 由提交顺序
  单调分配，保证“同一批任务多次调度”归属一致、可复现。

每次调度都会打印三类日志：`[调度输入]`、`[判定依据]`、`[最终归属]`。

## 租约与对账语义（摘要）

- 任务成功调度即获得租约：`holder`（执行者标识）+ `epoch`（执行权代号）+
  `expires_at_ms`。执行者必须在 TTL 内续约。
- 续约 / 完成要求 **holder 与 epoch 同时匹配且租约未过期**，任一不符即拒绝：
  - epoch 不符 → `LEASE_EXPIRED`（执行权已转移，旧持有者的写操作一律失效）；
  - holder 不符 → `LEASE_ALREADY_HELD`；
  - 租约过期 → `LEASE_EXPIRED`。
- 对账（启动时 / 心跳超时 / 定时）以**有效租约**为唯一事实重建资源账，分三类：

  | 情况 | 处理 | 任务 | 资源账 |
  |------|------|------|--------|
  | 节点 DOWN / 消失 | 租约作废 epoch+1，换持有者重授 | LOST → 重新调度 | 旧节点清账，新节点记账 |
  | 节点存活但租约过期 | **原位续约**，holder/epoch 不变 | 继续 RUNNING，不迁移 | 保留 |
  | 租约有效 | 不动 | RUNNING | 保留 |

  关键点：健康节点上的任务资源先原位锁定，LOST 任务重调度只能使用真正空闲的
  容量，**绝不抢占存活任务**；没有可用落点时保持 LOST，等待节点恢复后的下一轮
  对账。整个对账幂等，可重复执行。
- 所有状态变更在一把全局 `RLock` 内完成并先持久化再返回，配合整数资源向量，
  保证并发提交 / 续约下不超卖、账目守恒（见 `resource_ledger_balances()`）。

## 升级兼容（摘要）

- 任务调度成功时锁定 `spec_at_schedule`。运行中任务始终按锁定版本判定：
  任务声明规格升级不会让它切换版本或漂移；节点世代（generation）变化也不影响。
- 节点 `upgrade_node()` 只做加法（增加支持版本 / 调整容量）并递增 generation，
  已有任务与占用全部保留。
- 新规格（如 v2）任务必须找到显式支持 v2 的节点，否则 `CONSTRAINT_CONFLICT`，
  不会静默落到只支持 v1 的节点。
- 容量收缩导致的 `allocated > capacity` 属于管理员显式操作，老任务保留，
  超配量由 `capacity_violations()` 显式暴露，与“账本守恒”分开报告。

## 配置项

| 配置 | 默认 | 用途 | 如何验证 |
|------|------|------|----------|
| `lease_ttl_ms` | 10000 | 租约有效期；超期未续约在对账时裁决 | 推进时钟超过 TTL 后调用 `/reconcile` |
| `heartbeat_timeout_ms` | 30000 | 节点心跳超时，超过则判 DOWN | 停止心跳并推进时钟，观察节点状态与任务 LOST |
| `reconcile_interval_ms` | 5000 | 建议的后台对账周期（驱动层使用） | 自定义循环中按此间隔调用 `reconcile()` |
| `storage_path` | `None` | JSON 持久化路径；`None` 为纯内存 | 指定路径后用“新 service 实例”模拟重启对账 |

> 时间使用**随状态持久化的逻辑时钟（毫秒）**，因此跨进程重启后时间仍然可比。
> 测试中用 `advance_clock()` 精确控制；生产中可替换为墙钟驱动。

## HTTP 接口与错误码

| 方法 & 路径 | 说明 |
|-------------|------|
| `GET  /health` | 存活状态、当前时钟、资源账是否守恒 |
| `POST /nodes` | 注册 / 重新上线节点 |
| `POST /nodes/{id}/heartbeat` | 节点心跳 |
| `POST /nodes/{id}/cordon` | 封锁节点（不接新任务，保留存量） |
| `POST /nodes/{id}/upgrade` | 升级容量 / 支持版本 |
| `GET  /nodes`、`/nodes/{id}` | 查询节点（含 allocated/available） |
| `POST /tasks` | 提交任务并立即调度 |
| `POST /tasks/{id}/renew` | 续约 |
| `POST /tasks/{id}/complete` | 完成并释放资源 |
| `POST /reconcile` | 触发对账 |
| `GET  /tasks`、`/tasks/{id}` | 查询任务（含 reject_reason/detail） |

错误码（HTTP 状态码 + body.code）：

| 异常 | HTTP | code |
|------|------|------|
| 资源不足 | 409 | `NO_CAPACITY` |
| 无可用节点 | 503 | `NO_AVAILABLE_NODE` |
| 约束冲突 | 422 | `CONSTRAINT_CONFLICT` |
| 租约/epoch 过期 | 410 | `LEASE_EXPIRED` |
| 持有者冲突 | 409 | `LEASE_ALREADY_HELD` |
| 不存在 | 404 | `NOT_FOUND` |

## 验证方法

```bash
uv run pytest                                  # 全量：37 个用例
uv run pytest tests/test_concurrency.py -q     # 并发提交/续约，资源守恒
uv run pytest tests/test_lease_reconcile.py -q # 续约拒绝、心跳超时、重启恢复
uv run pytest tests/test_scheduler.py -q       # 三类拒绝与稳定可复现
uv run pytest tests/test_compatibility.py -q   # 升级兼容、老任务保留
```

所有用例结束时都会断言 `resource_ledger_balances()`（节点占用与 RUNNING 任务
一一对应），并发用例额外断言总量不超容量、成功任务数精确等于可容纳数。
