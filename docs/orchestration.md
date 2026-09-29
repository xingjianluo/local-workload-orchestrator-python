# 本地工作负载编排控制面

用本地数据模拟节点与任务的小型编排控制面，打通三件事：

1. **调度**：按资源、标签、能力选择节点，拒绝原因可区分、结果可复现；
2. **租约**：运行任务携带带 fencing token 的租约，支持续约与过期回收；
3. **故障恢复**：控制面重启 / 心跳超时后重新对账，任务不重复、不丢失、
   资源不超卖。

## 1. 模块结构

| 模块 | 职责 |
| --- | --- |
| `orchestrator/models.py` | 领域类型：`Resources`、`Node`、`Task`、`Lease`、决策与枚举 |
| `orchestrator/scheduler.py` | 纯函数式确定性选节点 |
| `orchestrator/compatibility.py` | 资源/标签/能力判定与升级兼容性，结论可解释 |
| `orchestrator/lease.py` | 租约发放、续约、过期与 fencing token 校验 |
| `orchestrator/recovery.py` | 心跳失活、租约撤销、资源账重建 |
| `orchestrator/storage.py` | JSON 快照持久化（临时文件 + 原子 rename，含 fsync） |
| `orchestrator/controlplane.py` | 门面：**单把 `RLock`** 串行化所有变更并持久化 |
| `orchestrator/api.py` | FastAPI 接口层 |
| `orchestrator/trace.py` | 调度审计日志：输入、逐节点判定依据、最终归属 |
| `orchestrator/clock.py` | 时间抽象，测试用 `FakeClock` 精确推进时间 |

## 2. 调度规则

对一次提交，调度器在节点快照上执行**纯函数**判定：

1. 候选节点按 `node_id` 字典序升序遍历；
2. 对每个节点依次检查（短路顺序即下面的排列顺序）：
   1. 节点状态必须为 `ONLINE`（`DRAINING/UNKNOWN` → `NODE_UNAVAILABLE`）；
   2. `required_labels` 中每个 `(key, value)` 必须与节点标签完全相等，
      否则 → `LABEL_CONSTRAINT_CONFLICT`；
   3. `required_capabilities` 中每个能力名必须存在，否则 →
      `CAPABILITY_INCOMPATIBLE`；
   4. 空闲资源 `capacity - used` 在 cpu / memory_mb / gpu 三维上都不小于
      任务需求，否则 → `INSUFFICIENT_RESOURCES`；
3. 第一个可行节点即归属（字典序 tie-break）；全部不可行时，取**有序评估中
   第一个阻断原因**作为拒绝原因，保证多冲突并存时结果仍然稳定；
4. 集群没有任何节点 → `NO_CANDIDATE_NODE`；重复 task_id →
   `DUPLICATE_TASK`。

**可复现性**：候选顺序、谓词顺序、tie-break 全部固定，同一批任务面对相同
集群状态，无论调用次序如何，归属完全一致（见
`tests/test_scheduler.py::test_repeated_decisions_are_reproducible`）。

**资源不超卖**：可行性检查与资源借记发生在控制面同一把锁内，提交即预留；
并发提交时后到者看到的是已借记后的账（见 `tests/test_concurrency.py`，
80 线程争抢 10 个槽位，恰好准入 10 个）。

## 3. 租约语义

* 每次准入发放一个 `Lease(task_id, node_id, fencing_token, acquired_at,
  expires_at, generation)`；
* `fencing_token` 为全局单调递增整数，持久化，重启后继续递增；
* 续约必须出示**当前** token，且租约未过期：
  * token 不符 → `FencingError`（HTTP 409 `STALE_FENCING_TOKEN`）；
  * 已过期 → `LeaseExpiredError`，该任务不会被续约，等待对账回收；
* 任务完成也必须出示当前 token。旧持有者在租约被撤销、token 被新分配取代
  后，其任何完成/续约操作都会被 fencing 拒绝，因此**不会重复执行或重复
  释放资源**。

## 4. 对账（reconciliation）语义

`ControlPlane.start()`（进程启动/重启）与显式 `reconcile()` 都执行：

1. **心跳失活**：`ONLINE` 节点 `now - last_heartbeat >
   heartbeat_timeout_seconds`（或从未心跳）→ 标记 `UNKNOWN`；
2. **撤销租约**：对每个 RUNNING 任务的租约——
   * `now > expires_at`（租约超时），或
   * 所属节点 `UNKNOWN` / 已消失（节点失活）

   则移除租约、任务回到 `PENDING` 等待重新调度，并记录
   `LEASE_REVOKED` 事件（携带被退役的 token）；孤儿租约（没有 RUNNING
   任务背书）记录 `ORPHAN_LEASE_DROPPED`；
3. **重建资源账**：不信任增量计数，直接遍历全部有效 RUNNING 租约 + 任务
   规格，重算每个节点的 `used` 与 `running_task_ids`。因此撤销恰好释放
   一次，崩溃恢复后 `used == Σ RUNNING 预留`，守恒且永不超卖。

任务只有三种有意义的状态路径：`PENDING → RUNNING → COMPLETED`，
或运行中租约失效时 `RUNNING → PENDING`（重新调度）；准入失败为终态
`REJECTED`。任务记录只有一条（以 task_id 为主键），重新调度复用同一条
PENDING 记录，故不重复也不丢失。

## 5. 升级兼容

* 节点能力/容量升级通过 `upgrade_node()` 提升节点 `generation`，**不会**
  驱逐或迁移其上的老任务；
* 任务规格升级通过 `upgrade_task_spec()` 走一次**新的准入判定**
  （`check_upgraded_task`）：新规格在当前节点可行才替换，并发放新
  fencing token；不可行（缺能力、资源不足、generation 倒退等）时保留
  老分配、老规格原样不动，verdict 中带有可读原因。绝不让老任务"悄悄按
  新规格跑到不合适的节点上"。

## 6. 配置项

`Config`（环境变量覆盖）：

| 配置 / 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `state_path` / `ORCH_STATE_PATH` | `data/state.json` | JSON 快照文件；测试传 `""`/`None` 走纯内存 |
| `lease_ttl_seconds` / `ORCH_LEASE_TTL_SECONDS` | `30.0` | 租约有效期 |
| `heartbeat_timeout_seconds` / `ORCH_HEARTBEAT_TIMEOUT_SECONDS` | `45.0` | 心跳多久没到判定节点 `UNKNOWN` |
| `reconcile_interval_seconds` / `ORCH_RECONCILE_INTERVAL_SECONDS` | `5.0` | 建议的后台对账周期（显式 `POST /reconcile` 亦可） |
| `storage_dir` / `ORCH_STORAGE_DIR` | `data` | 数据目录 |

持久化采用"写临时文件 → fsync → `os.replace` 原子替换"，崩溃不会留下
半截文件；无法解析时抛 `StorageCorruptionError`。

## 7. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /nodes/register` | 注册模拟节点（容量、标签、能力） |
| `POST /nodes/{id}/heartbeat` | 节点心跳；`UNKNOWN` 节点重新心跳回到 `ONLINE` |
| `POST /nodes/{id}/drain` | 管理面摘节点，立即撤销其上租约 |
| `POST /tasks/submit` | 提交任务，同步返回归属与 fencing token；拒绝返回 409 |
| `POST /tasks/{id}/renew` | 携带 token 续约 |
| `POST /tasks/{id}/complete` | 携带 token 完成并释放资源 |
| `GET  /tasks` | 任务清单（状态/归属/拒绝原因） |
| `POST /reconcile` | 触发一次对账，返回事件列表 |
| `GET  /health` | 健康检查 |

409 响应体 `detail.reason` 为机器可读原因：`INSUFFICIENT_RESOURCES`、
`NODE_UNAVAILABLE`、`LABEL_CONSTRAINT_CONFLICT`、
`CAPABILITY_INCOMPATIBLE`、`NO_CANDIDATE_NODE`、`DUPLICATE_TASK`、
`STALE_FENCING_TOKEN`、`LEASE_EXPIRED`。

## 8. 验证方法

```bash
uv sync                                  # 安装依赖（含 pytest）
uv run pytest                            # 全部单测，日志含调度判定依据
uv run python examples/demo.py           # 端到端本地模拟（设 PYTHONPATH=.）
uv run uvicorn main:app --reload         # 启动 HTTP 服务
```

测试覆盖：

* **调度**（`tests/test_scheduler.py`）：资源、标签、能力、空集群等各类
  拒绝路径，拒绝原因可区分，重复决策可复现；
* **租约/恢复**（`tests/test_lease_recovery.py`）：续约、过期撤销、心跳
  超时、**控制面重启后加载快照重建账目**、重复/超卖检查；
* **并发**（`tests/test_concurrency.py`）：80 线程并发提交不超卖、同 id
  并发只准入一次、并发续约、提交与完成交织，全部断言资源账守恒；
* **升级兼容**（`tests/test_compatibility.py`）：新老规格/节点升级判定，
  老任务不被清掉；
* **接口**（`tests/test_api.py`）：HTTP 全链路与各类 409 原因码。

单测以 INFO 级别打印调度审计日志：调度输入（镜像、generation、资源需求、
标签/能力约束、候选顺序）、每个节点的可行与否及阻断原因、最终
`ACCEPTED -> node` / `REJECTED [原因]` 归属。
