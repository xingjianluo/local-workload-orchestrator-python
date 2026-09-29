# 语义说明：调度规则、租约与对账

本文档给出控制面的精确语义，便于代码审查与测试对照。

## 1. 状态机

### 1.1 任务 `TaskState`

```
PENDING ──调度成功──▶ RUNNING ──complete──▶ COMPLETED
   │                    │
   │调度失败            ├─ 节点 DOWN/消失（对账）─▶ LOST ──重调成功──▶ RUNNING
   ▼                    └─ 节点存活但租约过期（对账）─▶ 原位续约，仍为 RUNNING
REJECTED（带 reject_reason/detail，终态）
```

- `REJECTED` 是终态：拒绝原因被持久化，可事后查询，不会被后台对账“悄悄复活”。
- `LOST` 表示“执行权被回收、等待重新调度”，不是失败终态；节点恢复后的下一轮
  对账会重新安置它。

### 1.2 节点 `NodeState`

- `ONLINE`：可参与调度；
- `CORDONED`：不接收新调度，已有任务、租约、占用全部保留；
- `DOWN`：心跳超时或显式判死，不参与调度，其上任务在对账中走 LOST 流程。

重新 `register_node` 一个已存在的节点视为节点进程重启：置 ONLINE、刷新心跳，
但不清任务——是否安全由对账依据租约裁决。

## 2. 调度判定

对单个任务，调度器输入：任务资源请求、标签选择器、规格版本；候选节点集合。

1. 候选按 `node_id` 升序排列（确定性）。
2. 无 ONLINE 节点 → `NO_AVAILABLE_NODE`。
3. 对每个 ONLINE 节点依次检查：
   1. 标签：`node.labels[k] == v` 对 selector 中每个 `(k,v)` 成立；
   2. 规格：`effective_spec_version ∈ node.supported_versions`；
      - 新调度：`effective = task.spec.spec_version`；
      - 已在运行（存在 `spec_at_schedule`）：`effective = spec_at_schedule`；
   3. 资源：`task.request` 放入 `node.capacity - Σ allocations`。
4. 第一个全部满足的节点即归属；全部失败时聚合原因：
   - 存在任一节点通过步骤 3.1+3.2 → `NO_CAPACITY`（硬约束可满足，只是没容量）；
   - 否则 → `CONSTRAINT_CONFLICT`。

资源一律用**整数**（cpu_millis、memory_mib），避免浮点误差导致的超卖。

### 2.1 批次稳定性

- 任务的先后顺序由持久化的单调计数器 `submission_seq` 决定，
  平局按 `task_id` 字典序；
- 节点评估顺序固定按 `node_id`；
- 因此“同样的节点集合 + 同样的任务集合”，无论调用顺序/线程交错如何，
  最终归属一致（测试 `test_repeated_batch_scheduling_is_stable_and_reproducible`）。

## 3. 租约

租约字段：`task_id, node_id, holder, epoch, acquired_ms, expires_at_ms`。

- **授予（grant）**：调度成功时签发，`expires_at = now + lease_ttl_ms`，epoch=1。
- **续约（renew）**：仅延长 `expires_at`，holder/epoch 不变；
  `now >= expires_at` 即拒（`LeaseExpiredError`）。
- **执行权转移（regrant）**：只发生在对账确认节点不可用后：
  旧租约 `epoch += 1`、`expires_at = now`（立即失效），由控制面以新持有者
  `reconciler` 重新授予。旧持有者随后任何带旧 epoch 的续约/完成都返回
  `LEASE_EXPIRED`，从协议上杜绝“两个执行者同时写”。

续约/完成的校验顺序（service 层）：

1. 任务存在且 RUNNING、租约存在；
2. **epoch 匹配**（不匹配 → LEASE_EXPIRED，优先于 holder 校验，
   这样即使新旧持有者偶然同名也能被准确识别为过期执行权）；
3. holder 匹配（不匹配 → LEASE_ALREADY_HELD）；
4. 租约未过期。

## 4. 对账算法

输入：持久化快照 + `now_ms`。步骤：

1. **心跳超时检测**：对 ONLINE/CORDONED 节点，
   `now - last_heartbeat_ms > heartbeat_timeout_ms` → 置 DOWN。
2. **逐任务裁决**（仅处理 RUNNING 且有租约的任务）：
   - 租约所在节点为 DOWN/不存在：作废租约（epoch+1），任务 → LOST；
   - 节点存活但租约已过期：**原位续约**
     （新 `expires_at = now + TTL`，保留 holder/epoch），任务保持 RUNNING；
   - 租约有效：不动。
3. **重建资源账**：清空所有节点 allocations，仅对“RUNNING 且持有此刻有效租约”
   的任务按其租约节点重新记账。LOST 任务旧占用被清除，存活任务占用精确恢复。
4. **重新调度**：对 LOST/PENDING 任务按 `(submission_seq, task_id)` 排序，
   逐个用调度器在**当前账本**上选节点；成功则以新 epoch 授予租约并记账，
   失败则保持 LOST/PENDING。

设计要点：

- **不抢占**：步骤 2 中健康节点上的任务先保住资源，步骤 4 只能在空闲容量里
  安置 LOST 任务，因此节点故障引发的迁移不会挤掉健康任务。
- **不重复执行**：旧持有者受 epoch 封锁；账本以有效租约重建，每任务至多一笔。
- **不丢失**：LOST/PENDING 任务持久化，每轮对账都会重试；节点恢复即能回来。
- **幂等**：在同一 `now_ms` 重复对账，第二次报告为空、状态与账本不变。

## 5. 并发与一致性

- 控制面用一把 `RLock` 串行化所有写操作（提交、续约、完成、心跳、升级、对账）。
- 每次写操作：改内存快照 → `revision += 1` → 存储层原子落盘 → 返回。
  JSON 存储采用“临时文件 + fsync + os.replace”，保证不会读到半写文件。
- 账本守恒 `resource_ledger_balances()` 的定义：
  对每个节点，`allocations` 必须**恰好**等于该节点上 RUNNING 任务的请求集合；
  每个 RUNNING 任务的 `assigned_node` 必须存在。
- “不超容量”与“账本守恒”刻意分离：
  显式容量收缩可造成可解释超配，由 `capacity_violations()` 报告超出量，
  但调度器不会再往超配节点放新任务（剩余量为负即不足）。

## 6. 时间模型与重启

- 使用随快照持久化的整数逻辑时钟 `clock_ms`。这保证“重启后”时间单调可比，
  不会因进程重启把时钟归零而误判租约。
- 推荐启动序列：构造 service（加载快照）→ 立即 `reconcile()`：
  - 停机时间很短：租约仍有效，对账为空操作；
  - 停机超过 TTL 但节点仍在心跳/存活：原位续约；
  - 节点已超时：走 DOWN → LOST → 重调。
- 生产部署可把 `advance_clock`/对账替换为墙钟定时器驱动，领域语义不变。

## 7. 观测性

- 调度日志三段式：`[调度输入]`（需求+候选画像）、`[判定依据]`
  （逐节点接受/拒绝理由）、`[最终归属]`（节点或拒绝原因）。
- 对账日志：节点心跳超时、租约作废/原位续约、重调结果（holder/epoch/到期时间）。
- `GET /health` 直接返回 `ledger_balances`，可作为巡检与测试断言钩子。
