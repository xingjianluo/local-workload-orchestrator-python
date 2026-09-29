# local-workload-orchestrator-python

本地工作负载编排控制面：用本地数据模拟节点与任务，跑通**资源/标签调度、
租约与故障恢复**。

## 特性

- 按资源、标签、能力确定性选节点；资源不足 / 节点不可用 / 约束冲突分别给出
  可区分的拒绝原因；同一批任务多次调度结果稳定可复现。
- 运行任务携带带单调递增 **fencing token** 的租约；控制面重启、节点心跳
  超时后重新对账，任务不重复、不丢失、资源不超卖。
- 单把锁串行化所有变更，已用并发压测验证不超卖、资源账守恒。
- 节点/任务规格升级有可解释的兼容判定，老任务不被清除、不被悄悄迁移。
- 调度审计日志打印输入、逐节点判定依据与最终归属。

## 快速开始

```bash
uv sync
uv run pytest                          # 单元/并发/恢复测试
PYTHONPATH=. uv run python examples/demo.py   # 端到端本地模拟
uv run uvicorn main:app --reload       # HTTP 服务
```

## 文档

设计与语义（调度规则、租约与对账、配置项与验证方法）见
[docs/orchestration.md](docs/orchestration.md)。
