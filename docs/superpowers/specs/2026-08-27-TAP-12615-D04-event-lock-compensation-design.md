# TAP-12615 D04 事件级锁与批次补偿设计

## 目标

在 D03 同任务租约锁的保护下，完成重处理批次创建前后的事件级并发控制：只有完整锁定选中的可重处理事件后才允许下发；锁定数量不足或锁定调用异常时，不下发 Engine 消息，批次进入 `CANCELED`，并恢复已成功锁定事件的原始可重处理状态。

## 现状与约束

- `dql_events.current_batch_id` 是事件级锁的归属字段。
- `lockEvents` 只能更新 `PENDING`、`RECOVERY_FAILED` 且 `current_batch_id` 为空的事件，并通过一次 `updateMulti` 返回实际修改数。
- MongoDB 多文档更新与批次写入不按本步骤引入跨文档事务；本步骤采用应用层补偿，D08 再负责进程异常后的超时恢复。
- 既有 `releaseBatchLocks(batchId, targetStatus)` 继续用于统一目标状态释放；D04 新增按事件恢复原状态的释放路径。

## 方案

1. 先创建 `CREATED` 批次并持有 D03 任务锁。
2. 读取排序后的事件快照，记录每个事件原始状态。
3. 条件更新事件为 `REPROCESSING` 并写入 `current_batch_id`；实际修改数必须等于选中数。
4. 数量一致时更新批次为 `DISPATCHED` 并进入消息下发流程。
5. 数量不足或锁定调用异常时，只按当前批次、`REPROCESSING` 和事件 ID 范围释放已锁事件：原为 `PENDING` 的恢复为 `PENDING`，原为 `RECOVERY_FAILED` 的恢复为 `RECOVERY_FAILED`；批次置为 `CANCELED`，释放任务锁，且不下发消息。
6. 批次已派发但消息发送或派发前状态更新失败时，使用同一份原始状态映射释放事件，批次置为 `FAILED`，释放任务锁。

## 不在本步骤范围

- `dqlRecovery` 消息 DTO 与 Agent 路由（D05）。
- Engine 结果回调和批次状态机（D06-D07）。
- 进程异常后的批次超时扫描（D08）。
