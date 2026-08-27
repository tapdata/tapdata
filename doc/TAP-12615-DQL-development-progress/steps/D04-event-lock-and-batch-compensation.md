# D04 事件级锁和批次创建事务补偿

状态：已完成  
完成日期：2026-08-27  
依赖：D02 回放顺序、D03 同任务批次互斥锁

## 本步骤完成内容

### 1. 批次优先和事件级锁

- `DqlRecoveryBatchService.start` 先创建 `CREATED` 批次，并在整个前置流程中持有 D03 任务租约锁。
- `DqlEventRepository.lockEvents` 只允许锁定 `PENDING`、`RECOVERY_FAILED` 且没有 `current_batch_id` 的选中事件，并以一次条件 `updateMulti` 将事件置为 `REPROCESSING`、写入批次 ID、刷新 `updated/ttl_at`。
- 实际锁定数量必须等于排序后的选中数量；数量不一致时不进入消息下发。

### 2. 按原始状态补偿

- 批次锁定前基于事件快照保存每个事件的原始可重处理状态。
- 部分锁定、事件锁调用异常、批次派发前失败时，按原始状态分组释放；只匹配当前批次、`REPROCESSING` 和对应事件 ID，避免误释放其他批次或已完成事件。
- 原状态为 `PENDING` 的事件恢复为 `PENDING`，原状态为 `RECOVERY_FAILED` 的事件恢复为 `RECOVERY_FAILED`。
- 锁定数量不足或锁定异常时批次置为 `CANCELED`；消息派发或派发前状态更新失败时批次置为 `FAILED`；两类路径都会释放 D03 任务锁。
- Engine 已经开始后的 `BATCH_FAILED` 处理仍使用既有统一失败状态和 D03 锁释放逻辑，未提前引入 D06 回调状态机。

## 代码产出

- `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
  - 保留统一目标状态释放方法。
  - 新增按 `eventId -> targetStatus` 分组的释放方法，并保留当前批次与 `REPROCESSING` 条件。
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
  - 保存原始事件状态。
  - 为锁定数量不匹配、锁定异常和派发失败接入差异化补偿。
- 对应 Repository 与 Service 测试覆盖正常锁定、部分锁定、锁定异常、状态恢复和派发失败。

## 设计边界

本步骤实现的是 MongoDB 多文档操作的应用层补偿，不宣称批次文档和事件文档具备跨文档 ACID 事务语义。进程在补偿前退出时，由后续 D08 超时扫描负责清理异常批次、事件锁和任务租约。

## 验证结果

TDD 先行验证：新增的按原始状态释放测试在重载尚未实现时无法编译；实现后恢复为绿色。

定向测试结果：

```text
DqlEventRepositoryTest       Tests run: 14, Failures: 0, Errors: 0
DqlRecoveryBatchServiceTest  Tests run: 21, Failures: 0, Errors: 0
BUILD SUCCESS
```

## 后续依赖

D05 在本步骤产出的批次状态和事件锁语义上定义独立的 `dqlRecovery` 消息 DTO、Agent 路由和发送契约；下发失败必须继续使用本步骤的 `FAILED` 补偿路径。
