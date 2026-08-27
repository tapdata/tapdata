# TAP-12615 D06 回调状态机设计

## 范围

D06 只处理 TM 已收到的 Engine recovery callback，不实现 Engine `dqlRecovery` handler、串行回放、超时扫描或重复回调幂等。输入由 `DqlRecoveryResultReportVo` 表示，批次上下文由 `DqlRecoveryBatchDto` 表示。

## 回调状态矩阵

| 回调 | 前置批次状态 | 处理 | 失败保护 |
| --- | --- | --- | --- |
| `BATCH_STARTED` | `DISPATCHED` | 批次条件更新为 `RUNNING` | 其他状态返回 `DqlRecovery.InvalidBatchState` |
| `EVENT_STARTED` | `RUNNING` | 校验事件在批次有序列表中，且按批次锁追加 `RUNNING` attempt | 条件更新未命中返回 `DqlRecovery.EventNotInBatch` |
| `EVENT_RESULT` | `RUNNING` | 严格解析终态结果，条件更新事件并追加终态 attempt，成功后增加对应批次计数 | 结果非法或事件锁不属于当前批次时拒绝 |
| `BATCH_FINISHED` | `RUNNING` | 对账计数后写 `SUCCESS` 或 `PARTIAL_FAILED`，释放任务锁 | `selected != success + failed + skipped` 返回 `DqlRecovery.CountMismatch` |
| `BATCH_FAILED` | `CREATED`/`DISPATCHED`/`RUNNING` | 释放当前批次事件锁，写 `FAILED`，告警并释放任务锁 | 终态批次不可重复失败 |

## 事件归属和原子性

回调首先验证批次存在、路径 taskId 与批次 taskId 一致，再验证 eventId 属于 `orderedEventIds`；没有有序列表的历史批次回退到 `eventIds`。Repository 的事件状态迁移额外使用：

```text
event_id = report.eventId
status = REPROCESSING
current_batch_id = report.batchId
```

因此即使 Service 读取到的批次状态正确，事件已经被其他批次释放或迁移时也不会被错误更新。`EVENT_STARTED` 只追加运行中 attempt；`EVENT_RESULT` 追加终态 attempt 并在同一次更新中释放事件锁、刷新摘要、增加恢复次数。

## 批次完成对账

批次完成只接受非负计数，并要求：

```text
selected_count = success_count + failed_count + skipped_count
```

所有结果为 `SUCCESS` 时批次进入 `SUCCESS`；存在 `FAILED` 或 `SKIPPED` 时进入 `PARTIAL_FAILED`。对账失败不能写终态或释放任务锁，以便后续收到缺失结果或由 D08 超时扫描收敛。

## D07 边界

D06 只保证非法状态和错误归属不会更新；如果同一 `attemptId` 的回调重复到达，D07 负责通过条件更新和 attempt 判重使事件恢复次数、批次计数、attempt 历史和告警均保持幂等。
