# TAP-12615 D07 Recovery 回调幂等设计

## 范围

D07 只增强 TM 已有 recovery callback 的幂等和冲突识别，不实现 Engine recovery handler、批次超时扫描或跨文档事务。回调输入仍使用 `DqlRecoveryResultReportVo`，批次状态机沿用 D06。

## 幂等身份

一次事件恢复尝试由以下三元组定位：

```text
batch_id + event_id + attempt_id
```

`attemptId` 在同一批次内必须稳定；`batchId` 防止旧批次的迟到回调修改新批次；`eventId` 防止把结果写到同批次其他事件。

## Repository 条件更新

### EVENT_STARTED

允许条件：

```text
event_id = eventId
status = REPROCESSING
current_batch_id = batchId
recovery_attempts 中不存在 batchId + attemptId
```

满足条件时追加 `RUNNING` attempt 并刷新 `updated`、`ttl_at`，返回 `APPLIED`。条件更新未命中后：已有同身份 attempt 返回 `DUPLICATE`，否则返回 `NOT_IN_BATCH`。

### EVENT_RESULT

成功结果追加 `SUCCESS` attempt 并把事件置为 `RECOVERED`；失败、跳过和超时结果追加对应终态 attempt 并把事件置为 `RECOVERY_FAILED`。两类操作均同时更新当前批次、恢复摘要、`recovery_count` 和 TTL，并以同一 attempt 不存在作为条件。

条件更新未命中后的分类：

| 已有同身份 attempt | 本次请求结果 | 返回 |
| --- | --- | --- |
| 相同终态 | 相同终态 | `DUPLICATE` |
| 不同终态 | 任意不同终态 | `CONFLICT` |
| 无 | 事件不属于当前批次或不在 `REPROCESSING` | `NOT_IN_BATCH` |

`DqlRecoveryCallbackResultEnum` 是 TM Common 中的共享契约。D06 已有 boolean Repository 方法保留，D06 之后的 callback Service 只调用带结果枚举的幂等方法。

## Service 行为

事件结果处理规则：

```text
APPLIED      -> 增加 success/failed/skipped 对应计数，FAILED 触发一次告警
DUPLICATE    -> no-op
CONFLICT     -> DqlRecovery.AttemptConflict
NOT_IN_BATCH -> DqlRecovery.EventNotInBatch
```

批次级状态幂等规则：

| 回调 | 重复状态 | 行为 |
| --- | --- | --- |
| BATCH_STARTED | RUNNING | no-op |
| BATCH_FINISHED | SUCCESS/PARTIAL_FAILED | no-op |
| BATCH_FAILED | FAILED | no-op |

当批次已经进入终态时，Service 允许能够从事件历史中精确匹配的重复事件回调 no-op；不匹配的回调仍返回非法批次状态，避免终态被迟到或冲突结果覆盖。

## 一致性边界

D07 保证同一事件回调在并发或重复投递下只被一个请求应用，Service 也只对该次应用增加批次计数。事件更新和批次计数仍是两个 Mongo 文档更新，跨文档瞬时失败的修复由 D08 超时扫描和后续对账能力收敛，不在本步骤引入 Mongo 事务。

## 测试要求

- Repository：重复启动、重复成功、同 attempt 冲突和历史 boolean 方法兼容。
- Batch Service：活动批次重复事件结果、终态批次迟到重复结果、冲突结果、三类批次重复回调。
- 回归必须覆盖 D06 原有事件状态机和 DQL 其他 Repository/Controller/DTO 测试。
