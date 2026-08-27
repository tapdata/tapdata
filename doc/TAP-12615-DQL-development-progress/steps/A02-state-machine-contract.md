# A02 冻结事件、批次和 Attempt 状态机

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依据：详细设计第 5.1、5.2、7.9、11 和 17 节

## 事件状态机

| 当前状态 | 允许的下一状态 | 触发条件 |
| --- | --- | --- |
| 新事件 | `PENDING` | Payload 完整且满足重处理前提 |
| 新事件 | `NOT_REPROCESSABLE` | Payload 不完整或拓扑不支持重处理 |
| `PENDING` | `REPROCESSING` | TM 成功锁定到一个恢复批次 |
| `RECOVERY_FAILED` | `REPROCESSING` | 用户再次发起并由 TM 成功锁定 |
| `REPROCESSING` | `RECOVERED` | Engine 上报当前批次单事件成功 |
| `REPROCESSING` | `RECOVERY_FAILED` | Engine 上报失败、超时或批次补偿释放 |

`RECOVERED` 和 `NOT_REPROCESSABLE` 为事件终态。状态更新必须同时校验 `current_batch_id`，旧批次回调不能修改已被其他批次持有的事件。

## 批次状态机

| 当前状态 | 允许的下一状态 | 触发条件 |
| --- | --- | --- |
| 新批次 | `CREATED` | 预览复核通过并写入批次 |
| `CREATED` | `DISPATCHED` | 事件全部锁定且消息下发成功 |
| `CREATED` | `FAILED`、`CANCELED` | 锁定、校验或下发前失败 |
| `DISPATCHED` | `RUNNING` | Engine 上报 `BATCH_STARTED` |
| `DISPATCHED` | `FAILED`、`PARTIAL_FAILED` | 下发后失败或超时补偿 |
| `RUNNING` | `SUCCESS`、`PARTIAL_FAILED`、`FAILED` | Engine 完成、失败或 TM 超时汇总 |

批次终态为 `SUCCESS`、`PARTIAL_FAILED`、`FAILED`、`CANCELED`。终态不能被迟到的普通回调覆盖。

## Attempt 和回调规则

- Attempt 结果固定为 `RUNNING`、`SUCCESS`、`FAILED`、`SKIPPED`、`TIMEOUT`。`RUNNING` 表示当前重放仍在执行，允许 `finishedAt` 为空。
- 回调类型固定为 `BATCH_STARTED`、`EVENT_STARTED`、`EVENT_RESULT`、`BATCH_FINISHED`、`BATCH_FAILED`。
- `EVENT_STARTED` 只记录开始信息，不增加成功、失败或跳过计数。
- `EVENT_RESULT` 以 `batchId + eventId + attemptId` 幂等处理。
- 批次数量必须最终满足 `selected_count = success_count + failed_count + skipped_count`。

## TTL 联动

事件或批次发生合法状态推进时，`updated` 和 `ttl_at` 使用同一个时间值原子更新；非法或重复回调不得刷新 TTL。

## 已识别的代码跟进

- D06-D08 需要将上述迁移约束落实为条件更新和超时补偿。
- 当前枚举已经覆盖状态值，但终态保护和重复回调幂等仍需要在 D06/D07 完成。
