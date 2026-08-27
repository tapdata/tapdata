# TAP-12615 D05 `dqlRecovery` 消息设计

## 目标

为 TM 到 Engine 的重处理批次下发定义独立、可验证的消息契约，复用现有 pipe/WebSocket 队列传输，不复用 `DataSyncMq.opType`，并确保批次状态先落库为 `DISPATCHED` 后才发送命令。

## 消息契约

消息 `data` 使用 `DqlRecoveryMessageDto`，字段为：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `type` | string | 固定为 `dqlRecovery`，用于 Engine handler 路由 |
| `taskId` | string | 原任务 ID |
| `batchId` | string | TM 重处理批次 ID |
| `taskVersion` | long | 发起时任务版本；非严格测试/兼容路径从事件快照取值 |
| `orderedEventIds` | string[] | D02 固化的唯一可信回放顺序 |
| `operatorId` | string | 发起操作人 ID |
| `operatorName` | string | 发起操作人名称 |
| `mode` | string | 固定初始值 `AUTO`，Engine 根据任务运行态选择执行模式 |

禁止字段：`opType`、DataSync 生命周期操作值和未排序的 `eventIds`。

## 发送顺序与失败语义

1. D04 已创建批次并完成所有事件锁定。
2. TM 条件更新批次为 `DISPATCHED`。
3. TM 将类型 DTO 转为稳定顺序的 Map，通过 `MessageQueueService.sendPipeMessage(payload, "tm", agentId)` 发送到批次记录中的 Agent。
4. 状态更新或消息发送抛出异常时，沿用 D04 的原始事件状态释放、批次 `FAILED` 和任务锁释放补偿。

## 兼容边界

- 不修改 `DataSyncMq` 和 `dataSync` handler。
- 本步骤只实现 TM 侧契约和下发；Engine `dqlRecovery` handler 在 E01 实现。
- 消息采用现有离线 Agent 队列能力，发送方法成功仅表示已交给现有传输层；Agent 接受和结果回调由 E01、D06 处理。
