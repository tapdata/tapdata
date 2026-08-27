# E01 `dqlRecovery` 消息 Handler

状态：已完成  
完成日期：2026-08-28  
依赖：D05 `dqlRecovery` 消息下发

## 本步骤完成内容

### 1. 消息解析和边界校验

Engine 新增 `DqlRecoveryMessageParser`，把 TM pipe Map 转换为 `DqlRecoveryMessageDto`，并校验：

- `type=dqlRecovery`；
- `taskId`、`batchId` 非空；
- `taskVersion` 为非负整数；
- `orderedEventIds` 非空、无空值、无重复且不超过 200 条；
- `mode` 缺省时为 `AUTO`，当前只接受 `AUTO`。

事件 ID 列表转换为不可变副本，避免 Handler 启动后被消息 Map 的后续修改影响执行顺序。

### 2. 任务上下文和 Agent 校验

`DqlRecoveryMessageHandler` 在启动协调器前校验任务是否存在、任务 ID 是否匹配、任务版本是否匹配以及任务 Agent 是否为当前 Engine。任务上下文通过既有 `ClientMongoOperator` 从任务集合读取；消息处理失败不会进入协调器，也不会发送回放回调。

### 3. 跨 Handler 幂等和启动语义

WebSocket 框架会按消息反射创建 Handler，因此幂等状态不能只保存在单个 Handler 实例中。本步骤新增进程级 `DqlRecoveryBatchRegistry`，由实际 WebSocket 适配器使用；同一批次在不同 Handler 实例中也只会 claim 一次。

- 协调器尚未成功接收前失败：释放 claim，允许 TM 重试；
- 协调器已接收后 `BATCH_STARTED` 回调失败：保留 claim，后续重复消息返回 `DUPLICATE`，避免重复回放；
- 接受或重复消息统一返回 `dqlRecoveryResult`，非法消息返回错误结果。

### 4. Engine 到 TM 的 BATCH_STARTED 回调

公共 DQL Client/Reporter 增加独立 recovery callback，路径为：

```text
POST /api/task/{taskId}/dql-events/recovery/report
```

`DqlRecoveryReport` 不携带 Payload，仅发送 `batchId`、`type=BATCH_STARTED` 和开始时间。只有 TM 返回 `Boolean.TRUE` 才视为回调成功；异常或空/非成功响应包装为 `DqlEventReportException`。

## 代码产出

- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventHandler.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryMessageHandler.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryMessageParser.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBatchRegistry.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlRecoveryReport.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/io/tapdata/dql/client/DqlTmClient.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/iengine/iengine-common/src/main/java/io/tapdata/dql/reporter/DqlEventReporter.java`

## 验证结果

通过 Engine 依赖闭包定向回归：

```text
DqlTmClientTest                 Tests run: 5, Failures: 0, Errors: 0
DqlEventReporterTest            Tests run: 8, Failures: 0, Errors: 0
DqlRecoveryMessageHandlerTest   Tests run: 7, Failures: 0, Errors: 0
DqlRecoveryMessageParserTest    Tests run: 2, Failures: 0, Errors: 0
BUILD SUCCESS
```

覆盖了正常接受、非法消息、任务上下文不匹配、同实例重复、跨 Handler 实例重复、协调器启动失败重试以及回调失败后的保守幂等语义。

## 后续依赖

E02 在本步骤的消息边界之上新增 `TapdataDqlRecoveryEvent`，从 DQL 存储快照重建 I/U/D 事件，并保留原始事件身份、`exactlyOnceId`、批次和 attempt 上下文。E03 再将 `DqlRecoveryCoordinator` 从当前接口接入实际串行回放执行。
