# TAP-12615 DLQ 异常事件接口文档

本文已按 TAP-12615 V1.2 口径更新：DLQ 与任务级重试在同一任务内分层生效，`dql_events` 只保存记录级 DLQ 事件；共享临时异常继续走现有任务级重试，不通过异常事件列表表达。

## 1. 通用约定

- Base URL：沿用 TM 服务地址。
- 返回包装：所有接口返回 `ResponseMessage<T>`。

```json
{
  "reqId": "uuid",
  "ts": 1787700000000,
  "code": "ok",
  "message": null,
  "data": {}
}
```

- 时间字段：请求参数使用毫秒时间戳；响应中的 `Date` 字段按 TM 当前 Jackson 配置输出。
- 业务命名：业务是 DLQ，接口和集合按详细设计使用 `dql-events` / `dql_events`。
- 权限：页面接口需要异常事件菜单权限 `v2_exception_events`，并按任务可见范围过滤；Engine 回调接口沿用内部认证。
- 路由边界：`POST /api/task/{taskId}/dql-events/report` 只接受 `exceptionScope=RECORD` 且 `routeDecision=RECORD_DLQ` 的记录级异常；缺省时 TM 会标准化为 `RECORD` / `RECORD_DLQ`。网络抖动、数据库临时不可用、连接不可用、TM 不可用、任务配置错误等共享或系统异常不得上报为 DLQ 事件。

## 2. 枚举

### DqlEventStatus

| 值 | 文案 | 可重处理 |
| --- | --- | --- |
| `PENDING` | 待处理 | 是 |
| `REPROCESSING` | 重处理中 | 否 |
| `RECOVERED` | 已恢复 | 否 |
| `RECOVERY_FAILED` | 重处理失败 | 是 |
| `NOT_REPROCESSABLE` | 不可重处理 | 否 |

### DqlErrorType

`MALFORMED_RECORD`、`POISON_RECORD`、`TRANSFORM_ERROR`、`TARGET_CONSTRAINT_ERROR`、`UNKNOWN_RECORD_ERROR`

### DqlExceptionScope

`RECORD`、`TASK_SHARED`、`SYSTEM`、`UNKNOWN`

说明：`dql_events` 主记录只允许保存 `RECORD`。其他 scope 用于 Engine 分类、日志和告警，不进入本接口的页面查询结果。

### DqlRouteDecision

`RECORD_DLQ`、`TASK_RETRY`、`TASK_ERROR`

说明：`dql_events` 主记录只允许保存 `RECORD_DLQ`。`TASK_RETRY` 和 `TASK_ERROR` 通过任务状态、任务告警和日志表达。

### DqlRecoveryBatchStatus

`CREATED`、`DISPATCHED`、`RUNNING`、`SUCCESS`、`PARTIAL_FAILED`、`FAILED`、`CANCELED`

### DqlRecoveryAttemptResult

`SUCCESS`、`FAILED`、`SKIPPED`、`TIMEOUT`

### DqlRecordIdentityType

`PRIMARY_KEY`、`UNIQUE_INDEX`、`FULL_FIELD_HASH`、`UNKNOWN`

说明：Engine 生成 `recordIdentity` 时按“主键优先、无主键使用唯一索引、无主键无唯一索引使用全字段 hash”的顺序确定同一业务记录。TM 只保存和匹配该身份，不反向解析完整业务字段。

## 3. Engine 上报异常事件

```http
POST /api/task/{taskId}/dql-events/report
```

请求体：

```json
{
  "taskRecordId": "64f...",
  "taskName": "sync_order",
  "taskVersion": 7,
  "agentId": "agent-1",
  "sourceNodeId": "source-node",
  "sourceNodeName": "mysql_src",
  "failedNodeId": "js-node",
  "failedNodeName": "JS Processor",
  "failedStage": "PROCESSOR",
  "sourceTable": "orders",
  "targetTable": "orders_sink",
  "tableId": "orders",
  "dmlType": "U",
  "eventTime": 1787580000000,
  "captureSeq": 1,
  "eventKey": { "id": 1001 },
  "eventKeyMissing": false,
  "eventIdentity": "sha256:...",
  "recordIdentity": "key:orders:id=1001",
  "recordIdentityType": "PRIMARY_KEY",
  "recordIdentityFields": ["id"],
  "payloadFormat": "tap-record-event-json-v1",
  "payloadData": {},
  "payloadHash": "sha256:...",
  "payloadSize": 2048,
  "payloadComplete": true,
  "payloadPreview": {},
  "payloadPreviewTruncated": false,
  "errorType": "TRANSFORM_ERROR",
  "errorCode": "JS_PROCESS_FAILED",
  "exceptionScope": "RECORD",
  "routeDecision": "RECORD_DLQ",
  "classificationReason": "JS process failed on single TapRecordEvent",
  "classificationConfidence": "RULE",
  "errorDetails": "script failed",
  "rawErrorRef": "optional-log-ref"
}
```

说明：

- `captureSeq` 为空时 TM 从任务 `attrs.dqlEventSeq` 原子递增分配。
- `eventIdentity` 为空时 TM 基于任务、表、DML、时间、payload hash 等生成。
- `recordIdentity` 用于识别同一业务记录的后续成功写入风险。Engine 应优先按主键生成，其次唯一索引，最后全字段 hash；若缺失，TM 会基于 `eventKey` 或 `payloadHash` 做兜底生成，但以 Engine 显式上报为准。
- `payloadComplete=false` 时事件状态为 `NOT_REPROCESSABLE`。
- `exceptionScope` 缺省时按 `RECORD` 保存，`routeDecision` 缺省时按 `RECORD_DLQ` 保存；若显式传入其他值，TM 返回错误，Engine 不允许 skip。
- `classificationReason` 和 `classificationConfidence` 用于页面展示路由依据和误分类排查，不能包含未脱敏敏感数据。

响应 `data`：

```json
{
  "eventId": "DQL-64f000-000001",
  "status": "PENDING",
  "duplicate": false
}
```

### 3.1 Engine 上报后续成功写入

```http
POST /api/task/{taskId}/dql-events/record-success/report
```

该接口由 Engine 在 DLQ 异常跳过开启后、同一任务后续记录成功写入目标端时调用。TM 使用 `recordIdentity` 查找同任务、同记录、尚未恢复完成的前序 DQL 事件，并标记覆盖风险。

请求体：

```json
{
  "taskRecordId": "64f...",
  "sourceTable": "orders",
  "targetTable": "orders_sink",
  "tableId": "orders",
  "eventKey": { "id": 1001 },
  "recordIdentity": "key:orders:id=1001",
  "recordIdentityType": "PRIMARY_KEY",
  "recordIdentityFields": ["id"],
  "dmlType": "U",
  "eventTime": 1787580100000,
  "captureSeq": 12,
  "payloadHash": "sha256:...",
  "successAt": 1787580102300
}
```

响应 `data`：

```json
{
  "marked": true,
  "eventId": "DQL-64f000-000001",
  "recordIdentity": "key:orders:id=1001",
  "overwriteRiskMessage": "该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作"
}
```

说明：

- `marked=true` 表示存在前序未完成 DQL 事件被标记；`marked=false` 表示没有匹配到需要提示风险的事件。
- 匹配范围为同 `taskId`、同 `recordIdentity`，并优先使用 `taskRecordId`、`tableId` 收窄范围。
- TM 标记最新一条时间不晚于成功事件的未完成异常事件，状态范围为 `PENDING`、`REPROCESSING`、`RECOVERY_FAILED`。
- 该接口不会创建新的 DQL 主记录，也不会改变事件状态，只写入覆盖风险提示元数据。

## 4. 查询异常事件列表

```http
GET /api/dql-events
```

查询参数：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `taskId` | String | 任务 ID |
| `taskName` | String | 任务名模糊匹配 |
| `sourceTable` | String | 源表模糊匹配 |
| `targetTable` | String | 目标表模糊匹配 |
| `keyword` | String | 匹配事件 ID、任务名、表名、错误码、错误摘要 |
| `dmlType` | String | `I`、`U`、`D` |
| `errorType` | String | `DqlErrorType` |
| `status` | String | `DqlEventStatus` |
| `startTime` | Long | `failedAt` 起始时间 |
| `endTime` | Long | `failedAt` 结束时间 |
| `skip` | Long | 默认 `0` |
| `limit` | Int | 默认 `10` |
| `order` | String | 默认 `failedAt` 倒序；支持 `-failedAt`、`failedAt`、`-eventTime`、`-recoveryCount`、`-lastRecoveryTime`，也支持 `failed_at desc` 形式 |

响应 `data`：

```json
{
  "total": 1,
  "items": [
    {
      "id": "66c...",
      "eventId": "DQL-64f000-000001",
      "taskId": "64f...",
      "taskName": "sync_order",
      "sourceTable": "orders",
      "targetTable": "orders_sink",
      "dmlType": "U",
      "errorType": "TRANSFORM_ERROR",
      "errorCode": "JS_PROCESS_FAILED",
      "routeDecision": "RECORD_DLQ",
      "classificationReason": "JS process failed on single TapRecordEvent",
      "recordIdentity": "key:orders:id=1001",
      "recordIdentityType": "PRIMARY_KEY",
      "overwriteRisk": true,
      "overwriteRiskMessage": "该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作",
      "laterSuccessAt": 1787580102300,
      "laterSuccessEventTime": 1787580100000,
      "laterSuccessCaptureSeq": 12,
      "laterSuccessDmlType": "U",
      "eventTime": 1787580000000,
      "failedAt": 1787580010000,
      "status": "PENDING",
      "recoveryCount": 0,
      "lastRecoveryTime": null
    }
  ]
}
```

前端安全说明：

- 列表接口不会返回完整 `payloadData`。
- 列表接口不会返回 `recoveryAttempts` 明细，重处理历史只在详情接口展示。
- 列表只返回已经进入 `dql_events` 的记录级事件；共享临时异常、任务级重试中、任务级重试耗尽等场景不会产生列表行。
- 当 `overwriteRisk=true` 时，前端应在列表行、详情抽屉或重处理预览中提示 `overwriteRiskMessage`。

## 5. 查询异常事件详情

```http
GET /api/dql-events/{eventId}
```

响应 `data` 包含列表字段外的：

- `payloadPreview`
- `errorDetails`
- `exceptionScope`
- `routeDecision`
- `classificationReason`
- `classificationConfidence`
- `recordIdentity`
- `recordIdentityType`
- `recordIdentityFields`
- `overwriteRisk`
- `overwriteRiskMessage`
- `laterSuccessAt`
- `laterSuccessEventTime`
- `laterSuccessCaptureSeq`
- `laterSuccessDmlType`
- `recoveryAttempts`
- `currentBatch`

说明：

- 不会返回完整 `payloadData`，完整 payload 只给 Engine 回放使用。
- 当事件处于 `REPROCESSING` 且存在 `currentBatchId` 时，`currentBatch` 返回当前批次摘要，用于前端“查看进度”入口。
- `recoveryAttempts` 最多返回最近 20 条，用于详情抽屉的重处理历史。
- `exceptionScope` 固定为 `RECORD`，`routeDecision` 固定为 `RECORD_DLQ`；页面展示这些字段是为了说明该事件为什么进入 DLQ，而不是展示所有任务异常。

## 6. 查询统计

```http
GET /api/dql-events/summary
```

查询参数同列表接口，忽略 `skip`、`limit`、`order`。

响应 `data`：

```json
{
  "total": 42,
  "pending": 12,
  "reprocessing": 1,
  "recovered": 20,
  "recoveryFailed": 8,
  "notReprocessable": 1
}
```

统计只覆盖 `dql_events` 中的记录级异常事件，不统计共享临时异常的任务级重试次数、任务错误次数或被未知异常保护抑制的事件估算数。

## 7. 重处理预览

```http
POST /api/dql-events/recovery/preview
```

请求体：

```json
{
  "eventIds": ["DQL-64f000-000001", "DQL-64f000-000002"]
}
```

响应 `data`：

```json
{
  "taskId": "64f...",
  "taskName": "sync_order",
  "canSubmit": true,
  "orderedEvents": [
    {
      "eventId": "DQL-64f000-000001",
      "eventTime": 1787580000000,
      "captureSeq": 1,
      "dmlType": "I",
      "sourceTable": "orders",
      "overwriteRisk": true,
      "overwriteRiskMessage": "该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作",
      "laterSuccessAt": 1787580102300,
      "laterSuccessEventTime": 1787580100000,
      "laterSuccessCaptureSeq": 12,
      "laterSuccessDmlType": "U"
    }
  ],
  "blockedEvents": [],
  "message": ""
}
```

规则：

- 只能选择同一任务的事件；跨任务返回 `DqlRecovery.CrossTaskNotAllowed`。
- 可提交状态为 `PENDING` 或 `RECOVERY_FAILED`，且 `payloadComplete != false`。
- 排序固定为 `eventTime ASC, captureSeq ASC, eventId ASC`。
- `orderedEvents[].overwriteRisk=true` 时，前端确认弹窗需要展示覆盖风险提示；该提示不阻塞提交，但用户必须能在确认前看到。
- 任务必须处于 `running` 或 Engine 可回放暂停态；`stop`、未完成初始化、已有恢复批次运行中、任务版本不兼容等场景返回阻塞事件或错误码。
- 同一任务内批量允许跨表；不允许跨任务、跨不相关有序流批量。

## 8. 发起重处理

```http
POST /api/dql-events/recovery
```

请求体：

```json
{
  "eventIds": ["DQL-64f000-000001", "DQL-64f000-000002"],
  "confirm": true
}
```

说明：

- `confirm` 必须为 `true`。缺失或为 `false` 时返回 `IllegalArgument`，参数为 `confirm`。
- 前端必须先调用预览接口，再在用户确认后调用本接口。

响应 `data`：

```json
{
  "batchId": "DQLB-20260826-092548001",
  "taskId": "64f...",
  "taskName": "sync_order",
  "status": "DISPATCHED",
  "selectedCount": 2,
  "successCount": 0,
  "failedCount": 0,
  "skippedCount": 0,
  "recoveryMode": "LIVE_TASK",
  "taskStatusBefore": "running",
  "sourceReadPaused": false,
  "orderedEventIds": ["DQL-64f000-000001", "DQL-64f000-000002"]
}
```

当前第一步已创建批次、锁定事件并预留 `dqlRecovery` 消息下发结构；Engine 侧实际回放处理在后续步骤接入。

## 9. 查询重处理批次

```http
GET /api/dql-events/recovery-batches/{batchId}
```

响应 `data`：`DqlRecoveryBatchDto`，包含批次 ID、任务、状态、选择数量、成功/失败/跳过数量、事件 ID 列表、开始/结束时间和消息。

建议批次 DTO 补充以下字段，便于前端说明任务状态保护结果：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `recoveryMode` | String | `LIVE_TASK` 或 `RECOVERY_ONLY` |
| `taskStatusBefore` | String | 发起重处理前任务状态 |
| `taskStatusAfter` | String | 批次结束后任务状态 |
| `sourceReadPaused` | Boolean | 是否已暂停普通源读取 |
| `sourceReadResumeResult` | String | 普通源读取恢复结果 |
| `message` | String | 批次级摘要，失败时包含安全错误信息 |

## 10. Engine 回调重处理结果

```http
POST /api/task/{taskId}/dql-events/recovery/report
```

请求体：

```json
{
  "batchId": "DQLB-20260826-092548001",
  "eventId": "DQL-64f000-000001",
  "attemptId": "A-000001",
  "type": "EVENT_RESULT",
  "result": "SUCCESS",
  "message": "Recovered",
  "errorCode": null,
  "errorDetails": null,
  "startedAt": 1787580100000,
  "finishedAt": 1787580102300
}
```

`type` 可选值：

- `BATCH_STARTED`
- `EVENT_STARTED`
- `EVENT_RESULT`
- `BATCH_FINISHED`
- `BATCH_FAILED`

响应 `data`：

```json
true
```

## 11. 前端建议封装

后续前端可新增 `tapdata-web/packages/api/src/core/dql-event.ts`：

```typescript
export function fetchDqlEvents(params: DqlEventQueryParams)
export function fetchDqlEventDetail(eventId: string)
export function fetchDqlEventSummary(params: DqlEventQueryParams)
export function previewDqlRecovery(eventIds: string[])
export function startDqlRecovery(eventIds: string[])
export function fetchDqlRecoveryBatch(batchId: string)
```

本次提交不修改前端代码。

## 12. 错误码

| code | 场景 |
| --- | --- |
| `IllegalArgument` | 请求参数缺失或非法 |
| `NoPermission` | 无异常事件菜单或任务可见权限 |
| `DqlEvent.NotFound` | 事件不存在 |
| `DqlRecovery.CrossTaskNotAllowed` | 选择了多个任务的事件 |
| `DqlRecovery.EventNotReprocessable` | 预览存在阻塞事件仍发起重处理 |
| `DqlRecovery.EventLockFailed` | 发起重处理时事件锁定失败 |
| `DqlRecovery.BatchNotFound` | 批次不存在 |
| `DqlEvent.InvalidRouteDecision` | Engine 上报的 `exceptionScope` 或 `routeDecision` 不允许写入 DLQ |
| `DqlRecovery.TaskNotRunnable` | 任务不处于运行中或可回放暂停态 |
| `DqlRecovery.BatchAlreadyRunning` | 同一任务已有恢复批次运行中 |
| `DqlRecovery.TaskVersionChanged` | 预览后任务版本发生变化，需要重新预览 |
| `DqlRecovery.PayloadIncomplete` | Payload 不完整，无法重处理 |
