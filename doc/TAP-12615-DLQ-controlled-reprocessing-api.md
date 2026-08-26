# TAP-12615 DLQ 异常事件接口文档

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

`MALFORMED_RECORD`、`POISON_RECORD`、`TRANSFORM_ERROR`、`TARGET_WRITE_ERROR`、`UNKNOWN_RECORD_ERROR`

### DqlRecoveryBatchStatus

`CREATED`、`DISPATCHED`、`RUNNING`、`SUCCESS`、`PARTIAL_FAILED`、`FAILED`、`CANCELED`

### DqlRecoveryAttemptResult

`SUCCESS`、`FAILED`、`SKIPPED`、`TIMEOUT`

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
  "payloadFormat": "tap-record-event-json-v1",
  "payloadData": {},
  "payloadHash": "sha256:...",
  "payloadSize": 2048,
  "payloadComplete": true,
  "payloadPreview": {},
  "payloadPreviewTruncated": false,
  "errorType": "TRANSFORM_ERROR",
  "errorCode": "JS_PROCESS_FAILED",
  "errorDetails": "script failed",
  "rawErrorRef": "optional-log-ref"
}
```

说明：

- `captureSeq` 为空时 TM 从任务 `attrs.dqlEventSeq` 原子递增分配。
- `eventIdentity` 为空时 TM 基于任务、表、DML、时间、payload hash 等生成。
- `payloadComplete=false` 时事件状态为 `NOT_REPROCESSABLE`。

响应 `data`：

```json
{
  "eventId": "DQL-64f000-000001",
  "status": "PENDING",
  "duplicate": false
}
```

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

## 5. 查询异常事件详情

```http
GET /api/dql-events/{eventId}
```

响应 `data` 包含列表字段外的：

- `payloadPreview`
- `errorDetails`
- `recoveryAttempts`
- `currentBatch`

说明：

- 不会返回完整 `payloadData`，完整 payload 只给 Engine 回放使用。
- 当事件处于 `REPROCESSING` 且存在 `currentBatchId` 时，`currentBatch` 返回当前批次摘要，用于前端“查看进度”入口。
- `recoveryAttempts` 最多返回最近 20 条，用于详情抽屉的重处理历史。

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
      "sourceTable": "orders"
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
  "orderedEventIds": ["DQL-64f000-000001", "DQL-64f000-000002"]
}
```

当前第一步已创建批次、锁定事件并预留 `dqlRecovery` 消息下发结构；Engine 侧实际回放处理在后续步骤接入。

## 9. 查询重处理批次

```http
GET /api/dql-events/recovery-batches/{batchId}
```

响应 `data`：`DqlRecoveryBatchDto`，包含批次 ID、任务、状态、选择数量、成功/失败/跳过数量、事件 ID 列表、开始/结束时间和消息。

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
