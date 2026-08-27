# TAP-12615 DLQ 异常事件接口文档 （异常事件（DLQ）前端交互与 API 数据契约）

|版本|更新时间|
|---|---|
|V1|2026-08-26|
|V2|2026-08-27|

本文依据 TAP\-12753 当前前端实现整理，供后端生成和联调异常事件 API。接口根路径暂定为 `/api/dql-events`，其中 DQL 指 Dead Letter Queue。前端已支持模拟数据模式，以下字段、枚举和行为应视为前后端联调契约。

**接口优先级：**列表、汇总、详情、重处理预览、提交重处理，共 5 个前端依赖接口。重处理预览是提交前的服务端权威校验，不能只依赖前端禁用态。

# 1\. 当前 Web UI 交互

## 1\.1 入口与列表

菜单路径为「高级功能 / 异常事件」。页面首屏展示状态汇总标签和远程分页表格，默认按 `failedAt` 倒序。列表支持刷新和显示列设置，来源表、目标表、事件时间、最近重处理时间默认可隐藏。

|交互|前端行为|后端要求|
|---|---|---|
|状态标签|全部、待处理、处理中、已恢复、恢复失败、不可重处理。切换后同时刷新列表和各状态计数。|汇总必须与当前非状态筛选条件一致，不能返回全量统计。|
|高频筛选|关键词、任务、DML、错误类型在首行；关键词匹配任务或错误码。|支持对应 query 参数。关键词服务端至少覆盖 taskName、errorCode。|
|更多筛选|来源表、目标表、失败时间范围放入弹层，点击「应用筛选」后更新 URL query 并刷新。|支持表名模糊匹配和失败时间闭区间筛选。|
|路由同步|筛选和状态写入 URL query，页面加载时会从 query 恢复筛选。|无额外接口要求，参数命名需稳定。|
|自动刷新|存在「处理中」事件时，列表和汇总每 8 秒被动刷新。|查询接口需幂等且可承受短轮询。|

## 1\.2 事件详情抽屉

用户点击「详情」后按 `eventId` 获取完整事件。事件 ID 仅用于接口定位，不在界面展示。抽屉展示任务和表流向、失败位置、错误详情、Payload 安全预览，以及重处理历史。事件处于 `REPROCESSING` 时，前端每 3 秒刷新详情历史，直至该次重放结束。

## 1\.3 受控重处理

1. 列表可勾选的事件仅限 `PENDING` 或 `RECOVERY_FAILED`，且一次只能属于同一 `taskId`。

2. 单条「重处理」或批量「重处理」先调用预览接口，服务端返回可提交事件、阻塞事件及最终顺序。

3. 确认弹窗展示固定影响说明：使用当前已发布任务配置重处理原始事件；同步可能短暂暂停，完成后恢复；Payload 不会被修改。

4. 只有 `canSubmit=true` 才允许确认。提交时前端使用 `orderedEvents` 中的 eventId，而不是用户最初勾选顺序。

5. 提交成功后不打开新的批次抽屉，列表刷新状态。用户可从「查看进度」进入事件详情，在重处理历史中查看本次重放的运行态、完成态或失败原因。

# 2\. 接口清单

|方法|路径|用途|前端调用时机|
|---|---|---|---|
|GET|`/api/dql-events`|异常事件分页列表|页面加载、筛选、状态切换、手动刷新、处理中自动刷新|
|GET|`/api/dql-events/summary`|状态计数汇总|与列表相同，但不传 status、skip、limit、order|
|GET|`/api/dql-events/{eventId}`|事件详情|打开详情抽屉、刷新详情|
|POST|`/api/dql-events/recovery/preview`|校验、拦截并排序待重处理事件|打开确认重处理弹窗|
|POST|`/api/dql-events/recovery`|创建并提交重处理批次|用户确认重处理|
|GET|`/api/dql-events/recovery-batches/{batchId}`|服务端可选的批次诊断查询|当前 Web UI 不调用，由事件详情中的重放历史承载|

# 3\. 列表与汇总 API

## 3\.1 GET /api/dql\-events

**Query 参数**

|参数|类型|说明|
|---|---|---|
|`taskId`|string|任务 ID，当前任务下拉传此值。|
|`taskName`|string|预留任务名筛选。|
|`sourceTable` / `targetTable`|string|表名包含匹配。|
|`keyword`|string|任务名、错误码综合搜索。|
|`dmlType`|I \| U \| D|新增、更新、删除。|
|`errorType`|DqlErrorType|错误分类，枚举见第 6 节。|
|`status`|DqlEventStatus|状态标签对应的筛选条件。|
|`startTime` / `endTime`|string|失败时间范围。当前日期控件上传 Unix 毫秒字符串，服务端建议兼容 ISO 8601。|
|`skip` / `limit`|number|偏移分页，默认前端传 0 和 20。|
|`order`|string|默认 `-failedAt`，负号表示倒序。|

**响应**

```json
{
  "items": [/* DqlEvent[] */],
  "total": 128
}
```

## 3\.2 GET /api/dql\-events/summary

复用列表筛选参数，但不传 `status`、`skip`、`limit`、`order`。返回计数必须与列表所处数据集一致。

```json
{
  "total": 128,
  "pending": 54,
  "reprocessing": 3,
  "recovered": 60,
  "recoveryFailed": 7,
  "notReprocessable": 4
}
```

# 4\. 详情 API

## GET /api/dql\-events/\{eventId\}

路径参数是前端展示和操作使用的 `eventId`，不是内部 `id`。响应为 `DqlEventDetail`，即列表事件字段加上以下详情字段。

|字段|类型|用途|
|---|---|---|
|`sourceNodeName` / `targetNodeName`|string?|详情中的源、目标节点名称。|
|`failedNodeName` / `stage`|string?|失败节点和阶段，例如 SOURCE、TRANSFORM、TARGET。|
|`tableId`|string?|预留表标识。|
|`captureSeq`|number?|捕获序号，预览和重放顺序的稳定 tie\-breaker。|
|`eventKey` / `eventKeyMissing`|string? / boolean?|业务键及是否缺失。缺失时需标记不可安全重处理。|
|`payloadFormat` / `payloadHash` / `payloadSize`|string? / string? / number?|Payload 元数据。size 单位为字节。|
|`payloadComplete`|boolean?|为 false 时不可安全重处理。|
|`payloadPreview`|object?|服务端脱敏、限长后的可展示预览，不要求返回原始 Payload。|
|`payloadPreviewTruncated`|boolean?|为 true 时前端提示预览被截断，区别于 payload 不完整。|
|`errorDetails` / `rawErrorRef`|string?|可读错误详情及原始错误外部引用。|
|`recoveryAttempts`|DqlRecoveryAttempt\[\]?|重放历史，包含当前运行中的记录和每次失败原因，按最近优先或明确排序返回。|

# 5\. 重处理 API

## 5\.1 POST /api/dql\-events/recovery/preview

```json
{
  "eventIds": ["dlq_01J8K6CB1A2M04Q9X001", "dlq_01J8K69KKG0ACN5N4Q0B"]
}
```

服务端负责重新校验选中事件是否存在、是否属于同一任务、是否仍可重处理、Payload 是否完整，并返回唯一可信的处理顺序。建议排序规则为事件时间升序，再以捕获序号升序；若有更严格的 CDC 顺序语义，应由服务端覆盖并在该字段体现。blockedEvents 除内部 eventId 外，应同时返回 sourceTable、targetTable、dmlType、eventTime、captureSeq，供界面在不展示事件 ID 的情况下识别被拦截记录。

```json
{
  "taskId": "task-orders",
  "taskName": "订单同步",
  "canSubmit": true,
  "orderedEvents": [/* DqlEvent[]，服务端已排序 */],
  "blockedEvents": [
    { "eventId": "dlq_xxx", "message": "事件 payload 不完整，不能安全重处理" }
  ],
  "message": "存在不可提交事件时的总体提示"
}
```

## 5\.2 POST /api/dql\-events/recovery

```json
{
  "eventIds": ["dlq_01J8K6CB1A2M04Q9X001"],
  "confirm": true
}
```

提交接口仍须执行与预览相同的校验，以应对预览后状态变化。成功时返回完整 `DqlRecoveryBatch`。Payload 必须保持不变，重处理使用当前已发布任务配置。

## 5\.3 批次查询接口

当前 Web UI 不调用此接口或展示独立批次抽屉。若服务端保留该接口，可供运维诊断使用；页面进度以 GET /api/dql\-events/\{eventId\} 返回的 recoveryAttempts 为准。

```json
{
  "batchId": "batch_01J8",
  "taskId": "task-orders",
  "taskName": "订单同步",
  "status": "RUNNING",
  "selectedCount": 2,
  "successCount": 1,
  "failedCount": 0,
  "skippedCount": 0,
  "eventIds": ["dlq_a", "dlq_b"],
  "orderedEventIds": ["dlq_a", "dlq_b"],
  "startedAt": "2026-08-27T08:00:00.000Z",
  "finishedAt": null,
  "message": "可选的运行或失败说明"
}
```

# 6\. 公共数据结构与枚举

## 6\.1 DqlEvent

|字段|类型|必填|说明|
|---|---|---|---|
|`id`|string|是|内部记录 ID。|
|`eventId`|string|是|事件唯一 ID，列表主键、详情路径及重处理请求均使用它。|
|`taskId` / `taskName`|string|是|所属任务标识和展示名称。|
|`sourceTable` / `targetTable`|string|是|来源、目标表名。|
|`dmlType`|I \| U \| D|是|数据变更类型。|
|`errorType`|DqlErrorType|是|错误分类。|
|`errorCode`|string|是|可搜索的错误码或错误名。|
|`eventTime` / `failedAt`|ISO 8601 string|是|事件发生和失败时间。前端以本地格式展示。|
|`captureSeq`|number?|否|捕获顺序号，建议可重处理事件尽量提供。|
|`status`|DqlEventStatus|是|当前生命周期状态。|
|`recoveryCount`|number|是|历史提交重处理次数。|
|`lastRecoveryTime`|ISO 8601 string?|否|最近一次重处理时间。|

## 6\.2 枚举

|枚举|值|页面文案|
|---|---|---|
|DqlEventStatus|`PENDING`, `REPROCESSING`, `RECOVERED`, `RECOVERY_FAILED`, `NOT_REPROCESSABLE`|待处理、处理中、已恢复、恢复失败、不可重处理|
|DqlErrorType|`MALFORMED_RECORD`, `POISON_RECORD`, `TRANSFORM_ERROR`, `TARGET_WRITE_ERROR`, `UNKNOWN_RECORD_ERROR`|格式错误、不可处理记录、转换失败、目标写入失败、未知记录错误|
|DqlRecoveryBatchStatus|`CREATED`, `DISPATCHED`, `RUNNING`, `SUCCESS`, `PARTIAL_FAILED`, `FAILED`, `CANCELED`|批次生命周期状态|
|DqlRecoveryAttempt\.result|`SUCCESS`, `FAILED`, `SKIPPED`, `TIMEOUT`|单次历史处理结果|

## 6\.3 DqlRecoveryAttempt

attemptId、batchId、startedAt、finishedAt?、result、message?、errorMessage?。result 支持 RUNNING、SUCCESS、FAILED、SKIPPED、TIMEOUT。详情抽屉使用此结构展示当前重放、完成时间和重放失败错误。

# 7\. 服务端必须兜底的规则

- 预览和提交都必须限制为同一任务，且只允许 `PENDING`、`RECOVERY_FAILED` 两种可重处理状态。

- 当 `payloadComplete=false` 或事件主键缺失时，应拒绝安全重处理，并在 `blockedEvents` 中给出面向用户的原因。

- 预览结果里的 `orderedEvents` 和批次里的 `orderedEventIds` 是最终执行顺序。前端会按它们提交和展示。

- 提交应具备幂等或去重保护，避免重复点击、重复轮询或并发请求创建多个相同批次。

- 批次计数需满足 `successCount + failedCount + skippedCount <= selectedCount`；结束态需填写 `finishedAt`。

- 列表、汇总和批次读取接口需要处理权限隔离，不能通过 eventId 或 batchId 跨任务读取。

## 7.1 统一错误响应与 HTTP 语义

所有 DQL 接口失败时仍使用统一的 `ResponseMessage` 响应封装。前端应读取 `code` 和可直接展示的 `message`，业务数据保持为空；`reqId`、`ts` 等公共字段按现有封装返回。

```json
{
  "code": "DqlRecovery.EventNotReprocessable",
  "message": "Selected exception events cannot be reprocessed: ...",
  "data": null
}
```

HTTP 状态用于表达可恢复的交互语义，具体业务原因通过 `code` 和 `message` 传递：

|场景|错误码示例|HTTP 状态|前端处理建议|
|---|---|---:|---|
|请求参数或 Payload/路由校验失败|`IllegalArgument`、`DqlEvent.InvalidPayload`、`DqlEvent.InvalidRouteDecision`、`DqlRecovery.CrossTaskNotAllowed`|400|提示 `message`，修正参数或重新预览。|
|无异常事件菜单或任务数据权限|`NoPermission`|403|禁止当前操作并提示无权限。|
|事件、任务或重处理批次不存在|`DqlEvent.NotFound`、`Task.NotFound`、`DqlRecovery.BatchNotFound`|404|关闭详情或移除失效记录后刷新列表。|
|事件状态不可重处理、批次或事件锁冲突|`DqlRecovery.EventNotReprocessable`、`DqlRecovery.EventLockFailed`|409|提示状态已变化，重新获取详情或重新预览。|
|未分类的服务端异常|`SystemError`|500|提示稍后重试并使用 `reqId` 定位问题。|

列表、汇总在无任务数据权限时按第 7 节权限隔离规则返回空结果或零计数；只有菜单权限、任务归属或显式资源访问失败时才返回 `NoPermission`。前端不应依赖尚未冻结的内部错误码集合来判断流程，只需优先按 HTTP 状态处理，并展示服务端返回的 `message`。

# 8\. 联调建议

建议先完成列表和汇总，验证筛选、分页和状态计数一致性；再接详情，确认重放历史能返回运行态与失败错误；最后接预览和提交。详情处于 REPROCESSING 时，前端会每 3 秒刷新详情。服务端如需统一响应封装，可由请求层解包，但业务 data 必须保持本文结构；错误响应按第 7.1 节的 HTTP 状态和可展示 message 处理。
