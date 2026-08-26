# TAP-12615 DLQ API 与前端交互关系说明

## 1. 文档目的

本文解释后端 DLQ 异常事件 API 如何支撑前端交互设计文档中的页面、抽屉、弹窗和轮询流程。接口定义以 `TAP-12615-DLQ-controlled-reprocessing-api.md` 为准，本文补充“用户动作、调用顺序、关键字段、注意事项”的对应关系。

更新日期：2026-08-26。本文已同步 TAP-12615 V1.2 的分层路由口径：异常事件页面只消费 `RECORD_DLQ` 记录级事件；共享临时异常、任务级重试、任务级重试耗尽和未知异常保护通过任务状态、任务告警和日志表达，不会出现在 DLQ 列表中。

参考文档：

- `doc/TAP-12615-DLQ-controlled-reprocessing-frontend-interaction-design.md`
- `doc/TAP-12615-DLQ-controlled-reprocessing-api.md`

## 2. 前端页面与 API 总览

| 前端模块 | 用户动作 | API | 说明 |
| --- | --- | --- | --- |
| 异常事件列表页 | 进入页面、分页、筛选、排序、刷新 | `GET /api/dql-events` | 返回分页事件列表，服务端按任务可见范围过滤。 |
| 状态统计区 | 初始化统计、切换筛选后刷新统计 | `GET /api/dql-events/summary` | 返回各状态数量，供 SummaryTabs 展示。 |
| 详情抽屉 | 点击事件 ID 或详情按钮 | `GET /api/dql-events/{eventId}` | 返回事件详情、payload 预览、重处理历史和当前批次摘要。 |
| 重处理预览弹窗 | 单条或批量重处理前预览 | `POST /api/dql-events/recovery/preview` | 校验可重处理性，并返回服务端排序后的事件顺序和阻塞原因。 |
| 重处理确认 | 用户确认后提交 | `POST /api/dql-events/recovery` | `confirm` 必须为 `true`，创建批次并锁定事件。 |
| 批次进度抽屉 | 查看进度、轮询批次 | `GET /api/dql-events/recovery-batches/{batchId}` | 返回批次状态和处理计数，前端按状态决定是否继续轮询。 |
| Engine 回调 | Engine 上报异常、后续成功写入或重处理结果 | `POST /api/task/{taskId}/dql-events/report`、`POST /api/task/{taskId}/dql-events/record-success/report`、`POST /api/task/{taskId}/dql-events/recovery/report` | 内部接口，不由 Web 前端直接调用；后续成功写入会影响列表、详情和预览的覆盖风险提示。 |

### 2.1 分层路由约定

| Engine 路由结果 | 是否调用 DLQ 上报 API | 前端可见位置 |
| --- | --- | --- |
| `RECORD_DLQ` | 是，调用 `POST /api/task/{taskId}/dql-events/report` | 异常事件列表、详情、统计、重处理 |
| `TASK_RETRY` | 否 | 任务监控、任务状态、现有任务告警 |
| `TASK_ERROR` | 否 | 任务监控、任务错误告警、任务日志 |
| 未知异常触发保护 | 否 | 任务监控、任务告警或保护告警、任务日志 |

因此，`GET /api/dql-events` 返回为空只能说明当前筛选条件下没有记录级 DLQ 事件，不能说明任务没有发生共享异常或任务级重试。

## 3. 页面初始化

前端进入 `/exception-events` 页面后建议并行调用：

1. `GET /api/dql-events/summary`
2. `GET /api/dql-events?skip=0&limit=20&order=-failedAt`

列表接口返回 `Page<DqlEventDto>`：

- `total` 用于分页总数。
- `items` 用于表格行数据。
- 列表响应已清空 `payloadData`，不会把完整 payload 返回给浏览器。
- 列表响应已清空 `recoveryAttempts`，详情抽屉再按需加载历史。

## 4. 筛选与状态统计

列表和统计接口使用同一组查询参数：

| 前端筛选项 | Query 参数 | 服务端行为 |
| --- | --- | --- |
| 任务 | `taskId`、`taskName` | `taskId` 精确匹配，`taskName` 模糊匹配。 |
| 源表 | `sourceTable` | 模糊匹配。 |
| 目标表 | `targetTable` | 模糊匹配。 |
| 关键字 | `keyword` | 匹配事件 ID、任务名、源表、目标表、错误码、错误详情、路由依据。 |
| DML | `dmlType` | 精确匹配 `I`、`U`、`D`。 |
| 错误类型 | `errorType` | 精确匹配错误类型枚举。 |
| 状态 | `status` | 精确匹配事件状态枚举。 |
| 失败时间 | `startTime`、`endTime` | 按 `failedAt` 毫秒时间戳范围过滤。 |

状态 Tab 展示推荐使用 `summary` 返回字段：

| Tab | 字段 | 列表筛选值 |
| --- | --- | --- |
| 全部 | `total` | 不传 `status` |
| 待处理 | `pending` | `PENDING` |
| 重处理中 | `reprocessing` | `REPROCESSING` |
| 已恢复 | `recovered` | `RECOVERED` |
| 重处理失败 | `recoveryFailed` | `RECOVERY_FAILED` |
| 不可重处理 | `notReprocessable` | `NOT_REPROCESSABLE` |

如果希望统计展示“当前非状态筛选条件下的全状态分布”，前端请求 summary 时应去掉当前 `status` 参数。

## 5. 排序关系

前端 `TablePage` 的自定义排序可以直接映射到 `order` 参数：

| 前端排序字段 | 降序参数 | 升序参数 |
| --- | --- | --- |
| 失败时间 | `-failedAt` | `failedAt` |
| 事件时间 | `-eventTime` | `eventTime` |
| 重处理次数 | `-recoveryCount` | `recoveryCount` |
| 最近重处理时间 | `-lastRecoveryTime` | `lastRecoveryTime` |

服务端会把 camelCase 字段转换为 Mongo 字段，例如 `recoveryCount -> recovery_count`、`lastRecoveryTime -> last_recovery_time`。也兼容 `failed_at desc` 这类 snake_case 写法。

## 6. 详情抽屉

打开详情时调用：

```http
GET /api/dql-events/{eventId}
```

详情接口支撑前端 5 个展示区：

| 前端区域 | 关键响应字段 |
| --- | --- |
| 基本信息 | `eventId`、`taskId`、`taskName`、`sourceNodeName`、`failedNodeName`、`sourceTable`、`targetTable`、`dmlType`、`eventTime`、`failedAt`、`captureSeq` |
| 事件标识 | `eventIdentity`、`eventKey`、`eventKeyMissing`、`payloadHash`、`payloadSize`、`payloadComplete` |
| 错误信息 | `errorType`、`errorCode`、`exceptionScope`、`routeDecision`、`classificationReason`、`classificationConfidence`、`errorDetails`、`rawErrorRef` |
| Payload 预览 | `payloadPreview`、`payloadPreviewTruncated`、`payloadComplete` |
| 重处理历史 | `recoveryAttempts` |

安全说明：

- 详情接口不会返回完整 `payloadData`。
- 前端只展示 `payloadPreview`，不提供编辑、下载、复制完整 payload 的入口。
- `errorDetails` 按纯文本展示，不能作为 HTML 渲染。

当前批次说明：

- 当事件存在 `currentBatchId` 时，详情接口会返回 `currentBatch`。
- 前端在 `status=REPROCESSING` 且 `currentBatch` 存在时展示“查看进度”按钮。
- 点击“查看进度”后使用 `currentBatch.batchId` 打开批次进度抽屉。

路由展示说明：

- 列表、详情、统计和重处理预览只消费 `RECORD_DLQ` 主记录；共享临时异常和任务级异常不进入这些页面 API。
- Engine 上报异常事件时如果省略 `exceptionScope` 或 `routeDecision`，TM 会分别保存为 `RECORD` 和 `RECORD_DLQ`；如果显式上报其他值，TM 拒绝入库。
- 前端展示的 `classificationReason`、`classificationConfidence` 用于解释该事件为何进入 DLQ，不代表页面需要展示任务级重试或系统异常记录。

覆盖风险提示：

- 列表、详情和重处理预览均可能返回 `overwriteRisk`、`overwriteRiskMessage`、`laterSuccessAt`、`laterSuccessEventTime`、`laterSuccessCaptureSeq`、`laterSuccessDmlType`。
- 当 `overwriteRisk=true` 时，前端应展示服务端返回的提示文案：`该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作`。
- 该提示来源于 Engine 调用 `POST /api/task/{taskId}/dql-events/record-success/report`。Web 前端不直接调用该接口，只消费风险字段。
- 覆盖风险提示不改变事件状态，也不自动禁用重处理按钮；前端需要在用户确认重处理前明确展示，尤其是单条重处理详情和批量预览弹窗。

## 7. 单条重处理

单条重处理入口来自列表行或详情抽屉。调用顺序必须是：

1. `POST /api/dql-events/recovery/preview`
2. 用户确认
3. `POST /api/dql-events/recovery`

预览请求：

```json
{
  "eventIds": ["DQL-64f000-000001"]
}
```

预览响应：

- `canSubmit=true`：确认按钮可用。
- `orderedEvents`：展示服务端确认的执行顺序。
- `blockedEvents`：展示不可提交事件和原因；存在阻塞项时确认按钮禁用。

确认请求：

```json
{
  "eventIds": ["DQL-64f000-000001"],
  "confirm": true
}
```

`confirm` 缺失或为 `false` 时，服务端返回 `IllegalArgument`，参数为 `confirm`。

## 8. 批量重处理

批量重处理与单条重处理使用相同 API，只是 `eventIds` 包含多条事件。

后端规则：

- 所有事件必须属于同一任务，否则返回 `DqlRecovery.CrossTaskNotAllowed`。
- 事件状态必须是 `PENDING` 或 `RECOVERY_FAILED`。
- `payloadComplete=false` 的事件不可提交。
- 发起时服务端会再次锁定事件；锁定失败返回 `DqlRecovery.EventLockFailed`。
- 任务必须处于运行中或 Engine 可回放暂停态；任务停止、未完成初始化、任务版本变化或同任务已有批次运行中时不可提交。

前端本地同任务和状态校验只是体验优化，最终以预览接口和发起接口返回为准。

## 9. 批次进度抽屉

发起重处理成功后，响应返回 `DqlRecoveryBatchDto`：

- `batchId`
- `taskId`
- `taskName`
- `status`
- `selectedCount`
- `successCount`
- `failedCount`
- `skippedCount`
- `recoveryMode`
- `taskStatusBefore`
- `taskStatusAfter`
- `sourceReadPaused`
- `sourceReadResumeResult`
- `orderedEventIds`
- `startedAt`
- `finishedAt`
- `message`

前端打开批次抽屉后调用：

```http
GET /api/dql-events/recovery-batches/{batchId}
```

轮询规则：

| 批次状态 | 前端行为 |
| --- | --- |
| `CREATED`、`DISPATCHED`、`RUNNING` | 每 3 秒继续请求批次详情。 |
| `SUCCESS`、`PARTIAL_FAILED`、`FAILED`、`CANCELED` | 停止轮询，刷新列表和统计。 |

进度百分比按 `(successCount + failedCount + skippedCount) / selectedCount` 计算；当 `selectedCount=0` 时展示 0%。

## 10. 自动刷新

列表中存在 `REPROCESSING` 事件时，前端可以每 8 秒静默刷新：

1. `GET /api/dql-events`
2. `GET /api/dql-events/summary`

批次抽屉打开时，批次轮询独立执行，不依赖列表自动刷新。批次到达终态后，前端应刷新列表和统计，让事件状态从 `REPROCESSING` 更新为终态。

## 11. 权限与错误处理

页面接口需要菜单权限 `v2_exception_events`，并由服务端按任务可见范围过滤。前端直接访问路由或轮询时遇到 `NoPermission`，应展示无权限状态并停止对应请求。

常见错误码映射：

| code | 推荐前端处理 |
| --- | --- |
| `IllegalArgument` | 展示接口 message；如果参数是 `confirm`，提示需要重新确认后提交。 |
| `NoPermission` | 展示无权限状态，停止自动刷新或轮询。 |
| `DqlEvent.NotFound` | 关闭详情抽屉并刷新列表。 |
| `DqlRecovery.CrossTaskNotAllowed` | 提示只能处理同一任务，清空选择。 |
| `DqlRecovery.EventNotReprocessable` | 展示预览阻塞原因或接口 message，刷新列表。 |
| `DqlRecovery.EventLockFailed` | 提示事件状态已变化，清空选择并刷新列表。 |
| `DqlRecovery.BatchNotFound` | 关闭批次抽屉，提示批次不存在或已无权限查看。 |
| `DqlEvent.InvalidRouteDecision` | 一般只发生在 Engine 内部上报；前端遇到时展示服务端 message 并刷新。 |
| `DqlRecovery.TaskNotRunnable` | 在预览弹窗展示阻塞原因，引导到任务监控查看状态。 |
| `DqlRecovery.BatchAlreadyRunning` | 提示同一任务已有重处理批次运行中，刷新列表并引导查看进度。 |
| `DqlRecovery.TaskVersionChanged` | 提示任务版本已变化，需要重新预览。 |
| `DqlRecovery.PayloadIncomplete` | 展示 Payload 不完整，不允许提交。 |

## 12. Engine 内部接口关系

Web 前端不直接调用 Engine 回调接口，但这些接口决定页面数据来源和状态变化：

| Engine 行为 | API | 对前端可见影响 |
| --- | --- | --- |
| 捕获记录级确定性异常 | `POST /api/task/{taskId}/dql-events/report` | 列表新增 `PENDING` 或 `NOT_REPROCESSABLE` 事件。 |
| 同记录后续成功写入 | `POST /api/task/{taskId}/dql-events/record-success/report` | 已存在的前序 `PENDING`、`REPROCESSING` 或 `RECOVERY_FAILED` 事件标记 `overwriteRisk=true`，列表、详情和预览展示覆盖风险提示。 |
| 捕获共享临时异常 | 不调用 DLQ 上报 API | 异常事件列表不新增记录；用户从任务监控或告警查看任务级重试。 |
| 任务级重试耗尽 | 不调用 DLQ 上报 API | 异常事件列表不新增积压记录；用户从任务错误状态和告警查看。 |
| 未知异常保护触发 | 不调用 DLQ 上报 API | 异常事件列表不继续新增未知事件；用户从保护告警或任务日志查看。 |
| 批次开始 | `POST /api/task/{taskId}/dql-events/recovery/report`，`type=BATCH_STARTED` | 批次状态变为 `RUNNING`。 |
| 单事件结果 | `type=EVENT_RESULT` | 事件变为 `RECOVERED` 或 `RECOVERY_FAILED`，批次计数递增。 |
| 批次完成 | `type=BATCH_FINISHED` | 批次进入 `SUCCESS` 或 `PARTIAL_FAILED`。 |
| 批次失败 | `type=BATCH_FAILED` | 批次进入 `FAILED`，未完成事件释放为 `RECOVERY_FAILED`。 |

## 13. 本次 API 补充点

结合前端交互设计，后端已补充以下保障：

1. 详情接口返回 `currentBatch`，支撑 `REPROCESSING` 事件从详情进入批次进度抽屉。
2. 列表和详情接口清空完整 `payloadData`，避免完整 payload 暴露到浏览器。
3. 列表接口支持前端 camelCase 排序字段，包含 `recoveryCount` 和 `lastRecoveryTime`。
4. 发起重处理接口强制要求 `confirm=true`，保证预览后确认的交互语义可以由服务端兜底。
5. 上报、列表和详情补充 `exceptionScope`、`routeDecision`、`classificationReason`、`classificationConfidence`，支撑前端展示 DLQ 入库原因。
6. 批次 DTO 补充 `recoveryMode`、`taskStatusBefore`、`taskStatusAfter`、`sourceReadPaused`、`sourceReadResumeResult`，支撑任务状态保护说明。
7. 错误码补充任务不可回放、已有批次运行、任务版本变化、Payload 不完整和非法路由场景。
8. 异常事件补充 `recordIdentity`、`recordIdentityType`、`recordIdentityFields` 和 `overwriteRisk` 系列字段，支撑“同记录后续成功写入后的重放覆盖风险”提示。
9. Engine 内部补充 `POST /api/task/{taskId}/dql-events/record-success/report`，用于在普通成功写入后标记前序 DQL 事件，不要求前端新增调用路径。

以上补充不要求前端新增调用路径，但需要同步类型定义、列表/详情展示字段、空态说明和错误码映射。
