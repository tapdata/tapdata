# TAP-12615 DLQ API 与前端交互关系说明

## 1. 文档目的

本文解释后端 DLQ 异常事件 API 如何支撑当前 Web UI 的列表、详情和受控重处理流程。前端交互和数据契约以 `TAP-12615-DLQ-controlled-reprocessing-api.md` 的 2026-08-27 版本为准；本文只补充“用户动作、调用顺序、关键字段、刷新条件”的对应关系，不扩展新的前端依赖。

异常事件页面只消费 `RECORD_DLQ` 记录级事件。共享临时异常、任务级重试、任务级重试耗尽和未知异常保护仍通过任务状态、任务告警和日志表达，不会出现在 DLQ 列表中。

参考文档：

- `doc/TAP-12615-DLQ-controlled-reprocessing-api.md`
- `doc/TAP-12615-DLQ-controlled-reprocessing-frontend-interaction-design.md`

## 2. 前端页面与 API 总览

当前 Web UI 依赖 5 个接口：

| 前端模块 | 用户动作 | API | 说明 |
| --- | --- | --- | --- |
| 异常事件列表页 | 进入页面、分页、筛选、刷新 | `GET /api/dql-events` | 返回分页事件列表，默认 `skip=0`、`limit=20`、`order=-failedAt`。 |
| 状态统计区 | 初始化、修改筛选或切换状态 | `GET /api/dql-events/summary` | 使用与列表相同的非状态筛选条件，不传 `status`、`skip`、`limit`、`order`。 |
| 详情抽屉 | 点击“详情”或“查看进度” | `GET /api/dql-events/{eventId}` | 返回完整事件安全视图和 `recoveryAttempts`；`eventId` 只用于接口定位，不在页面展示。 |
| 重处理预览弹窗 | 单条或批量重处理前预览 | `POST /api/dql-events/recovery/preview` | 由服务端重新校验、返回阻塞项并确定最终顺序。 |
| 重处理确认 | 用户确认后提交 | `POST /api/dql-events/recovery` | 使用预览结果 `orderedEvents` 中的 `eventId` 提交，`confirm` 必须为 `true`。 |

`GET /api/dql-events/recovery-batches/{batchId}` 可以由服务端保留用于运维诊断，但当前 Web UI 不调用，也不展示独立批次进度抽屉。

### 2.1 分层路由约定

| Engine 路由结果 | 是否写入 DLQ | 前端可见位置 |
| --- | --- | --- |
| `RECORD_DLQ` | 是 | 异常事件列表、详情、统计和重处理流程 |
| `TASK_RETRY` | 否 | 任务监控、任务状态和现有任务告警 |
| `TASK_ERROR` | 否 | 任务监控、任务错误告警和任务日志 |
| 未知异常触发保护 | 否 | 任务监控、保护告警或任务日志 |

因此，`GET /api/dql-events` 返回为空只表示当前筛选条件下没有记录级 DLQ 事件，不能据此判断任务没有发生共享异常或任务级重试。

## 3. 页面初始化

前端进入“高级功能 / 异常事件”后并行调用：

1. `GET /api/dql-events/summary`
2. `GET /api/dql-events?skip=0&limit=20&order=-failedAt`

列表响应：

```json
{
  "items": [/* DqlEvent[] */],
  "total": 128
}
```

`eventId` 是表格行主键及后续详情、预览、提交请求的定位字段，但不作为可见列展示。列表不返回完整 Payload，前端只在详情接口中消费服务端生成的安全预览。

## 4. 筛选、统计与路由同步

### 4.1 筛选分组

| 前端区域 | 筛选项 | Query 参数 | 服务端行为 |
| --- | --- | --- | --- |
| 高频筛选 | 关键词 | `keyword` | 至少匹配 `taskName`、`errorCode`。 |
| 高频筛选 | 任务 | `taskId` | 精确匹配任务；`taskName` 作为预留任务名筛选参数。 |
| 高频筛选 | DML | `dmlType` | 精确匹配 `I`、`U`、`D`。 |
| 高频筛选 | 错误类型 | `errorType` | 精确匹配 `DqlErrorType`。 |
| 更多筛选 | 来源表 | `sourceTable` | 表名包含匹配。 |
| 更多筛选 | 目标表 | `targetTable` | 表名包含匹配。 |
| 更多筛选 | 失败时间 | `startTime`、`endTime` | 按 `failedAt` 闭区间过滤；当前控件传 Unix 毫秒字符串，服务端建议兼容 ISO 8601。 |
| 状态标签 | 事件状态 | `status` | 精确匹配 `DqlEventStatus`。 |

筛选和状态写入 URL query；页面加载时从 query 恢复。更多筛选只有在用户点击“应用筛选”后才更新 query 并刷新列表和统计。

### 4.2 状态统计

| 标签 | 汇总字段 | 列表筛选值 |
| --- | --- | --- |
| 全部 | `total` | 不传 `status` |
| 待处理 | `pending` | `PENDING` |
| 处理中 | `reprocessing` | `REPROCESSING` |
| 已恢复 | `recovered` | `RECOVERED` |
| 恢复失败 | `recoveryFailed` | `RECOVERY_FAILED` |
| 不可重处理 | `notReprocessable` | `NOT_REPROCESSABLE` |

汇总请求必须去掉当前 `status`，确保各状态数字始终表示同一组非状态筛选条件下的分布。

## 5. 列表展示与排序

列表是远程分页表格，默认按 `failedAt` 倒序。来源表、目标表、事件时间、最近重处理时间默认隐藏，可通过显示列设置开启。

`DqlEvent` 至少包含：

- 内部定位：`id`、`eventId`。
- 任务和表：`taskId`、`taskName`、`sourceTable`、`targetTable`。
- 事件和错误：`dmlType`、`errorType`、`errorCode`、`eventTime`、`failedAt`、`captureSeq`。
- 生命周期：`status`、`recoveryCount`、`lastRecoveryTime`。

当前契约只固定默认排序参数 `-failedAt`。如果后续增加其他服务端排序字段，应先更新 API 契约，再开放对应的前端远程排序入口。

## 6. 详情抽屉与进度展示

打开详情时调用：

```http
GET /api/dql-events/{eventId}
```

详情接口支撑以下展示区：

| 前端区域 | 关键响应字段 |
| --- | --- |
| 任务与表流向 | `taskName`、`sourceTable`、`targetTable`、`sourceNodeId`、`sourceNodeName`、`targetNodeId`、`targetNodeName` |
| 失败位置 | `failedNodeId`、`failedNodeName`、`stage`、`tableId`、`dmlType`、`eventTime`、`failedAt`、`captureSeq` |
| Payload 元数据 | `eventKey`、`eventKeyMissing`、`payloadFormat`、`payloadHash`、`payloadSize`、`payloadComplete` |
| 错误信息 | `errorType`、`errorCode`、`errorDetails`、`rawErrorRef` |
| Payload 安全预览 | `payloadPreview`、`payloadPreviewTruncated`、`payloadComplete` |
| 重处理历史与进度 | `recoveryAttempts` |

安全要求：

- 页面不展示事件 ID，也不提供事件 ID 复制入口。
- 详情不要求返回完整 `payloadData`；前端只展示服务端脱敏、限长后的 `payloadPreview`。
- `errorDetails` 按纯文本展示，不能作为 HTML 渲染。
- `payloadComplete=false` 表示原始 Payload 不完整；`payloadPreviewTruncated=true` 只表示展示预览被截断，两者必须使用不同提示。

当事件状态为 `REPROCESSING` 时，详情抽屉每 3 秒重新请求当前事件，使用 `recoveryAttempts` 中 `result=RUNNING` 的记录展示当前进度，并在 attempt 进入终态或事件离开 `REPROCESSING` 后停止轮询。

## 7. 单条与批量重处理

单条和批量重处理使用相同调用顺序：

1. `POST /api/dql-events/recovery/preview`
2. 展示服务端排序、阻塞原因和固定影响说明
3. 用户确认
4. `POST /api/dql-events/recovery`

列表仅允许勾选 `PENDING`、`RECOVERY_FAILED`，且一次选择必须属于同一 `taskId`。这些前端限制只是体验优化，预览和提交接口都必须重新执行同样的校验。

### 7.1 预览

```json
{
  "eventIds": ["dlq_01J8K6CB1A2M04Q9X001"]
}
```

预览响应处理：

- 只有 `canSubmit=true` 才允许确认。
- `orderedEvents` 是服务端确定的最终顺序，界面按数组顺序展示。
- `blockedEvents` 除内部 `eventId` 外，还应包含 `sourceTable`、`targetTable`、`dmlType`、`eventTime`、`captureSeq` 和面向用户的 `message`；界面使用这些业务字段识别记录，不展示 `eventId`。
- 影响说明固定为：使用当前已发布任务配置重处理原始事件；同步可能短暂暂停，完成后恢复；Payload 不会被修改。

### 7.2 提交

提交请求的 `eventIds` 必须从本次预览响应的 `orderedEvents` 依次提取，不能继续使用用户最初的勾选顺序。

```json
{
  "eventIds": ["dlq_01J8K6CB1A2M04Q9X001"],
  "confirm": true
}
```

提交接口仍须再次校验事件状态、任务归属和 Payload 完整性，并返回完整 `DqlRecoveryBatch`。前端提交成功后关闭预览、清空选择并刷新列表和统计，不自动打开新的批次抽屉。

## 8. 刷新与进度入口

| 场景 | 前端行为 | 停止条件 |
| --- | --- | --- |
| 当前列表存在 `REPROCESSING` | 每 8 秒静默刷新列表和汇总 | 当前页不再存在 `REPROCESSING` 或页面卸载 |
| 打开 `REPROCESSING` 事件详情 | 每 3 秒刷新事件详情 | attempt 结束、事件离开 `REPROCESSING`、抽屉关闭或页面卸载 |
| 提交重处理成功 | 立即刷新列表和汇总 | 单次动作 |

列表行“查看进度”直接打开该事件的详情抽屉。重处理运行态、完成态、失败原因都由 `recoveryAttempts` 承载。

## 9. 权限与错误处理

页面路由仍由异常事件菜单权限控制，服务端按用户可见任务范围过滤列表、汇总、详情和重处理操作；不能通过 `eventId` 或可选的 `batchId` 越权读取。

前端按 HTTP 语义和服务端可展示 `message` 处理错误：

| HTTP 语义 | 典型场景 | 前端处理 |
| --- | --- | --- |
| `400` | 参数缺失、格式错误、`confirm` 非 true | 保留当前页面或弹窗，展示 message。 |
| `404` | 事件不存在或已无权限访问 | 关闭详情，刷新列表；不泄露资源是否属于其他任务。 |
| `409` | 状态变化、跨任务、重复批次、Payload 不完整等提交冲突 | 禁止或停止提交，展示 message，清空失效选择并刷新列表。 |
| 无权限 | 菜单或任务数据权限不足 | 展示无权限状态并停止相关自动刷新。 |

不要依赖前端禁用态兜底状态冲突，也不要根据提交成功自行推断重处理成功。

## 10. Engine 内部接口关系

Web 前端不直接调用 Engine 内部接口，但它们决定页面数据来源和状态变化：

| Engine 行为 | 内部 API 或处理 | 对前端可见影响 |
| --- | --- | --- |
| 捕获记录级确定性异常 | `POST /api/task/{taskId}/dql-events/report` | 列表新增 `PENDING` 或 `NOT_REPROCESSABLE` 事件。 |
| 捕获共享临时异常 | 不写 DLQ | 异常事件列表不新增记录；用户从任务监控或告警查看任务级重试。 |
| 单事件开始回放 | 结果回调写入运行中 attempt | 详情 `recoveryAttempts` 出现 `RUNNING`。 |
| 单事件完成或失败 | 结果回调更新事件和 attempt | 列表状态及详情历史变为 `RECOVERED` 或 `RECOVERY_FAILED`。 |
| 批次完成或失败 | 批次状态在服务端收敛 | 当前 UI 通过各事件详情历史观察，不新增批次页面。 |

## 11. 当前契约关键点

1. 当前 Web UI 依赖列表、汇总、详情、预览、提交 5 个接口。
2. 事件 ID 只用于接口定位和前端内部行键，不在界面展示。
3. `DqlErrorType` 的目标写入错误值为 `TARGET_WRITE_ERROR`。
4. `DqlRecoveryAttempt.result` 支持 `RUNNING`、`SUCCESS`、`FAILED`、`SKIPPED`、`TIMEOUT`。
5. 预览是服务端权威校验，提交使用预览后的顺序并再次校验。
6. 进度由事件详情的 `recoveryAttempts` 承载，不使用独立批次进度抽屉。
7. 当前批次查询接口仅为服务端可选诊断能力，不是 Web 联调阻塞项。
