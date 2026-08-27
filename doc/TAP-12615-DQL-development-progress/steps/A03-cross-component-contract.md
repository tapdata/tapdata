# A03 冻结 TM、Engine 和 Web 交接契约

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依据：详细设计第 6、7、9、12 节及 DQL 接口文档

## 字段规则

- Mongo 持久化字段固定使用 snake_case，Java DTO/VO 和 JSON 固定使用 camelCase。
- 时间在 API 和 Engine 消息中使用 Unix 毫秒时间戳，在 Mongo 中保存为 BSON Date。
- 完整 `payloadData` 只允许 Engine 上报、TM 持久化和 Engine 回放使用。
- 查询、详情和统计 API 不返回完整 `payloadData`；详情只返回 `payloadPreview`。
- `exceptionScope` 和 `routeDecision` 由 Engine 明确上报时必须分别为 `RECORD`、`RECORD_DLQ`；省略时 TM 可按兼容规则补默认值。

## API 契约

| 调用方 | 方法和路径 | 用途 |
| --- | --- | --- |
| Engine | `POST /api/task/{taskId}/dql-events/report` | 上报记录级异常事件 |
| Engine | `POST /api/task/{taskId}/dql-events/record-success/report` | 标记同记录后续成功造成的覆盖风险 |
| 当前 Web | `GET /api/dql-events` | 分页、过滤和排序查询 |
| 当前 Web | `GET /api/dql-events/{eventId}` | 查询安全详情和 `recoveryAttempts` 进度 |
| 当前 Web | `GET /api/dql-events/summary` | 查询状态统计 |
| 当前 Web | `POST /api/dql-events/recovery/preview` | 校验并获取服务端唯一可信的回放顺序 |
| 当前 Web | `POST /api/dql-events/recovery` | 按预览顺序创建并下发恢复批次 |
| 可选运维诊断 | `GET /api/dql-events/recovery-batches/{batchId}` | 查询批次详情；当前 Web 不依赖、不展示独立批次抽屉 |
| Engine | `POST /api/task/{taskId}/dql-events/recovery/report` | 上报批次和事件执行结果 |

当前 Web 只依赖上述 5 个事件接口。`eventId` 仅用于接口定位和提交，不作为用户可见字段；提交必须使用预览 `orderedEvents` 返回的 eventId 顺序。详情通过 `recoveryAttempts` 展示并刷新进度，不依赖批次详情接口。

## 消息契约

- TM 到 Engine 使用独立消息类型 `dqlRecovery`。
- 消息必须包含 `batchId`、`taskId`、任务版本、恢复模式和服务端固化的 `orderedEventIds`。
- 不扩展或复用 start、stop、reset、delete 等任务生命周期 opType。
- Engine 必须按消息中的顺序执行，不在 Engine 端重新按其他字段排序。

## 错误码契约

固定使用接口文档中的错误码：`IllegalArgument`、`NoPermission`、`DqlEvent.NotFound`、`DqlRecovery.CrossTaskNotAllowed`、`DqlRecovery.EventNotReprocessable`、`DqlRecovery.EventLockFailed`、`DqlRecovery.BatchNotFound`、`DqlEvent.InvalidRouteDecision`、`DqlRecovery.TaskNotRunnable`、`DqlRecovery.BatchAlreadyRunning`、`DqlRecovery.TaskVersionChanged`、`DqlRecovery.PayloadIncomplete`。

## 兼容规则

- 新增 API 不修改现有 skip-error-table API。
- Engine 回调同时兼容项目现有 `/api/task` 与 `/api/Task` 路径风格，新增调用统一使用小写路径。
- Web 开发不在本计划范围；B09、B11 和 D01/D04 完成后向 Web 负责人交付当前 5 个接口契约。D09 只作为可选运维诊断能力。

## 已识别的代码跟进

- B01 需要补齐异常范围和路由决策强类型枚举。
- D05 和 E01 需要用明确 DTO 代替松散 Map 消息，并增加契约测试。
