# TAP-12615 DLQ 异常事件前端交互设计

## 1. 文档说明

本文面向 TapData Web 前端开发人员，描述“异常事件（DLQ）与受控重处理”的页面结构、接口调用、状态展示、刷新策略、错误处理和组件边界。

更新日期：2026-08-27。前端交互与 API 数据结构以 `TAP-12615-DLQ-controlled-reprocessing-api.md` 的当前版本为准。此前设计中的独立批次进度抽屉、批次轮询和页面展示事件 ID 已取消，重处理进度统一由事件详情中的 `recoveryAttempts` 承载。

输入文档：

- `doc/TAP-12615-DLQ-controlled-reprocessing-design.md`
- `doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md`
- `doc/TAP-12615-DLQ-controlled-reprocessing-api.md`

代码现状参考：

- `tapdata-web/apps/daas/src/router/menu.ts`
- `tapdata-web/apps/daas/src/router/routes.ts`
- `tapdata-web/packages/business/src/components/TablePage.vue`
- `tapdata-web/packages/component/src/filter-bar/Main.vue`

本文只描述前端交互与实现落点，不重新定义 Engine 内部上报、回放或批次状态机。

## 2. 目标和边界

### 2.1 前端目标

- 在“高级功能 / 异常事件”提供跨任务统一入口。
- 页面只展示已经进入 `dql_events` 的记录级 DLQ 事件，不代表所有任务异常。
- 支持状态汇总、远程分页、显示列设置和 URL query 恢复。
- 高频筛选为关键词、任务、DML、错误类型；更多筛选为来源表、目标表、失败时间范围。
- 支持查看任务与表流向、失败位置、错误详情、Payload 安全预览和重处理历史。
- 支持单条和同一任务内的批量受控重处理。
- 提交前必须调用服务端预览，以服务端校验结果和排序为准。
- 提交后刷新列表；运行进度从事件详情进入，并通过 `recoveryAttempts` 查看。
- 不展示事件 ID，不展示或编辑完整 Payload。

### 2.2 不做内容

| 不做内容 | 前端落地影响 |
| --- | --- |
| 不编辑、下载或复制完整 Payload | 只展示服务端返回的 `payloadPreview`。 |
| 不在异常事件页控制普通任务启停 | 任务控制仍由任务列表和任务监控页面承载。 |
| 不把共享临时异常虚拟成 DLQ 行 | 网络、数据库等共享异常继续从任务状态、告警和日志查看。 |
| 不用前端校验替代服务端校验 | preview 和 recovery 都必须执行服务端权威校验。 |
| 不根据提交成功推断重处理成功 | 结果以列表状态和详情 `recoveryAttempts` 为准。 |
| 不建设独立批次进度抽屉 | 当前 Web UI 不调用批次查询接口；批次接口仅为服务端可选诊断能力。 |

## 3. 命名、状态和错误类型

页面展示统一使用“异常事件”和“重处理”；API 与集合沿用既有 `dql-events` / `dql_events` 命名。

### 3.1 事件状态

| 值 | 页面文案 | 可勾选 | 行操作 |
| --- | --- | --- | --- |
| `PENDING` | 待处理 | 是 | 重处理、详情 |
| `REPROCESSING` | 处理中 | 否 | 查看进度 |
| `RECOVERED` | 已恢复 | 否 | 详情 |
| `RECOVERY_FAILED` | 恢复失败 | 是 | 重处理、详情 |
| `NOT_REPROCESSABLE` | 不可重处理 | 否 | 详情 |

### 3.2 错误类型

| 值 | 页面文案 |
| --- | --- |
| `MALFORMED_RECORD` | 格式错误 |
| `POISON_RECORD` | 不可处理记录 |
| `TRANSFORM_ERROR` | 转换失败 |
| `TARGET_WRITE_ERROR` | 目标写入失败 |
| `UNKNOWN_RECORD_ERROR` | 未知记录错误 |

### 3.3 重处理历史结果

| 值 | 页面文案 |
| --- | --- |
| `RUNNING` | 处理中 |
| `SUCCESS` | 成功 |
| `FAILED` | 失败 |
| `SKIPPED` | 已跳过 |
| `TIMEOUT` | 超时 |

## 4. 页面结构、路由和菜单

推荐页面结构：

```text
高级功能 / 异常事件
  PageContainer
    SummaryTabs                状态汇总与状态筛选
    TablePage
      PrimaryFilters          关键词、任务、DML、错误类型
      MoreFilters             来源表、目标表、失败时间
      Operation               刷新、显示列设置
      Table                   异常事件列表
      MultipleSelectionActions 批量重处理
    EventDetailDrawer          事件详情、重处理历史与进度
    RecoveryPreviewDialog      单条/批量重处理确认
```

建议文件：

```text
tapdata-web/packages/api/src/core/dql-event.ts
tapdata-web/packages/business/src/views/exception-events/List.vue
tapdata-web/packages/business/src/views/exception-events/components/SummaryTabs.vue
tapdata-web/packages/business/src/views/exception-events/components/EventStatusTag.vue
tapdata-web/packages/business/src/views/exception-events/components/EventDetailDrawer.vue
tapdata-web/packages/business/src/views/exception-events/components/PayloadPreview.vue
tapdata-web/packages/business/src/views/exception-events/components/RecoveryPreviewDialog.vue
```

路由可以继续使用 `/exception-events` 和 `v2_exception_events` 权限码，但菜单位置必须落在“高级功能”分组下，页面标题为“异常事件”。前端不追加任务 Edit、Start 等操作权限判断；数据范围和操作权限由服务端按当前产品口径兜底。

## 5. API 封装和类型

### 5.1 API 封装

```typescript
import { requestClient } from '../request'

const BASE_URL = '/api/dql-events'

export function fetchDqlEvents(params: DqlEventQueryParams) {
  return requestClient.get<DqlEventPage>(BASE_URL, { params })
}

export function fetchDqlEventSummary(params: DqlEventSummaryQueryParams) {
  return requestClient.get<DqlEventSummary>(`${BASE_URL}/summary`, { params })
}

export function fetchDqlEventDetail(eventId: string) {
  return requestClient.get<DqlEventDetail>(`${BASE_URL}/${eventId}`)
}

export function previewDqlRecovery(eventIds: string[]) {
  return requestClient.post<DqlRecoveryPreview>(`${BASE_URL}/recovery/preview`, { eventIds })
}

export function startDqlRecovery(eventIds: string[]) {
  return requestClient.post<DqlRecoveryBatch>(`${BASE_URL}/recovery`, {
    eventIds,
    confirm: true,
  })
}
```

当前前端封装不包含 `fetchDqlRecoveryBatch`。服务端如保留 `GET /api/dql-events/recovery-batches/{batchId}`，仅供运维诊断，不属于当前 Web UI 的接口依赖。

### 5.2 查询和公共枚举

```typescript
export type DqlEventStatus =
  | 'PENDING'
  | 'REPROCESSING'
  | 'RECOVERED'
  | 'RECOVERY_FAILED'
  | 'NOT_REPROCESSABLE'

export type DqlErrorType =
  | 'MALFORMED_RECORD'
  | 'POISON_RECORD'
  | 'TRANSFORM_ERROR'
  | 'TARGET_WRITE_ERROR'
  | 'UNKNOWN_RECORD_ERROR'

export type DqlRecoveryAttemptResult =
  | 'RUNNING'
  | 'SUCCESS'
  | 'FAILED'
  | 'SKIPPED'
  | 'TIMEOUT'

export interface DqlEventQueryParams {
  taskId?: string
  taskName?: string
  sourceTable?: string
  targetTable?: string
  keyword?: string
  dmlType?: 'I' | 'U' | 'D'
  errorType?: DqlErrorType
  status?: DqlEventStatus
  startTime?: string
  endTime?: string
  skip?: number
  limit?: number
  order?: string
}

export type DqlEventSummaryQueryParams = Omit<
  DqlEventQueryParams,
  'status' | 'skip' | 'limit' | 'order'
>
```

### 5.3 列表、详情和汇总

```typescript
export interface DqlEvent {
  id: string
  eventId: string
  taskId: string
  taskName: string
  sourceTable: string
  targetTable: string
  dmlType: 'I' | 'U' | 'D'
  errorType: DqlErrorType
  errorCode: string
  eventTime: string
  failedAt: string
  captureSeq?: number
  status: DqlEventStatus
  recoveryCount: number
  lastRecoveryTime?: string | null
}

export interface DqlEventPage {
  items: DqlEvent[]
  total: number
}

export interface DqlEventSummary {
  total: number
  pending: number
  reprocessing: number
  recovered: number
  recoveryFailed: number
  notReprocessable: number
}

export interface DqlRecoveryAttempt {
  attemptId: string
  batchId: string
  startedAt: string
  finishedAt?: string | null
  result: DqlRecoveryAttemptResult
  message?: string
  errorMessage?: string
}

export interface DqlEventDetail extends DqlEvent {
  sourceNodeId?: string
  sourceNodeName?: string
  targetNodeId?: string
  targetNodeName?: string
  failedNodeId?: string
  failedNodeName?: string
  stage?: string
  tableId?: string
  eventKey?: string
  eventKeyMissing?: boolean
  payloadFormat?: string
  payloadHash?: string
  payloadSize?: number
  payloadComplete?: boolean
  payloadPreview?: Record<string, unknown>
  payloadPreviewTruncated?: boolean
  errorDetails?: string
  rawErrorRef?: string
  recoveryAttempts?: DqlRecoveryAttempt[]
}
```

### 5.4 重处理预览和提交响应

```typescript
export interface DqlRecoveryBlockedEvent {
  eventId: string
  sourceTable?: string
  targetTable?: string
  dmlType?: 'I' | 'U' | 'D'
  eventTime?: string
  captureSeq?: number
  message: string
}

export interface DqlRecoveryPreview {
  taskId: string
  taskName: string
  canSubmit: boolean
  orderedEvents: DqlEvent[]
  blockedEvents: DqlRecoveryBlockedEvent[]
  message?: string
}

export interface DqlRecoveryBatch {
  batchId: string
  taskId: string
  taskName: string
  status: 'CREATED' | 'DISPATCHED' | 'RUNNING' | 'SUCCESS' | 'PARTIAL_FAILED' | 'FAILED' | 'CANCELED'
  selectedCount: number
  successCount: number
  failedCount: number
  skippedCount: number
  eventIds: string[]
  orderedEventIds: string[]
  startedAt: string
  finishedAt?: string | null
  message?: string
}
```

`DqlRecoveryBatch` 是提交成功响应，不代表前端需要展示批次页面。页面只用成功响应确认请求已受理，然后刷新列表。

## 6. 页面初始化、筛选和统计

### 6.1 初始化

页面加载后并行请求：

- `GET /api/dql-events/summary`
- `GET /api/dql-events?skip=0&limit=20&order=-failedAt`

列表默认使用远程分页，`skip=(currentPage - 1) * pageSize`，默认 `pageSize=20`。默认排序固定为 `order=-failedAt`。

### 6.2 状态汇总

| 标签 | 汇总字段 | 列表请求 |
| --- | --- | --- |
| 全部 | `total` | 移除 `status` |
| 待处理 | `pending` | `status=PENDING` |
| 处理中 | `reprocessing` | `status=REPROCESSING` |
| 已恢复 | `recovered` | `status=RECOVERED` |
| 恢复失败 | `recoveryFailed` | `status=RECOVERY_FAILED` |
| 不可重处理 | `notReprocessable` | `status=NOT_REPROCESSABLE` |

每次调用 summary 都必须保留当前非状态筛选，移除 `status`、`skip`、`limit`、`order`。

### 6.3 高频筛选

| 控件 | Query | 行为 |
| --- | --- | --- |
| 关键词 | `keyword` | 匹配任务名或错误码。 |
| 任务 | `taskId` | 下拉选中后精确筛选；`taskName` 仅作预留参数。 |
| DML | `dmlType` | 全部、Insert、Update、Delete。 |
| 错误类型 | `errorType` | 使用第 3.2 节枚举。 |

### 6.4 更多筛选

更多筛选放在弹层中：

| 控件 | Query | 行为 |
| --- | --- | --- |
| 来源表 | `sourceTable` | 包含匹配。 |
| 目标表 | `targetTable` | 包含匹配。 |
| 失败时间 | `startTime`、`endTime` | 传 Unix 毫秒字符串，按 `failedAt` 闭区间查询。 |

用户修改弹层中的临时值时不立即请求；点击“应用筛选”后才更新正式筛选、URL query、列表和汇总。

### 6.5 URL query

- 状态和全部筛选条件同步到 `route.query`。
- 页面加载时从 query 恢复表单和状态标签。
- 清空筛选时移除 query key，不传空字符串。
- query 变化后列表回到第一页，并并行刷新列表和汇总。

## 7. 列表设计

### 7.1 表格列

| 列 | 字段 | 默认显示 | 说明 |
| --- | --- | --- | --- |
| 选择 | - | 是 | 仅 `PENDING`、`RECOVERY_FAILED` 可选。 |
| 任务 | `taskName` | 是 | 可跳转现有任务监控。 |
| 来源表 | `sourceTable` | 否 | 显示列设置可开启。 |
| 目标表 | `targetTable` | 否 | 显示列设置可开启。 |
| DML | `dmlType` | 是 | 标签展示。 |
| 错误类型 | `errorType` | 是 | 枚举文案。 |
| 错误码 | `errorCode` | 是 | 超长省略并显示 tooltip。 |
| 事件时间 | `eventTime` | 否 | 显示列设置可开启。 |
| 失败时间 | `failedAt` | 是 | 默认排序字段。 |
| 状态 | `status` | 是 | 状态标签。 |
| 重处理次数 | `recoveryCount` | 是 | 数字。 |
| 最近重处理时间 | `lastRecoveryTime` | 否 | 显示列设置可开启。 |
| 操作 | - | 是 | 详情、重处理或查看进度。 |

`eventId` 不作为可见列。它只作为 `row-key`、详情路径参数和重处理请求参数保存在前端行对象中。

### 7.2 行选择

- 可选择状态仅为 `PENDING`、`RECOVERY_FAILED`。
- 选中第一条后记录 `selectedTaskId`，禁用不同 `taskId` 的行。
- 全选只选当前页、同一任务且状态可重处理的行。
- 列表刷新后，如果任一已选事件状态或任务约束失效，清空选择并提示重新选择。

### 7.3 空态和加载态

| 状态 | 展示 |
| --- | --- |
| 首次加载 | 表格 loading，汇总区 skeleton。 |
| 无筛选且无数据 | “暂无异常事件”。 |
| 有筛选且无结果 | “未找到匹配的异常事件”。 |
| 列表失败 | 保留筛选，展示 message 和刷新按钮。 |
| 无权限 | 页面级无权限提示，并停止自动刷新。 |

空态辅助说明：

```text
本页面只展示已隔离到 DLQ 的记录级异常。网络抖动、数据库临时不可用等共享异常会走任务级重试，请在任务监控或告警中查看。
```

## 8. 详情抽屉设计

### 8.1 打开方式

- 列表行点击“详情”。
- `REPROCESSING` 行点击“查看进度”。
- 预览弹窗需要查看事件时，通过前端持有的内部 `eventId` 打开，但弹窗不显示该 ID。

打开后调用 `GET /api/dql-events/{eventId}`。请求失败时保留抽屉并提供重试；404 时关闭抽屉并刷新列表。

### 8.2 头部

头部展示任务名、状态和失败时间，不展示事件 ID。

- `PENDING`、`RECOVERY_FAILED`：显示“重处理”。
- `REPROCESSING`：显示处理中状态，启动详情轮询。
- 其他状态：只展示详情刷新。

### 8.3 内容分区

任务与表流向：

- `taskName`
- `sourceNodeId` / `sourceNodeName` → `targetNodeId` / `targetNodeName`
- `sourceTable` → `targetTable`

失败位置：

- `failedNodeId` / `failedNodeName`
- `stage`
- `tableId`
- `dmlType`
- `eventTime`
- `failedAt`
- `captureSeq`

Payload 元数据与安全状态：

- `eventKey`、`eventKeyMissing`
- `payloadFormat`、`payloadHash`、`payloadSize`
- `payloadComplete`
- `payloadPreview`、`payloadPreviewTruncated`

错误信息：

- `errorType`、`errorCode`
- `errorDetails`、`rawErrorRef`

重处理历史：

| 列 | 字段 |
| --- | --- |
| 批次 | `batchId` |
| 开始时间 | `startedAt` |
| 结束时间 | `finishedAt` |
| 结果 | `result` |
| 消息 | `message` |
| 失败原因 | `errorMessage` |

历史按服务端返回顺序展示；服务端应按最近优先或提供明确顺序。`RUNNING` attempt 是当前进度，终态 attempt 展示完成时间或失败原因。

### 8.4 Payload 提示

- `payloadPreviewTruncated=true`：提示“Payload 预览已截断，仅展示服务端返回的安全预览”。
- `payloadComplete=false`：提示“Payload 不完整，当前事件不可安全重处理”。
- 两个字段语义不同，不能因为预览截断就禁用重处理，也不能把原始 Payload 不完整显示为普通预览截断。

### 8.5 详情轮询

事件状态为 `REPROCESSING` 时，每 3 秒重新请求详情。以下任一条件满足时停止：

- 事件状态不再是 `REPROCESSING`。
- 运行中的 attempt 进入终态。
- 抽屉关闭、页面卸载或用户失去权限。

轮询应静默更新历史和状态，不重复打开 loading 骨架。

## 9. 单条和批量重处理

### 9.1 通用调用顺序

1. 前端检查当前选择是否满足状态和同任务约束。
2. 调用 `POST /api/dql-events/recovery/preview`。
3. 打开预览弹窗，展示服务端顺序、阻塞原因和固定影响说明。
4. 仅 `canSubmit=true` 时允许用户确认。
5. 从 `orderedEvents` 按顺序提取 `eventId`。
6. 调用 `POST /api/dql-events/recovery`，并传 `confirm=true`。
7. 成功后关闭预览、清空选择、刷新列表和汇总。

前端最初的勾选顺序不得直接用于提交。

### 9.2 预览弹窗

固定影响说明：

```text
将使用当前已发布任务配置重处理原始异常事件。重处理期间，任务正常同步可能短暂暂停；重处理完成后恢复。Payload 不会被修改。
```

`orderedEvents` 表格不展示事件 ID，使用以下字段让用户识别和确认顺序：

| 列 | 字段 |
| --- | --- |
| 顺序 | 数组下标 + 1 |
| 来源表 | `sourceTable` |
| 目标表 | `targetTable` |
| DML | `dmlType` |
| 事件时间 | `eventTime` |
| 捕获顺序 | `captureSeq` |

`blockedEvents` 同样不显示 `eventId`，展示来源表、目标表、DML、事件时间、捕获顺序和 `message`。确认按钮只绑定 `canSubmit`；存在阻塞原因时仍完整展示服务端 message。

### 9.3 单条重处理

入口来自列表行或详情抽屉。按钮仅在 `PENDING`、`RECOVERY_FAILED` 时显示。即使前端只提交一条，也必须先 preview，不能直接调用 recovery。

### 9.4 批量重处理

- 选择数量必须大于 0。
- 选择事件必须属于同一 `taskId`。
- 所有事件必须为 `PENDING` 或 `RECOVERY_FAILED`。
- 跨任务、状态变化、Payload 不完整等最终以服务端 preview/recovery 响应为准。

### 9.5 提交后行为

提交成功表示服务端已创建并提交批次，不表示重处理已经成功。页面：

- 不打开批次抽屉。
- 立即刷新列表和汇总。
- 对进入 `REPROCESSING` 的行显示“查看进度”。
- 用户点击“查看进度”后打开事件详情，通过 `recoveryAttempts` 观察运行态、完成态或失败原因。

## 10. 刷新策略

| 刷新对象 | 条件 | 周期 | 停止条件 |
| --- | --- | --- | --- |
| 列表和汇总 | 当前列表存在 `REPROCESSING` | 8 秒 | 当前页无处理中事件、页面不可用或卸载 |
| 事件详情 | 当前详情为 `REPROCESSING` | 3 秒 | attempt 结束、状态变化、抽屉关闭或页面卸载 |

手动刷新始终同时刷新列表和汇总。列表静默刷新不显示全表 loading，并在行状态变化导致选择失效时清空选择。

## 11. 错误和权限交互

前端以 HTTP 语义和服务端 `message` 为主，不硬编码依赖一组尚未在当前 API 契约中冻结的业务错误码。

| HTTP 语义 | 前端处理 |
| --- | --- |
| `400` | 展示参数或确认错误，保留筛选和弹窗。 |
| `404` | 关闭已失效详情，刷新列表；统一提示不存在或无权访问。 |
| `409` | 展示状态/任务/Payload/并发冲突原因，禁用提交，清空失效选择并刷新。 |
| 无权限 | 清空详情和选择，展示无权限状态，停止列表及详情轮询。 |
| 网络错误 | 保留用户上下文，允许重试；不得自行推断操作结果。 |

服务端必须再次校验同一任务、可重处理状态、Payload 完整性、业务键和并发冲突。前端禁用态只优化体验。

## 12. 组件职责和页面状态

### 12.1 `List.vue`

- 管理筛选、URL query、分页、列表、汇总和 8 秒自动刷新。
- 管理选择和同任务约束。
- 协调详情抽屉和预览弹窗。
- 提交成功后刷新，不维护批次抽屉状态。

### 12.2 `SummaryTabs.vue`

- 展示 summary。
- 维护状态标签选择。
- 不直接调用接口。

### 12.3 `EventDetailDrawer.vue`

- 按内部 `eventId` 加载详情，但不显示该 ID。
- 展示 Payload 安全预览和 `recoveryAttempts`。
- 在 `REPROCESSING` 时管理 3 秒详情轮询。
- 抛出单条重处理动作。

### 12.4 `RecoveryPreviewDialog.vue`

- 展示 `orderedEvents`、`blockedEvents`、`message` 和固定影响说明。
- 不显示 eventId。
- 仅 `canSubmit=true` 时允许确认。
- 确认时把预览后的有序 ID 列表交给父组件。

### 12.5 页面状态示例

```typescript
const searchParams = ref<DqlEventQueryParams>({})
const summary = ref<DqlEventSummary>()
const order = ref('-failedAt')

const selectedRows = ref<DqlEvent[]>([])
const selectedTaskId = computed(() => selectedRows.value[0]?.taskId)

const detailVisible = ref(false)
const currentEventId = ref('')

const previewVisible = ref(false)
const previewLoading = ref(false)
const previewData = ref<DqlRecoveryPreview>()
const submitting = ref(false)
```

## 13. 安全、响应式和可访问性

- Payload 只读，不请求完整 `payloadData`。
- 错误详情按纯文本渲染，禁止 HTML 注入。
- URL query 不写入 Payload、错误详情或事件 ID。
- 页面不提供事件 ID 的复制或展示入口。
- 权限失效时清空详情、选择和预览数据。
- 桌面端优先；表格在窄屏横向滚动，详情抽屉在小屏使用全宽。
- 弹窗打开后管理焦点；Enter 不直接提交重处理。
- 纯图标刷新按钮提供 tooltip 或 aria label。
- 所有定时器在组件卸载时清理。

## 14. API 与交互映射

| 用户动作 | API | 后续行为 |
| --- | --- | --- |
| 进入页面 | `GET /api/dql-events/summary`、`GET /api/dql-events` | 并行加载汇总和列表。 |
| 修改筛选或状态 | 同上 | 更新 URL query，列表回第一页。 |
| 打开详情 | `GET /api/dql-events/{eventId}` | 展示安全详情。 |
| 查看进度 | `GET /api/dql-events/{eventId}` | 打开详情，并在处理中每 3 秒刷新。 |
| 单条/批量预览 | `POST /api/dql-events/recovery/preview` | 展示服务端排序和阻塞原因。 |
| 确认提交 | `POST /api/dql-events/recovery` | 使用 `orderedEvents` 的 eventId 顺序，成功后刷新列表和汇总。 |
| 列表存在处理中事件 | `GET /api/dql-events`、`GET /api/dql-events/summary` | 每 8 秒静默刷新。 |

## 15. 关键边界处理

### 15.1 预览后状态变化

提交接口再次执行校验。发生 409 时保留服务端 message，禁用继续提交，清空选择并刷新列表。

### 15.2 重复点击提交

提交按钮 loading 期间禁用，页面维护唯一 `submitting` 状态。是否做幂等或去重最终由服务端兜底。

### 15.3 自动刷新导致选择失效

任一选择行不再是 `PENDING` 或 `RECOVERY_FAILED` 时，清空选择并提示重新选择。

### 15.4 详情长时间处于处理中

前端持续按 3 秒请求详情，不自行标记失败。超时或失败状态必须来自服务端 attempt 或事件状态。

### 15.5 服务端保留批次查询接口

批次接口不纳入当前页面调用。诊断工具可以使用该接口，但不能因此恢复 `RecoveryBatchDrawer` 或让前端依赖批次字段完成主流程。

## 16. 研发落地顺序

1. 更新 API 封装和类型，移除批次查询前端依赖。
2. 实现高级功能分组下的路由、菜单和 i18n。
3. 实现汇总、远程分页、显示列设置和 URL query。
4. 实现高频筛选与“更多筛选”应用流程。
5. 实现不展示 eventId 的列表与详情抽屉。
6. 实现 `recoveryAttempts` 历史和 3 秒详情轮询。
7. 实现单条/批量预览，按 `orderedEvents` 提交。
8. 实现提交后列表刷新和 8 秒处理中刷新。
9. 补齐 HTTP 错误语义、权限、空态、安全和可访问性。

## 17. 需求符合性检查

| 契约点 | 前端设计覆盖 |
| --- | --- |
| 高级功能 / 异常事件 | 第 4 节 |
| 汇总与列表筛选一致 | 第 6 节 |
| 高频/更多筛选分组 | 第 6.3、6.4 节 |
| URL query 恢复 | 第 6.5 节 |
| eventId 仅内部定位 | 第 7、8、13 节 |
| Payload 安全预览 | 第 8.3、8.4、13 节 |
| 只允许两个状态重处理 | 第 3.1、7.2、9 节 |
| 同一任务批量 | 第 7.2、9.4 节 |
| 预览权威校验和排序 | 第 9.1、9.2 节 |
| 提交使用预览顺序 | 第 9.1 节 |
| 提交后不打开批次抽屉 | 第 9.5 节 |
| 详情历史承载进度 | 第 8.3、8.5、10 节 |
| 列表 8 秒、详情 3 秒刷新 | 第 10 节 |
| `TARGET_WRITE_ERROR` | 第 3.2、5.2 节 |
| 参数/不存在/冲突错误语义 | 第 11 节 |

## 18. 结论

当前前端闭环由“列表与汇总 → 详情 → 预览 → 提交 → 列表刷新 → 详情历史查看进度”构成。事件详情及其 `recoveryAttempts` 是进度和失败原因的唯一页面入口；批次查询保留为可选诊断能力，不是当前 Web UI 依赖。
