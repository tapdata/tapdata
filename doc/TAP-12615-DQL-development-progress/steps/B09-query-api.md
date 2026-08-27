# B09 列表、详情和统计 API

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B02、B06
- 接口依据：`doc/TAP-12615-DLQ-controlled-reprocessing-api.md`

## 完成内容

- 为列表和详情引入独立的查询侧 VO，避免直接序列化 `DqlEventDto` 的持久化、身份和审计字段。
- 列表 API 保留任务、表、关键字、DML、错误类型、状态和失败时间范围筛选；控制器默认分页大小调整为 20，Repository 继续提供 `-failedAt` 默认排序和失败时间闭区间筛选。
- 统计 API 在同一权限上下文下复制查询条件，并清除 `status`、`skip`、`limit`、`order`，使总数和各状态计数对应同一非状态数据集。
- 详情 API 将 `failedStage` 映射为 `stage`，将业务键输出为稳定的脱敏 JSON 字符串，将 attempt 的 `error_details` 映射为 `errorMessage`，并按最近优先返回最多 20 条历史，保留 `RUNNING` 记录。
- 详情预览复用递归脱敏、字段长度、嵌套深度和条目数限制；列表和详情均不返回完整 `payload_data`，列表也不暴露覆盖风险、业务身份等内部字段。
- `currentBatch` 仅作为已有内部调用的兼容字段，并使用 `@JsonIgnore` 排除在当前 Web 契约之外。

## 设计决策

`DqlEventDto` 继续作为 Mongo 映射和服务内部模型；`DqlEventListVo`、`DqlEventDetailVo` 和 `DqlRecoveryAttemptVo` 负责查询侧契约。事件键使用排序后的规范 JSON 输出，敏感字段统一替换为 `******`，以保证稳定性和安全性。详情 attempt 的顺序与前端轮询场景一致：最新记录在前，运行中的 attempt 不被过滤。

## TDD 与验证

- RED 阶段新增并确认以下缺口：控制器默认分页不是 20、统计未清除状态和分页参数、列表可能泄露内部字段、详情字段名称未按契约映射、attempt 顺序不是最近优先。
- GREEN 阶段聚焦执行 `DqlEventControllerTest` 和 `DqlEventServiceTest`，共 37 个测试，全部通过。
- A01-B09 回归执行既有枚举、日志切面、DQL Repository、TTL 初始化、身份、校验、Controller 和 Service 测试，共 84 个测试，全部通过。
- Maven 在本地离线模式运行；由于 Mockito inline mock 需要，命令通过 `mockito-core-5.20.0.jar` javaagent 启动测试。

## 后续依赖

B10 继续补齐任务数据权限的统一错误语义和跨任务隔离；B11 负责对外错误码与 HTTP 语义收敛。B09 已暂停，等待代码 review 和前端联调确认后再进入后续步骤。
