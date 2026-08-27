# B08 Engine 上报 API

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B06、B07

## 完成内容

- 固化 Engine 异常事件上报 `POST /api/task/{taskId}/dql-events/report` 和后续成功写入回调 `POST /api/task/{taskId}/dql-events/record-success/report` 的 Controller 契约；新增调用使用小写 `/api/task` 路径，同时兼容既有 `/api/Task` 路径。
- 两个 Engine 请求继续使用 `IgnoreRequestBodyLog`，避免完整 Payload、事件键和错误详情进入通用请求体日志。
- 上报链路继续复用 B06 的任务、路由、安全校验和 B07 的身份兜底、唯一 upsert、顺序/并发去重及“仅新主记录告警”语义。
- 完成上报契约测试：首次上报返回 `PENDING`；顺序重复不再次持久化或告警；原子 upsert 竞争返回已有事件并标记 `duplicate=true`；显式非 `RECORD_DLQ` 路由被拒绝。
- 完成后续成功回调契约测试：只接收 Repository 选出的最新未完成前序事件并返回覆盖风险；无匹配时返回 `marked=false`，不创建新事件、不改变事件状态。
- 为 `dql_events` 保存增加失败边界：Repository 抛出运行时异常或返回空结果时均视为保存失败，触发 `notifySaveFailed` 后向 Engine 返回 `SystemError`，不返回成功结果，因此 Engine 不得继续 skip 当前记录。
- 保存失败告警原因只保留异常类型及最长 512 字符的非敏感摘要；告警本身失败时仍保留原始持久化错误。

## 设计决策

- B08 不新增独立保存失败错误码，沿用现有 `BizException(Throwable)` 的 `SystemError`；B11 再统一参数、资源、状态冲突和内部错误的 HTTP 语义及可展示 message。
- 保存失败只在实际调用 Repository 后触发；参数校验失败、非法路由、顺序重复和并发重复不触发保存失败告警。
- Repository 返回空结果与抛出异常具有相同的失败语义，避免空值被后续 `eventId` 比较转换成非预期成功或 `NullPointerException`。
- 告警服务是辅助副作用，告警抛错不能遮蔽保存失败；Engine 仍接收到失败响应并走任务错误路径。

## TDD 与验证

- 红灯阶段先增加保存抛错、空保存结果、无匹配后续成功和 Controller 路由映射测试；实现前保存结果为空会在 `eventId` 比较处触发空指针，保存异常也不会触发保存失败告警。
- 新增 5 个 B08 测试：`DqlEventServiceTest` 4 个、`DqlEventControllerTest` 1 个；保留 B06/B07 的首次、重复、非法路由、安全处理和并发去重测试。
- B08 局部执行：`DqlEventControllerTest` 10 个、`DqlEventServiceTest` 21 个，共 31 个测试通过。
- A01-B08 全量回归：`tm-common` 的 `DqlEnumContractTest` 4 个，加上 `tm` 模块指定回归测试 79 个，共 83 个测试通过；Maven reactor 7 个模块全部成功。
- 测试 JVM 使用项目既有的 Mockito javaagent 方式运行；未使用 javaagent 时本机 JetBrains JDK 的 Mockito inline attach 会失败，该问题属于测试运行环境而非断言失败。
- 编译执行：`mvn -o -pl manager/tm -am -DskipTests -Dcheckstyle.skip=true package`，Maven reactor 7 个模块构建成功。

## 后续依赖

- B09 继续完成列表、详情和统计 API 的查询口径及专用 Web DTO，不改变 B08 Engine 回调路径。
- B11 统一 B08 当前使用的 `SystemError` 与参数、资源不存在、状态冲突的 HTTP 错误语义。
- C01/C06 使用 B08 的上报响应、重复响应和失败响应契约；上报失败时仍不得 skip。
- F04 将 `notifySaveFailed` 接入真实告警发送逻辑，同时保持告警失败不影响保存失败返回。
