# B11 统一错误语义与 API 文档

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B08、B09、B10
- 接口依据：`doc/TAP-12615-DLQ-controlled-reprocessing-api.md` V2

## 1. 本步骤目标

为 DQL 列表、详情、预览、提交和批次诊断接口统一错误响应语义，使前端可以优先依据 HTTP 状态和可展示 `message` 处理失败，不依赖尚未冻结的内部错误码集合。

## 2. 完成内容

### 2.1 HTTP 状态映射

`tm-api` 的 `ExceptionHandler` 增加 DQL 请求范围内的状态映射，保留统一 `ResponseMessage` 封装：

|场景|错误码|HTTP 状态|
|---|---|---:|
|参数、Payload、路由或跨任务校验失败|`IllegalArgument`、`DqlEvent.InvalidPayload`、`DqlEvent.InvalidRouteDecision`、`DqlRecovery.CrossTaskNotAllowed`|400|
|菜单或任务数据无权限|`NoPermission`|403|
|事件、任务或批次不存在|`DqlEvent.NotFound`、`Task.NotFound`、`DqlRecovery.BatchNotFound`|404|
|状态不可重处理或锁冲突|`DqlRecovery.EventNotReprocessable`、`DqlRecovery.EventLockFailed`|409|
|未分类服务端异常|`SystemError`|500|

映射仅对 DQL 请求和 DQL 业务错误生效；既有非 DQL 接口的异常状态语义不作扩展。兼容历史权限异常消息 `NoPermission` 的普通运行时异常，同时将 DQL 权限服务统一改为抛出 `BizException("NoPermission")`。

### 2.2 错误消息

在默认、英文、简体中文和繁体中文资源中补齐以下可展示消息：`NoPermission`、`DqlEvent.InvalidPayload`、`DqlEvent.InvalidRouteDecision`、`DqlEvent.NotFound`、`DqlRecovery.CrossTaskNotAllowed`、`DqlRecovery.EventNotReprocessable`、`DqlRecovery.EventLockFailed`、`DqlRecovery.BatchNotFound`。

### 2.3 API 文档同步

`TAP-12615-DLQ-controlled-reprocessing-api.md` 更新为 V2，新增统一错误响应示例、400/403/404/409/500 状态表和前端处理建议，并明确列表/汇总的无数据权限空结果规则。详细设计第 17.2 节同步同一状态表并更新日期；开发计划和本进度索引同步标记 B11 已完成。

## 3. TDD 与验证

先添加 RED 测试，确认旧实现无法满足状态映射、稳定权限错误码和本地化消息：

- `ExceptionHandlerTest`：新增用例初次运行 8 个测试中 5 个失败，分别暴露 400、404、409 状态未设置及 `NoPermission` 被降级为 `SystemError`。
- `DqlErrorSemanticsTest`：初次运行 2 个测试均失败，分别暴露权限异常类型和消息资源缺失。

完成实现后，聚焦测试通过：

```text
tm-api ExceptionHandlerTest: 8 tests, 0 failures, 0 errors
tm DqlErrorSemanticsTest: 2 tests, 0 failures, 0 errors
```

随后执行 A01-B11 相关 TM API、DQL 服务、Repository、权限和错误语义回归：

```text
tm-api ExceptionHandlerTest: 12 tests, 0 failures, 0 errors
tm A01-B11 regression set: 99 tests, 0 failures, 0 errors
```

回归覆盖 DQL Controller、Service、Repository、TTL 初始化、权限、日志和错误语义；Maven 离线构建成功。

## 4. 产出文件

- `manager/tm-api/src/main/java/com/tapdata/tm/base/handler/ExceptionHandler.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventPermissionService.java`
- `manager/tm/src/main/resources/messages*.properties`
- `manager/tm-api/src/test/java/com/tapdata/tm/base/handler/ExceptionHandlerTest.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/DqlErrorSemanticsTest.java`
- `doc/TAP-12615-DLQ-controlled-reprocessing-api.md`
- `doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md`

## 5. 后续依赖

B12 继续补齐 TM 基础能力测试；C/D 阶段联调可直接使用本步骤冻结的错误封装和 HTTP 状态语义。B11 完成本地 commit 后暂停，等待 review 确认再进入后续步骤。
