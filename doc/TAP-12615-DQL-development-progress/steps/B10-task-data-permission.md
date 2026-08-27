# B10 任务数据权限

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A03、B09
- 接口依据：`doc/TAP-12615-DLQ-controlled-reprocessing-api.md`
- 详细设计依据：`doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md` 第 13 节

## 完成内容

- 将 `DataPermissionMenuEnums.ExceptionEvents` 收敛为只声明 `View` 权限 `v2_exception_events`，不再声明异常事件专用的 `Edit` 或 `Start` 权限，落实 POC“可见即可操作”。
- 增加查询任务范围解析：无任务筛选时复用 `TaskService.findAll(Query, user)` 的现有数据权限过滤，仅提取用户可见任务 ID；指定 `taskId` 时先检查任务可见性，不枚举其他任务。
- 列表和汇总 API 在菜单权限通过后使用同一任务 ID 范围查询；汇总的总数和各状态计数复用同一范围，避免列表与统计出现越权或口径不一致。
- 详情 API 在按 `eventId` 查找前检查菜单权限，查到事件后检查事件所属任务可见性；批次详情按相同顺序检查菜单和批次所属任务。
- 重处理预览在读取事件前检查菜单权限，读取后逐一检查所有选中事件所属任务，再执行跨任务拒绝；因此跨任务请求不会绕过其中任一任务的权限检查。提交复用预览校验。
- 未改变 Engine 上报和回调接口的无用户权限流程；B11 继续负责对外错误码和 HTTP 语义统一，F01 继续负责权限资源初始化与升级脚本。

## 设计决策

权限边界以最新 API 契约和详细设计为准：列表、汇总、详情、预览、提交和批次读取均要求 `v2_exception_events` 菜单 View 权限，并按任务数据范围隔离；POC 不额外要求任务 Edit/Start。无任务可见时查询返回空范围，指定不可见任务或读取不可见事件/批次时返回现有统一 `NoPermission` 异常。

查询侧通过 Repository 的任务范围参数注入 `$in` 条件；空范围使用不会命中真实任务 ID 的哨兵值，避免无任务权限时退化为全量查询。事件详情和批次读取保留资源不存在与权限校验的既有异常顺序，同时在菜单无权限时不先读取资源。

## TDD 与验证

- RED 阶段先增加 View-only 菜单、查询范围、详情/预览菜单先验、跨任务逐项校验和汇总共享范围测试，确认旧实现缺少这些约束。
- GREEN 阶段聚焦执行 TM DQL 权限、Service 和 Repository 测试，共 50 个测试，全部通过。
- 权限枚举测试执行 9 个测试，全部通过。
- A01-B10 回归执行 DQL 枚举、日志切面、Repository、批次 Repository、TTL 初始化、身份、校验、Controller、Service 和权限测试，共 97 个测试，全部通过。
- Maven 使用本地离线模式；由于 Mockito inline mock 需要，测试通过 `mockito-core-5.20.0.jar` javaagent 启动。

## 产出文件

- `manager/tm-api/src/main/java/com/tapdata/tm/permissions/constants/DataPermissionMenuEnums.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventPermissionService.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventService.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- 对应权限、查询范围和权限顺序测试

## 后续依赖

B10 已暂停等待 review。B11 需要在本步骤的 `NoPermission`、资源不存在和状态冲突边界上统一 HTTP 状态与可展示 message；F01 仍需补充权限资源初始化、升级兼容和端到端权限验证。确认 review 通过后再进入 B11。
