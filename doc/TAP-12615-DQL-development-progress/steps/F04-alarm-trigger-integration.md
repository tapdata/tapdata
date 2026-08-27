# F04 DQL 告警触发点接入

## 结论

F04 已完成，标记为“待集成验证”。DQL 告警已从仅有 key 和模板，接入到 TM/Engine 的实际业务触发点；告警属于可观测性旁路，不能改变 DQL 事件持久化、Engine skip 决策或重处理状态机。

## 触发点和幂等口径

| 场景 | 触发位置 | 告警 key | 幂等/失败处理 |
| --- | --- | --- | --- |
| 新 DQL 主记录首次落库 | `DqlEventService.report` | `TASK_DQL_EVENT` | 只有首次成功 upsert 调用；重复上报和并发命中重复记录不再次告警 |
| DQL 主记录保存失败 | `DqlEventService.persist` | `TASK_DQL_SAVE_FAILED` | 保存异常向 Engine 返回失败；告警异常被隔离，不覆盖原始保存异常 |
| 单事件回放失败 | `DqlRecoveryBatchService` 结果处理 | `TASK_DQL_RECOVERY_FAILED` | 复用批次失败告警入口，按现有回调状态机幂等 |
| 批次部分失败 | `DqlRecoveryBatchService` 结果处理 | `TASK_DQL_RECOVERY_FAILED` | 与单事件失败使用同一告警语义，计数来自批次摘要 |
| 未知异常 Storm Guard 触发 | Engine 捕获点 -> TM Storm Guard callback | `TASK_DQL_STORM_GUARD` | 每次触发发送一次安全摘要；不再写入新的 DQL 记录，任务级路由保持原选择 |
| 共享异常 | 既有任务错误处理 | 既有任务告警 | 不新增 DQL 事件告警，保持任务级重试/错误语义 |

## 实现内容

- `DqlEventAlarmService` 使用现有 `AlarmService.save(AlarmInfo)`，统一构造 `ING`、`WARNING`、前端组件、同步任务告警类型及 DQL metric。
- 告警参数只允许任务、事件、表、DML、错误分类/错误码、批次、状态、计数、路由和时间等安全摘要；文本统一去除换行、截断，并对 password、token、payload、recordIdentity、eventKey、stackTrace 等敏感内容脱敏。
- 新增 `DqlStormGuardReportVo` 和 `/api/task/{taskId}/dql-events/storm-guard/report`（同时兼容 `/api/Task/...`）内部回调接口。Engine 使用 `DqlStormGuardDecision` 生成报告，guardKey 只发送 SHA-256 摘要，不发送归一化异常文本。
- `DqlEventReporter` 将 Storm Guard callback 的空响应或失败视为上报失败，但捕获侧会隔离该异常，避免告警通道故障改变已确定的任务级路由。
- 事件创建告警由 TM 的去重结果保护；回放失败告警继续依赖 TM 现有批次状态机和回调幂等，不在 Engine 端重复造批次告警。

## 代码和文档产出

- `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlStormGuardReportVo.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventAlarmService.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlEventService.java`
- `manager/tm/src/main/java/com/tapdata/tm/dql/controller/DqlEventController.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlStormGuardReport.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/client/DqlTmClient.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/reporter/DqlEventReporter.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlStormGuardKey.java`
- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlEventAlarmServiceTest.java`
- Engine DQL Client、Reporter、Storm Guard 和捕获回归测试

## 验证记录

通过：

```text
mvn -o -pl iengine/iengine-common \
  -Dtest=DqlTmClientTest,DqlEventReporterTest,DlqStormGuardTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：30 项测试通过，包含 Storm Guard 安全标识、TM 路径和 reporter ack 校验。

通过：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  '-Dtest=SkipErrorEventAspectTaskTest$TargetWriteCaptureTest#stormGuardProtectedFailureShouldRollbackCandidateSkipCount' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：1 项测试通过，确认阈值触发后回滚 skip candidate，并发送安全 Storm Guard 报告。

通过：

```text
mvn -o -pl manager/tm -am \
  -Dtest=DqlEventAlarmServiceTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：3 项测试通过，覆盖事件告警、恢复/部分失败告警和告警后端异常隔离。

完整 `SkipErrorEventAspectTaskTest` 仍存在与本步骤无关的环境/历史基线失败：3 个日志调用次数断言以及 5 个测试中的 Mockito spy `ThreadPoolExecutor` JDK 运行时 NPE；新增的 `TargetWriteCaptureTest` 聚焦测试通过，代码编译通过，已保留该偏差供后续 F07/G12 复核。

## 后续依赖

F05 继续补齐 DQL 系统配置及读取逻辑；G07 负责在真实 TM/Engine 环境验证告警渠道、模板渲染、Storm Guard 告警聚合和不影响业务流程的故障注入结果。
