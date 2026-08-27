# D01 TM 重处理预览

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A05、B02、B03、B10
- 范围：TM 重处理预览校验、服务端排序结果和批次创建前置上下文
- 不包含：D02 独立顺序持久化、D03 同任务并发互斥锁、D04 事件锁事务补偿和 Engine 回放执行器

## 完成内容

### 1. 统一预览校验

生产构造的 `DqlRecoveryBatchService` 现在在预览阶段补齐以下约束：

- 选中事件必须属于同一任务；跨任务选择继续抛出 `DqlRecovery.CrossTaskNotAllowed`。
- 事件状态必须为可重处理状态；状态异常和 Payload 不完整仍按事件返回阻塞原因。
- 事件必须保留可用于恢复定位的业务身份：`eventKeyMissing=true` 或 `recordIdentity` 为空时返回 `event has no business key`。
- 通过 `TaskService` 查询当前任务，任务不存在返回 `Task.NotFound`；仅允许 `running` 和 `stop` 状态进入重处理预览。
- 事件捕获的 `taskVersion` 必须存在并与当前任务版本一致；不一致返回 `task version has changed`。
- 通过 `WorkerService` 查询任务当前 Agent，Agent 不存在、停止、删除、无心跳或心跳超时均返回 `agent is not available`。
- 单批默认最多 200 条事件；超出上限时所有选中事件返回 `recovery batch size exceeds 200`。
- 使用 B03 已提供的 `findActiveByTaskId` 检查 `CREATED`、`DISPATCHED`、`RUNNING` 活动批次；存在活动批次时返回 `an active recovery batch already exists`。

以上新增任务、版本、业务身份和 Agent 校验只由 Spring 生产构造启用；保留原五参数构造器用于既有测试和兼容调用，原有状态、Payload、权限和跨任务行为不变。

### 2. 完整预览摘要与稳定顺序

- `orderedEvents` 返回安全的事件公开摘要，包括事件/任务、来源表、目标表、DML、错误类型和错误码、事件时间、失败时间、捕获序号、状态、恢复次数、节点摘要、Payload 格式/hash/大小/完整性和脱敏预览等字段。
- 预览摘要复用 `DqlEventWebMapper` 的字段映射和脱敏逻辑，不返回原始 `payloadData`、内部记录身份或恢复 attempt。
- 服务端继续按 `event_time ASC, capture_seq ASC, event_id ASC` 排序；提交阶段从预览顺序生成批次 `orderedEventIds`。
- 批次创建时记录当前任务的 `taskStatusBefore`、`taskVersion` 和当前 Agent，供后续批次审计和 Engine 回放校验使用。
- `blockedEvents` 保留 `sourceTable`、`targetTable`、`dmlType`、`eventTime`、`captureSeq` 和用户可读 `message`。

本步骤没有把并发互斥或事件锁事务提前混入预览；预览只读取校验所需状态，不创建批次、不锁事件、不下发消息。

## 代码与文档产出

- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlRecoveryPreviewVo.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

### RED

先新增生产构造入口、业务身份、任务/版本、Agent、活动批次和批次上限测试。既有服务没有任务/Agent 注入构造器，测试按预期在测试编译阶段失败：

```text
无法将类 DqlRecoveryBatchService 中的构造器应用到给定类型
需要 5 个参数，找到 7 个参数
```

补充 `orderedEvents` 完整摘要断言后，再次得到预期编译失败，确认原 `OrderedEvent` 缺少目标表、错误摘要、失败时间和恢复计数字段。

### GREEN

定向执行 D01 测试：

```text
mvn -o -pl manager/tm -am \
  -Djacoco.skip=true \
  -Dtest=DqlRecoveryBatchServiceTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

DQL 相关 TM 回归：

```text
mvn -o -pl manager/tm -am \
  -Djacoco.skip=true \
  -Dtest='DqlEventServiceTest,DqlRecoveryBatchServiceTest,DqlEventControllerTest,DqlEventWebMapperTest,DqlEntityMappingTest,DqlErrorSemanticsTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest,DqlTtlIndexPatchTest,PatchesRunnerTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
Tests run: 97, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

首次不带 Java agent 的测试启动因本机 JetBrains JDK 禁止 Mockito inline 自附加而失败；按仓库既有测试方式显式加载 `mockito-core-5.20.0.jar` agent 后，定向测试和回归均通过。仓库已有 Maven model、Lombok、过时 API 和 Logback 警告未作为本步骤错误处理。

`git diff --check` 在提交前通过。

## 设计边界与后续依赖

- D01 完成 TM 侧重处理预览的前置安全校验和输出契约；活动批次检查是预览阻断，不是 D03 的并发原子互斥保证。
- D02 继续负责将排序结果固化到批次创建和下发链路；D03 负责并发发起时的同任务互斥及恢复策略。
- D04-D10 仍需补齐事件锁事务语义、回调状态机/幂等、超时扫描、批次详情和全量 TM 回归。
- 本步骤创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 code review 和运行验证确认，不进入 D02 或其他后续步骤。
