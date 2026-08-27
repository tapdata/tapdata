# C07 Engine 目标写入异常捕获

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C04-C06
- 范围：现有 `SkipErrorDataAspect` 目标写入边界的批量预分类、单条二次分类、Storm Guard 接入、DQL 上报确认和 skip 指标回滚
- 不包含：处理节点捕获、后续成功写入回调、独立 DQL 配置、回放执行器和新增告警

## 目标

将 C04-C06 的分类器、Storm Guard、Payload/身份工具和 TM 上报器接入现有目标写入拆单流程，满足以下顺序约束：

```text
批量写入失败
  -> 批量分类
  -> 共享/系统异常回现有任务错误路径
  -> 可疑记录级异常才拆单
  -> 每条单写失败再次分类
  -> RECORD_DLQ 才上报 DQL
  -> TM 确认成功后才记录 skip 并返回
```

## 实现内容

### 1. 批量失败前置分类

- 在批量拆单前使用 `DqlBatchContext.batchFailure(...)` 进行目标写入分类。
- 连接不可用、网络抖动、目标库临时不可用、连接池耗尽、线程中断、任务停止等共享或系统异常不再拆成逐条 DQL，原始异常继续交给现有任务重试/错误处理链路。
- 分类结果尚不能确认共享范围、且原有错误码允许记录隔离时，保留原有拆单入口。

### 2. 单条失败二次分类和 Storm Guard

- 每条拆单后的失败使用 `DqlBatchContext.singleRecord()` 重新分类，不能复用批量分类结果。
- `UNKNOWN` 结果先经过 `DlqStormGuard`；保护阈值触发后，不再进行记录级上报。
- 只有 `DqlRouteDecision.RECORD_DLQ` 才继续 DQL 上报；`TASK_RETRY`、`TASK_ERROR` 和被保护的任务级路由均抛回原始异常。

### 3. 上报确认后才 skip

- 在任务、表、DML、事件时间和错误码等元数据基础上构建 `DqlEventReport`。
- 使用 C02 的 Payload 快照、C03 的安全预览和事件身份生成器，目标 `TapTable` 直接参与身份计算，避免仅使用表名导致主键/唯一键身份丢失。
- `HttpClientMongoOperator` 启动时创建 `DqlEventReporter`；TM reporter 不可用时 fail-closed，记录不会被当作 skip 成功。
- 先调用 `DqlEventReporter.report(...)`，只有 TM 返回确认后才调用 `logSkipEvent(...)` 并返回已处理结果。
- 上报失败或运行时异常均转换为 `DqlEventReportException`，交回任务错误路径。

### 4. skip 指标回滚

- skip 计数仅作为限额判断的候选值先递增。
- 达到跳过上限、Storm Guard 保护或非 `RECORD_DLQ` 路由时不保留候选计数。
- TM 上报失败时递减候选计数，避免出现“指标显示已跳过但不存在 DQL 主记录”。

## 代码与文档产出

- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTaskTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先新增目标写入捕获测试，再补齐实现。新增 5 个场景覆盖：

1. 共享批量异常不拆单，保持原异常和任务级处理路径。
2. 目标约束错误拆单后重新分类，成功写入的记录继续同步，失败记录先上报后 skip。
3. 单条失败被任务级路由保护时不调用 TM、不增加 skip 计数。
4. TM 上报失败时抛出 `DqlEventReportException`，skip 候选计数回滚。
5. 未知单条异常经过 Storm Guard 后，允许的记录级结果包含 `UNKNOWN_RECORD_ERROR` 和 `UNKNOWN_SINGLE` 元数据。

C07 定向测试结果：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='SkipErrorEventAspectTaskTest$TargetWriteCaptureTest' test
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

生产模块编译结果：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am -DskipTests package
BUILD SUCCESS
```

当前 JetBrains JDK 17 环境下，Mockito Inline 无法通过 Byte Buddy self-attach；直接执行包含原有 executor spy 用例的完整测试类会在测试初始化阶段报 `Could not self-attach to current VM using external process`。定向测试曾使用临时、未提交的 Mockito subclass 测试资源完成验证，验证后该资源已移除；因此本步骤不将环境受限的完整测试类结果记为通过。C01-C06 公共模块联合回归已在 C06 完成并记录为 56 项通过。

## 设计边界与后续依赖

- 当前 DQL 捕获开关沿用既有 `SkipData` 配置；独立 DQL 配置属于 F05，不在 C07 引入。
- 共享异常不在本类中直接调用 `TaskRetryService`，而是保留原异常，由现有任务错误链路处理，确保既有重试间隔、重试耗尽和任务告警语义不变。
- 本步骤只接入目标写入捕获；处理节点捕获进入 C08，JS/自定义处理节点覆盖进入 C09，后续成功写入回调和统一捕获回归分别进入 C10-C12。
- C07 完成后等待用户 code review 和运行验证；未经确认不进入 C08。

## 提交与 review

C07 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 review 和验证确认。
