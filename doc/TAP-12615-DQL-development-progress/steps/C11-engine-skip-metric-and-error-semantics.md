# C11 Engine skip 指标与失败语义

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C06-C09
- 范围：目标写入和处理节点 DQL 捕获中的候选 skip 计数、提交/回滚、SkipData 边界及日志语义
- 不包含：C12 Engine 捕获全量回归、TM 重处理执行器、F05 独立 DQL 配置

## 目标

C11 收敛 C07-C09 遗留的指标和失败语义差异，保证 `skip` 计数只代表已经成功创建 DQL 主记录的记录。任何任务级路由、Storm Guard 保护、跳过限制或 TM 上报失败都不能留下“已跳过但没有 DQL 主记录”的计数。

统一处理流程如下：

```text
候选记录
  -> 预留 skip 计数
  -> 异常分类 / Storm Guard / 跳过限制
  -> TM DQL 上报
  -> TM 确认成功
  -> 记录隔离日志 + 提交 skip 计数 + 返回 intercepted/skip

任一失败或任务级路由
  -> 回滚候选 skip 计数
  -> 保留原有任务错误/重试语义
```

## 实现内容

### 1. 目标写入捕获统一候选计数生命周期

- `checkSkip` 在确定性记录异常和 Storm Guard 未知异常进入保护器前预留候选 `skip` 计数。
- 只有分类结果为 `RECORD_DLQ`、通过跳过限制且 `DqlEventReporter` 得到 TM 确认后，候选计数才提交。
- 路由被 Storm Guard 保护、异常属于系统/共享任务级范围、达到 `SkipByLimit`/`SkipByRate` 限制、TM 上报失败或捕获过程运行时失败时，统一在 `finally` 中回滚候选计数。
- `sync` 仍只统计实际成功写入的目标记录，作为跳过比例分母；处理节点捕获不伪造 `sync` 计数。

### 2. 处理节点捕获复用相同语义

- `SkipErrorProcessAspect` 只有在 `SkipData` 生效且 DQL reporter 可用时才进入 DQL 捕获逻辑。
- 处理节点的记录级异常使用和目标写入相同的候选计数、限制判断、上报确认和回滚流程。
- 初始化失败、资源失败、共享异常、非 DML 输入和 reporter 不可用时返回 `null`，继续既有任务级错误处理。
- `LimitMode.Disable` 明确表示不限制跳过，避免缺省/禁用限制模式错误地拒绝全部候选记录。

### 3. 日志区分记录级与任务级结果

- TM 确认成功后输出 `DQL record isolated`，表示记录确实已进入 DQL 并被拦截/跳过。
- Storm Guard、共享/系统异常、跳过限制和 TM 上报失败输出 `DQL task-level handling`，包含任务、表、异常范围、路由和原因，不在任务级日志中拼接完整事件载荷。
- 已有记录级 split 日志和特定写入异常的诊断字段保持不变，用于保留现有排障能力。

## 代码与测试产出

- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTaskTest.java`
- 本步骤文档、开发计划和进度索引

新增/调整的捕获测试覆盖：

- 目标写入记录级 DQL 上报成功并提交 `skip`；
- TM 上报失败、达到跳过限制和 Storm Guard 保护时回滚候选计数；
- 共享任务级异常不拆分、不上报、不增加 `skip`；
- 处理节点记录失败、处理节点上报失败和禁用 `SkipData` 边界；
- 记录隔离日志与任务级日志分流。

## TDD 与验证结果

生产实现前先运行目标写入与处理节点新增测试，预期 RED 为 13 项测试中 6 项失败，失败点集中在计数、日志和禁用 `SkipData` 边界；完成实现后重新验证：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest='SkipErrorEventAspectTaskTest$TargetWriteCaptureTest,SkipErrorEventAspectTaskTest$ProcessCaptureTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 14, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest='SkipErrorEventAspectTaskTest$LaterSuccessCaptureTest,DqlEventReporterTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
DqlEventReporterTest: Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
LaterSuccessCaptureTest: Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/iengine-app -am -DskipTests package
18 个 Reactor 模块均为 SUCCESS
BUILD SUCCESS

git diff --check
通过
```

第二条命令中，`DqlEventReporterTest` 在 `iengine-common` 执行 7 项，`LaterSuccessCaptureTest` 在 `skip-error-event-module` 执行 3 项，均为零失败。

构建仍会输出仓库已有的 Maven model、Lombok、过时 API、Log4j annotation processor 等警告；本步骤未引入编译错误或定向测试失败。

## 设计边界与后续依赖

- `skip` 的可观测值与成功上报的 DQL 主记录保持一致；TM 未确认时不返回记录级 intercepted/skip。
- 任务级路由继续交给原有错误处理和重试机制；C11 不改变 `TapCodeException` 等原始异常的既有传播方式。
- 未开启 `SkipData` 或没有 DQL reporter 的任务不进入处理节点 DQL 捕获；普通目标写入也不注册 C10 后续成功回调。
- C12 继续负责 Engine 捕获范围整体自动化回归和未开启 SkipData/DQL 的前后兼容验证。

## 提交与 review

C11 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 code review 和运行验证确认，不进入 C12。
