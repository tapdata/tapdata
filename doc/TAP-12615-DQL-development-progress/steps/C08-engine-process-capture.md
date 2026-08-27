# C08 Engine 处理节点异常捕获

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C04-C06
- 范围：处理节点 `tryProcess` 异常边界暴露单条输入事件，并将可确定的记录级处理失败接入 DQL 分类、Storm Guard、上报确认和拦截
- 不包含：自定义处理节点和脚本异常链扩展、后续成功写入回调、统一 skip 指标修正及回放执行器

## 目标

在处理节点保留现有 `errorHandle` 兜底语义的前提下，新增可被拦截的处理失败事件：

```text
处理节点 catch
  -> 构造 SkipErrorProcessAspect
  -> 单条 DML + PROCESSOR/PROCESSOR 分类
  -> UNKNOWN 经过 Storm Guard
  -> 仅 RECORD_DLQ 构造并上报 DQL
  -> TM 确认成功后返回 intercepted=true
  -> 其他路由继续现有 errorHandle
```

## 实现内容

### 1. 新增处理失败 Aspect

- 新增 `SkipErrorProcessAspect`，继承 `ProcessorNodeAspect`，携带原始 `TapdataEvent`、`ProcessorBaseContext`、异常、失败阶段、处理节点 ID 和名称。
- `HazelcastProcessorBaseNode.tryProcess(...)` 在 `TapCodeException` 和普通异常两个 catch 分支统一构造该 Aspect。
- 普通异常继续按原有逻辑转换为 `TapEventException(UNKNOWN_ERROR)`；若异常链中包含 `TapCodeException`，仍使用链中的原始 `TapCodeException`。
- 只有拦截器返回 `AspectInterceptResult.intercepted(true)` 时才跳过原有 `errorHandle`，因此未接入拦截器或非候选事件行为保持不变。

### 2. 处理节点记录级分类和保护

- `SkipErrorEventAspectTask` 注册 `SkipErrorProcessAspect` 处理器。
- 没有 `TapRecordEvent` 的心跳、控制或其他非 DML 输入直接返回未拦截。
- 单条 DML 使用 C04 分类器的 `DqlFailedStage.PROCESSOR` 和 `DqlNodeType.PROCESSOR` 上下文。
- 初始化异常、共享异常、任务停止或 SkipData 未开启时不会得到 `RECORD_DLQ`，继续现有任务级错误处理路径。
- UNKNOWN 分类先经过 C05 Storm Guard；保护后的任务级路由不会上报或拦截。

### 3. DQL 上报和处理节点元数据

- 处理节点报告记录 `failedStage=PROCESSOR`、处理节点 ID/名称、源表和 DML 类型。
- 复用 C02 的 Payload 快照、C03 的预览/脱敏/身份生成器和 C06 的 `DqlEventReporter`。
- 处理节点表结构可从 `ProcessorBaseContext` 的 `TapTableMap` 解析；解析不到时仍允许使用无表结构身份降级生成报告。
- TM 上报确认成功后才返回 `intercepted=true`；上报器不可用或上报失败时异常继续向调用方传播，不将记录错误地视为已处理。

## 代码与文档产出

- `iengine/api/src/main/java/io/tapdata/aspect/SkipErrorProcessAspect.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastProcessorBaseNode.java`
- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTaskTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先补充失败测试，确认 `SkipErrorProcessAspect` 尚不存在时测试编译失败；再实现 Aspect、处理器注册和 `tryProcess` 接入。

新增 5 个处理节点捕获场景：

1. JavaScript 单条处理失败进入 DQL 并返回拦截结果。
2. 脚本初始化失败不调用 DQL 上报。
3. 共享异常不调用 DQL 上报，保留任务级错误路径。
4. TM 上报失败原样传播，不返回拦截结果。
5. 非 DML 输入不调用 DQL 上报。

处理节点定向测试：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='SkipErrorEventAspectTaskTest$ProcessCaptureTest' test
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Engine 应用编译打包：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

当前 JetBrains JDK 17 环境下，Mockito Inline 无法通过 Byte Buddy self-attach；定向测试使用临时、未提交的 `mock-maker-subclass` 测试资源运行，验证后已删除该资源。完整测试类未作为 C08 通过依据。

## 设计边界与后续依赖

- C08 只接入处理节点的单条 DML 捕获；JS 和自定义处理节点更完整的异常链识别进入 C09。
- 当前捕获开关继续沿用既有 `SkipData` 配置，独立 DQL 配置由 F05 负责。
- skip 指标统一回滚、记录隔离/任务级日志语义和批量口径由 C11 继续收敛。
- C08 完成后等待用户 code review 和运行验证；未经确认不进入 C09。

## 提交与 review

C08 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 review 和验证确认。
