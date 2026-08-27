# C09 Engine JS 和自定义处理节点异常捕获

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C08
- 范围：补齐 JavaScript/自定义处理节点的执行异常识别，保留自定义脚本异常链，并复用 C08 的单条处理事件捕获、Storm Guard、DQL 上报和拦截链路
- 不包含：后续成功写入回调、skip 指标统一修正、回放执行器和 Engine 全量捕获回归

## 目标

在 C08 已暴露处理节点输入事件的基础上，区分以下异常边界：

```text
脚本初始化失败 / 共享资源失败
  -> 任务级 errorHandle，不创建 DQL 记录

单条处理执行失败
  -> 处理阶段分类为 TRANSFORM_ERROR
  -> C05 Storm Guard
  -> C06 上报确认
  -> C08 返回 intercepted=true

非 DML、无法定位单条记录或未知异常
  -> 保留任务级或 Storm Guard 保护路径
```

## 实现内容

### 1. 自定义处理器保留脚本执行异常

- `HazelcastCustomProcessor.executeAndGetResult(...)` 捕获 `ScriptException` 时转换为 `ScriptProcessorExCode_30.INVOKE_SCRIPT_FAILED`，并保留原始 `ScriptException` 作为 cause。
- 自定义处理器找不到 `process` 函数时使用同一执行失败 code，并保留 `NoSuchMethodException` cause，避免执行异常退化为无 code 的普通 `RuntimeException`。
- 自定义处理器引擎初始化仍使用已有的 `CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED`，因此初始化失败不会被当作记录转换失败。

### 2. 分类器增加嵌套脚本执行异常规则

- `DqlExceptionClassifier` 继续优先识别系统异常、显式脚本初始化 code 和共享网络/IO 异常。
- 直接的 `ScriptException` 仍按脚本初始化失败处理，返回 `SYSTEM/TASK_ERROR`。
- 在 `PROCESSOR` 阶段，当异常链包含嵌套 `ScriptException` 且已有单条 DML 输入事件时，返回 `RECORD/RECORD_DLQ/TRANSFORM_ERROR`。
- 明确的 `INVOKE_SCRIPT_FAILED` 和已有 JavaScript/Python 执行失败 code 仍按精确规则分类；共享 IO 异常由于优先级更高仍保留任务级重试/错误路径。
- C08 的 `SkipErrorEventAspectTask` 不新增重复分支，继续接收分类结果：只有 `RECORD_DLQ` 在 Storm Guard 允许且 TM 上报确认成功后才拦截。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlExceptionClassifier.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/classifier/DqlExceptionClassifierTest.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastCustomProcessor.java`
- `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastCustomProcessorTest.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTaskTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先补充嵌套脚本异常、直接脚本异常、自定义处理器 cause 保留和 C08 拦截集成测试；在生产实现前确认分类器和拦截测试分别将嵌套异常判为 `UNKNOWN_RECORD_ERROR`，自定义处理器测试仍抛出无 cause 的 `RuntimeException`。完成实现后执行以下定向验证：

```text
mvn -o -pl iengine/iengine-common -am \
  -Djacoco.skip=true -Dtest=DqlExceptionClassifierTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 20, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest='SkipErrorEventAspectTaskTest$ProcessCaptureTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/iengine-app -am \
  -Djacoco.skip=true \
  -Dtest='HazelcastCustomProcessorTest#testExecuteAndGetResultPreservesScriptFailureCause' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

当前 JetBrains JDK 17 环境下，Mockito Inline 无法通过 Byte Buddy self-attach；自定义处理器和 skip-event 定向测试使用了临时、未提交的 `mock-maker-subclass` 测试资源运行，验证后已删除。Engine 构建不依赖该临时配置。构建输出仍包含仓库既有的 Maven model、Lombok、过时 API 和 SLF4J 提示，本步骤未引入新的编译错误或测试失败。

## 设计边界与后续依赖

- C09 只扩展 JS/自定义处理节点的异常识别与 cause 保留，不改变 C08 的 Aspect、Storm Guard 和上报确认语义。
- 单条 DML 是记录级 DQL 的必要条件；批量未拆分、非 DML 和无法定位记录的异常不会被强制转成 DQL。
- 初始化失败和共享 IO/网络失败优先走任务级路径，避免把引擎不可用或连接故障错误地拆成大量记录事件。
- C10 继续负责后续成功写入回调；C11 继续负责 skip 指标和记录隔离/任务级失败语义；C12 负责 Engine 捕获范围的整体回归。
- C09 完成后等待用户 code review 和运行验证；未经确认不进入 C10。

## 提交与 review

C09 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 review 和验证确认。
