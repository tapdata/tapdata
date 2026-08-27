# C12 Engine 捕获回归测试

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C01-C11
- 范围：Engine DQL 公共能力、目标写入捕获、现有任务级重试、SkipData 兼容边界及 C07-C11 组合回归
- 不包含：D 阶段重处理执行器、F05 独立 DQL 配置、真实 TM/connector 端到端联调

## 目标

C12 将 C01-C11 的公共工具和 Engine 捕获链路放入同一组回归验证，确认 DQL 报告在目标写入边界的完整性、共享异常的原有任务级处理语义以及未开启 SkipData 时的兼容行为。

本步骤不扩展新的业务路由，仅补齐回归保护。发现的兼容缺口通过最小生产修复闭环：`skipErrorDataNoeAspectHandle` 在 handler 入口增加空 aspect 和非 `SkipData` 模式短路，避免 stale handler wiring 绕过任务配置后进入 DQL 捕获实现。

## 回归矩阵

| 场景 | 验证内容 |
| --- | --- |
| 目标写入完整报告 | 校验任务、表、DML、事件时间、错误码、异常范围、路由决策、错误类型、event/record identity、Payload 格式、数据、hash、脱敏 preview 和 skip 指标 |
| 目标写入敏感数据 | 校验 `password` 等敏感字段在 preview 中被掩码，原始值不泄露 |
| 批量网络异常 | 注入 `SocketTimeoutException`，校验批量保持原有任务级异常包装/重试路径，不拆分为 DQL，不增加 skip |
| 禁用 SkipData | 即使 handler 字段被旧 wiring 留存，禁用配置也直接返回既有错误处理结果，不调用 PDK 或 DQL reporter |
| 已有 C07-C11 组合 | 目标写入捕获、处理节点捕获、后续成功回调与 C12 新测试一起回归 |

## 实现内容

### 1. 新增 Engine 捕获回归测试

新增 `C12EngineCaptureRegressionTest`，通过现有 `SkipErrorEventAspectTask` 捕获入口验证：

- 目标写入失败后生成完整 DQL 报告，并保留 C02/C03 的 Payload、preview 和身份契约；
- TM 确认后才提交 skip 指标；
- 网络类 checked exception 经现有 `throwAsRuntime` 包装后仍保留原始 cause，且不触发 DQL 上报或 skip；
- 禁用 SkipData 的任务不进入 DQL 捕获，即便测试模拟了旧 handler wiring；
- 目标写入异常路径的敏感字段只进入脱敏 preview，不进入报告 preview 的明文内容。

### 2. 修复 handler 配置边界

`skipErrorDataNoeAspectHandle` 与已有处理节点、普通目标写入 handler 保持一致：

- `aspect == null` 时返回 `null`；
- 当前任务不是 `SkipData` 模式时返回 `null`；
- 只有启用 SkipData 才调用 DQL 捕获实现。

该保护不改变 `skipErrorDataNoeAspectImpl` 的分类、上报、计数或异常传播逻辑，也不改变未开启 DQL 任务的原有任务级重试/错误处理路径。

## 代码与文档产出

- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/C12EngineCaptureRegressionTest.java`
- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

生产修改前先运行 C12 测试。修正一个对 checked `SocketTimeoutException` 包装语义的测试断言后，得到预期 RED：

```text
Tests run: 3, Failures: 0, Errors: 1, Skipped: 0
错误：禁用 SkipData 时 handler 仍调用捕获实现，触发 PDK invoker 空指针
```

加入 handler 入口最小保护后，C12 测试 GREEN：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest=C12EngineCaptureRegressionTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

C07-C12 捕获组合回归：

```text
mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest='C12EngineCaptureRegressionTest,SkipErrorEventAspectTaskTest$TargetWriteCaptureTest,SkipErrorEventAspectTaskTest$ProcessCaptureTest,SkipErrorEventAspectTaskTest$LaterSuccessCaptureTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 20, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

C01-C06 Engine 公共能力回归：

```text
mvn -o -pl iengine/iengine-common -am \
  -Djacoco.skip=true \
  -Dtest='DqlTmClientTest,DqlEventReporterTest,DqlExceptionClassifierTest,DlqStormGuardTest,DqlPayloadSerializerTest,DqlPayloadPreviewBuilderTest,DqlEventIdentityGeneratorTest,DqlEventIdentityTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 59, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

C09 自定义处理器脚本异常定向回归：

```text
mvn -o -pl iengine/iengine-app -am \
  -Djacoco.skip=true \
  -Dtest='HazelcastCustomProcessorTest#testExecuteAndGetResultPreservesScriptFailureCause' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Engine 构建验证：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
18 个 Reactor 模块均为 SUCCESS
BUILD SUCCESS
```

`git diff --check` 通过。构建仍会输出仓库已有的 Maven model、Lombok、过时 API、SLF4J 和 Log4j annotation processor 等警告；本步骤未引入编译错误或定向测试失败。Engine package 阶段的 JaCoCo 还会提示部分历史 execution data 与当前 class 不匹配，该提示未影响构建结果。

## 设计边界与后续依赖

- C12 只补齐 Engine 捕获范围回归，不开始 D 阶段重处理执行器。
- 目标写入和处理节点仍遵循 C11 的“TM 确认后才计入 skip”语义。
- 共享网络异常仍由现有任务级重试/错误路径处理，不拆分为大量 DQL 记录。
- 未开启 SkipData 的任务不注册或执行 DQL 捕获；本步骤增加的入口保护用于覆盖旧 wiring、配置切换和直接 handler 调用边界。
- M2 的完整退出仍需要后续 TM/Engine 联调、故障注入和一致性证据；C12 完成不等同于 M2 已验收。

## 提交与 review

C12 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 code review 和运行验证确认，不进入 D01 或其他后续步骤。
