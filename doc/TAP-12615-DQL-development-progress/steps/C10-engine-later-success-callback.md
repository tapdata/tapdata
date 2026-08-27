# C10 Engine 后续成功写入回调

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C03、B08
- 范围：普通目标写入成功结果观察、事件身份和事件时间组装、TM 覆盖风险回调
- 不包含：skip 指标统一修正、恢复执行器和 Engine 全量捕获回归

## 目标

在普通目标写入链路中观察实际写入结果。对同批次中没有写入错误的记录，逐条调用 TM 的后续成功写入接口，使 TM 可以按任务、运行记录、记录身份和目标表匹配前序未完成 DQL 事件并标记覆盖风险。

```text
WriteRecordFuncAspect STATE_START
  -> 目标节点执行真实写入
  -> WriteListResult.errorMap 过滤成功记录
  -> 每条成功记录生成身份和事件时间
  -> 调用 TM record-success 回调
```

## 实现内容

### 1. 复用 DQL 身份和 TM 上报能力

- `DqlEventReporter` 增加 `reportRecordSuccess`，复用现有 `DqlTmClient`，校验空响应并将客户端运行时异常转换为 `DqlEventReportException`。
- `DqlEventIdentity` 增加面向 `DqlRecordSuccessReport` 的复制方法，复用 C03 的 `eventKey`、`recordIdentity`、身份类型、身份字段和 `payloadHash`。
- 成功回调填充 `taskRecordId`、source/target/table ID、DML 类型、`eventTime` 和 `successAt`；`captureSeq` 继续为空。事件时间按 `referenceTime`、`time`、回调时间依次回退。

### 2. 接入普通目标写入观察

- `SkipErrorEventAspectTask` 在已有 `SkipData` 生效且存在 DQL reporter 时，为 `WriteRecordFuncAspect.STATE_START` 注册成功结果观察器。
- 仅对 `WriteListResult.errorMap` 中不存在的记录回调 TM；失败记录、空事件列表和空写入结果均不回调。
- 每条记录独立捕获回调异常。TM 审计回调失败不会把已成功的目标写入改判为失败，也不会阻止后续成功记录继续上报。
- 目标表优先使用目标表 ID，无 ID 时回退到目标表名，再回退到事件表 ID。

### 3. 修正拆分写入的观察边界

- `HazelcastTargetPdkDataNode` 将写入结果观察器的输入改为“本次实际写入的事件列表 + 写入结果”。
- 普通批量写入继续传递原批次；`SkipData` 拆分写入时传递当前子批次，避免把未实际写入的同批记录误报为后续成功。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlEventIdentity.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/reporter/DqlEventReporter.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/DqlEventReporterTest.java`
- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTaskTest.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastTargetPdkDataNode.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先补充成功回调、混合成功/失败记录过滤、回调异常继续处理和禁用 `SkipData` 边界测试；在生产实现前，报告器测试因缺少 `reportRecordSuccess` 方法、捕获测试因缺少成功回调入口而处于预期 RED。完成实现后执行以下验证：

```text
mvn -o -pl iengine/iengine-common -am \
  -Djacoco.skip=true -Dtest=DqlEventReporterTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/modules/skip-error-event-module -am \
  -Djacoco.skip=true \
  -Dtest='SkipErrorEventAspectTaskTest$LaterSuccessCaptureTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS

mvn -o -pl iengine/iengine-app -am -DskipTests package
18 个 Reactor 模块均为 SUCCESS
BUILD SUCCESS
```

`git diff --check` 通过。构建输出仍包含仓库既有的 Maven model、Lombok、过时 API 和 Log4j 处理器提示，本步骤未引入编译错误或定向测试失败。

## 设计边界与后续依赖

- C10 只观察普通目标写入成功结果，不替换目标写入主流程，也不改变 `WriteListResult` 的失败语义。
- 成功判定以本次真实写入列表和 `errorMap` 为准；一个成功记录对应一次 TM 回调。
- 回调是成功写入后的 best-effort 审计动作，失败只记录日志；C11 继续负责 skip 指标和失败语义统一，C12 负责 Engine 捕获范围整体回归。
- 未开启 `SkipData` 或未注入 DQL reporter 时不注册成功回调。
- C10 完成后等待用户 code review 和运行验证；未经确认不进入 C11。

## 提交与 review

C10 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 review 和验证确认。
