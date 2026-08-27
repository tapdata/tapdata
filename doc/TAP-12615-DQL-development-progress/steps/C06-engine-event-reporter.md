# C06 Engine DQL 事件上报编排

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C01-C05、B08
- 范围：Engine 公共模块中的 TM 事件上报编排、确认结果校验和失败升级；不包含目标写入/处理节点捕获、日志告警接入和 skip 改造

## 目标

为后续 C07-C09 提供唯一的 Engine DQL 事件上报入口。只有 TM 返回可确认的事件主记录后，调用方才可继续后续成功处理；TM 不可用、超时、保存失败、空响应或非法响应均必须转任务错误路径，不能被当作 skip 成功。

## 实现内容

- 新增 `DqlEventReporter`，接收 `DqlTmClient` 并提供 `report(taskId, report)` 编排入口。
- 在进入 TM 调用前校验任务 ID、请求体和路由；Reporter 只允许显式 `RECORD_DLQ`，不限制 `exceptionScope`，因此兼容 C05 输出的 `UNKNOWN + RECORD_DLQ`。
- 复用 `DqlTmClient` 及其底层 `HttpClientMongoOperator` 的鉴权、超时和 HTTP 重试边界；Reporter 不再叠加第二层网络重试，避免同一请求形成不可控的重试乘积。
- TM 返回非空且带有 `eventId` 时视为成功，完整保留 `status` 和 `duplicate`；`duplicate=true` 表示幂等确认成功，不再次编排上报。
- 新增 `DqlEventReportException`，统一包装 TM 运行时失败和非法确认结果，保留原始异常 cause，并只在异常消息中保留任务 ID 及安全的失败摘要，不拼接请求 Payload 或原始异常详情。
- 本步骤不返回 skip 标志、不修改计数、不写日志告警；上报失败由后续捕获链路转换为现有任务错误处理，成功后的 skip 语义由 C07-C11 接入。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/reporter/DqlEventReporter.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/reporter/DqlEventReportException.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/DqlEventReporterTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先新增 `DqlEventReporterTest`，在实现类尚不存在时确认测试编译按预期失败；随后补齐 Reporter 和异常类型，C06 定向测试通过：

```text
mvn -o -pl iengine/iengine-common -am \
  -Djacoco.skip=true -Dtest=DqlEventReporterTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试覆盖正常确认、重复确认、TM 运行时失败包装、异常消息不泄露 Payload、空事件身份、非法路由和非法输入不触发 TM 调用。

补充执行 C01-C06 Engine 公共能力联合回归：

```text
mvn -o -pl iengine/iengine-common -am \
  -Djacoco.skip=true \
  -Dtest=DqlTmClientTest,DqlPayloadSerializerTest,DqlEventIdentityGeneratorTest,\
DqlEventIdentityTest,DqlPayloadPreviewBuilderTest,DqlExceptionClassifierTest,\
DlqStormGuardTest,DqlEventReporterTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 56, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Engine App 下游构建：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

最终提交前执行 `git diff --check`，并创建独立本地 commit，未执行 push。

## 设计边界与后续依赖

- C06 只提供上报编排，不接入现有目标写入或处理节点 catch；C07-C09 负责捕获链路接入。
- C06 不实现真正的 skip；C07 只有在 `report(...)` 正常返回后才允许继续处理 skip，异常必须回到任务错误路径。
- C06 依赖 C01 的 TM Client 作为唯一传输边界，依赖 B08 的首次、重复和保存失败响应语义；TM 保存失败即使返回 `SystemError` 也必须被包装为上报失败。
- 日志和专用告警由 C11、F02、F04 等后续步骤负责，异常消息不携带完整请求体。

## 提交与 review

C06 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户代码 review 和验证确认；确认后再进入 C07。
