# C01 Engine DQL 公共模型与 TM Client

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A03、B08
- 范围：Engine 公共 DQL 数据契约和 TM 内部回调客户端；不包含 Payload 生成、异常分类、风暴保护或捕获入口接入

## 目标

为阶段 C 后续步骤提供稳定的 Engine 侧公共类型和 TM 调用边界，严格对齐最新 API 契约。C01 只负责请求/响应模型、分类结果承载、Payload 快照承载和两个内部回调路径，不提前改变现有任务处理或重试行为。

## 实现内容

### 公共模型

- `DqlEventReport`：覆盖事件、任务、节点、表、DML、事件键、记录身份、错误分类和错误详情字段。
- `DqlPayloadSnapshot`：承载 `payloadFormat`、`payloadData`、`payloadHash`、`payloadSize`、`payloadComplete`、`payloadPreview` 和 `payloadPreviewTruncated`。
- `DqlClassificationResult`：承载 `exceptionScope`、`routeDecision`、`errorType`、`classificationReason` 和 `classificationConfidence`，可应用到事件上报模型。
- `DqlRecordSuccessReport`：覆盖后续成功写入回调的同记录定位字段和成功时间。
- `DqlEventReportResult`、`DqlRecordSuccessReportResult`：承载 TM 的事件上报、重复判定和覆盖风险确认结果。
- 使用类型化枚举固定 `DqlExceptionScope`、`DqlRouteDecision`、`DqlErrorType` 和 `DqlClassificationConfidence` 的契约值。

### TM Client

- `DqlTmClient.reportEvent` 调用 `POST task/{taskId}/dql-events/report`。
- `DqlTmClient.reportRecordSuccess` 调用 `POST task/{taskId}/dql-events/record-success/report`。
- 请求通过 `JSONUtil.mapper` 转为 Map，并使用 `@JsonUnwrapped` 将 Payload 快照字段保持为 API 要求的顶层 camelCase 字段，不产生额外的 `payload` 嵌套对象。
- 复用 `HttpClientMongoOperator`，因此继续沿用现有 TM 鉴权、重试和管理异常映射；Client 不吞掉或重定义底层 HTTP 错误。
- 对空任务 ID、空请求和空响应做边界校验，避免在没有 TM 确认时让调用方继续执行后续 skip 逻辑。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/client/DqlTmClient.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/DqlTmClientTest.java`
- 本步骤文档、开发计划和进度索引

## 验证结果

先以缺少 C01 类型为预期失败完成 RED 检查；补齐实现后执行：

```text
mvn -o -pl iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlTmClientTest -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试覆盖：契约字段和 Payload 顶层展开、两个回调路径和响应（包括 `duplicate=true`）、空响应异常、输入边界校验。由于当前本地 JVM 无法进行 Mockito inline 自附加，HTTP 记录测试使用 recording fake 隔离契约行为，没有修改生产测试运行配置。

同时执行下游 Engine 构建：

```text
mvn -o -pl iengine-app -am -DskipTests package
BUILD SUCCESS
```

## 设计边界与后续依赖

- C01 不实现 `TapRecordEvent` 的 I/U/D 序列化；该能力由 C02 完成。
- C01 不决定异常分类或是否允许 skip；C04、C05 和 C06 负责分类、保护和上报结果编排。
- 后续 C02-C06 应直接复用本步骤模型和 `DqlTmClient`，并继续以最新 API 文档为契约依据。

## 提交与 review

C01 变更已创建独立本地 commit，未执行 push。提交后暂停，等待代码和契约 review 确认。
