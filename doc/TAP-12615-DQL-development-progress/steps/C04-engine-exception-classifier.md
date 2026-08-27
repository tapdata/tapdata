# C04 Engine 异常分类器

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A04、C01
- 范围：Engine 侧异常链分类和任务/批次上下文模型；不包含 Storm Guard、目标写入捕获、处理节点捕获、DQL 上报编排或 skip 改造

## 目标

在 Engine 进入 DQL 捕获前，依据异常链、错误码、失败阶段、节点类型、单条事件、批次状态和任务状态，稳定输出 `DqlClassificationResult`。分类器必须优先保留任务级重试和错误语义，只有能够证明是单条记录确定性错误时才输出 `RECORD_DLQ`；未知异常交给 C05 的 Storm Guard。

## 实现内容

### 分类上下文

- 新增 `DqlFailedStage` 和 `DqlNodeType`，覆盖源读取、处理器、目标写入、TM 回调及其他阶段/节点。
- 新增 `DqlBatchContext`，记录批量写失败、批次大小、已拆单数量和同类异常数量；提供单条与未拆单批次工厂方法。
- 新增 `DqlTaskContext`，保存任务类型、任务状态、SkipData/DQL 开关、重试耗尽和配置有效性快照；只允许 sync/migrate 任务进入记录级分类。
- 新增 `DqlClassificationContext`，将上述上下文与可选 `TapRecordEvent` 组合，并对必需字段做非空校验。

### 分类优先级和路由

分类顺序固定为：系统级生命周期/任务状态和明确系统错误码 → 脚本初始化失败 → 共享临时异常 → 确定性记录错误 → 无事件批次兜底 → 未知单事件。

| 场景 | 输出 | 实现口径 |
| --- | --- | --- |
| TM 回调、任务停止、线程中断、取消、OOM/进程级错误 | `SYSTEM` + `TASK_ERROR` | 不允许进入 DQL |
| 账号密码、读写权限、offset、配置错误及处理器初始化错误码 | `SYSTEM` + `TASK_ERROR` | 错误码优先于底层异常类型 |
| 脚本引擎初始化、脚本资源/语法初始化失败 | `SYSTEM` + `TASK_ERROR` | 初始化错误优先于其携带的 I/O 原因 |
| 网络/连接/超时、`10001`、`10012`、目标通用可恢复写入错误 | `TASK_SHARED` + `TASK_RETRY` | 重试耗尽时转 `TASK_ERROR` |
| 目标单记录类型、长度、非空、唯一约束及 Poison Record | `RECORD` + `RECORD_DLQ` | 分别映射 `TARGET_WRITE_ERROR` 或 `POISON_RECORD` |
| JS/Python/脚本单条执行失败 | `RECORD` + `RECORD_DLQ` | 映射 `TRANSFORM_ERROR`；外部依赖、资源和初始化失败不走此路由 |
| 单条格式/转换异常 | `RECORD` + `RECORD_DLQ` | `NumberFormatException`、`DateTimeException`、`FieldProcessException` 映射 `MALFORMED_RECORD` |
| 无法构造单条事件 | `SYSTEM` 或 `TASK_SHARED` | 单条候选不进入 DQL；未拆单批次保留任务级重试，耗尽后任务错误 |
| 有单条事件但错误码未知 | `UNKNOWN` + `TASK_RETRY` | 返回 `UNKNOWN_RECORD_ERROR`，等待 C05 Storm Guard 决定是否允许记录级入库 |

异常链按外层到内层稳定遍历，既识别嵌套 `TapCodeException` 的明确错误码，也避免仅依赖异常消息字符串。分类原因只保留错误码、失败阶段和节点类型，不复制原始异常消息。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlFailedStage.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlNodeType.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlBatchContext.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlTaskContext.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlClassificationContext.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlExceptionClassifier.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/classifier/DqlExceptionClassifierTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先创建分类路由矩阵测试，确认在分类上下文和分类器尚不存在时发生测试编译失败；随后补齐最小上下文和分类器实现。开发中增加嵌套错误码、脚本初始化携带 I/O 原因、脚本执行错误、权限/配置错误、转换错误和批量重试耗尽回归用例，最终 C04 分类器测试为：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlExceptionClassifierTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

覆盖确定性目标写入、JS/Python/脚本执行、共享异常重试及耗尽、系统级异常、权限/配置、脚本初始化、无事件单条/批次、未知异常和输入校验。

补充执行仓库级全量测试：

```text
mvn -o -Djacoco.skip=true test
manager/tm-common: Tests run: 559, Failures: 0, Errors: 302, Skipped: 1
```

该命令在 `tm-common` 测试阶段因当前 JDK/沙箱无法完成 Mockito Inline 的 Byte Buddy Agent 自附加而停止，Engine 模块未被执行；该失败不涉及 C04 类和测试。C04 结论以模块定向回归和下游构建结果为准。

随后执行 C01-C04 联合回归：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlTmClientTest,DqlPayloadSerializerTest,DqlEventIdentityGeneratorTest,\
DqlEventIdentityTest,DqlPayloadPreviewBuilderTest,DqlExceptionClassifierTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 36, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Engine 下游构建：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

## 设计边界与后续依赖

- C04 只返回分类结果，不在目标写入、处理节点或 TM 回调中捕获异常，不调用 DQL 上报，也不执行 skip；C06-C09 负责接入这些链路。
- 未知单事件先返回 `UNKNOWN`，由 C05 Storm Guard 按任务、节点、表、错误码和窗口阈值决定是否升级为 `UNKNOWN_SINGLE`/`RECORD_DLQ`。
- `DqlTaskContext` 当前保存 DQL/SkipData 配置快照，但开关的真实读取与默认值由 F05 负责；未开启 DQL 的既有任务不应因本工具改变行为。
- `15019` 属于 `iengine-app` 的 `TaskTargetProcessorExCode_15`，公共模块使用其稳定错误码值，避免反向依赖 Engine App。

## 提交与 review

C04 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户代码 review 和验证确认；确认后再进入 C05 及后续捕获链路。
