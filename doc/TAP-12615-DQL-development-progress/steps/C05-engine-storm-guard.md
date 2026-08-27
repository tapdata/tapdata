# C05 Engine Storm Guard

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A05、C04
- 范围：Engine 公共模块中的未知异常窗口保护器和单元测试；不包含实际配置读取、目标/处理节点捕获、DQL 上报编排和告警发送

## 目标

在 C04 将异常分类为 `UNKNOWN` 后，使用任务、失败节点、表、错误码和归一化错误消息建立线程安全的内存窗口。窗口内允许有限的未知单记录进入 DQL；数量或批次比例超过阈值后，停止继续生成记录级 DQL，并返回任务级路由。

## 实现内容

### 配置

新增不可变 `DqlStormGuardConfig`，冻结 A05 的默认值：

| 配置 | 默认值 | 约束 |
| --- | --- | --- |
| `windowSeconds` | `60` | 大于 0 |
| `maxEvents` | `20` | 大于 0；第 20 条仍允许，第 21 条触发数量保护 |
| `maxBatchRatio` | `0.2` | `[0, 1]` |
| `decision` | `TASK_RETRY` | 只能是 `TASK_RETRY` 或 `TASK_ERROR` |

### 窗口 Key 和上下文

- `DqlStormGuardKey` 固化五个保护维度，并对空维度使用稳定占位值。
- 错误消息执行 Unicode 规范化、大小写归一、UUID/IP/十六进制/数字脱敏、空白折叠和 256 字符截断，避免连接请求 ID 等动态内容制造大量窗口。
- `DqlStormGuardContext` 携带批次信息和“是否能定位单条事件”标志。
- `DqlStormGuardDecision` 返回分类结果以及 `guardKey`、窗口起止时间、窗口计数、阈值、批次比例和抑制计数，供 C06/F02 后续链路使用。

### 路由和线程安全

- `exceptionScope != UNKNOWN` 时直接旁路，返回分类结果副本，不打开保护窗口。
- 可定位的未知单事件且窗口计数未超过阈值、未超过批次比例时，输出 `UNKNOWN + RECORD_DLQ + UNKNOWN_RECORD_ERROR + UNKNOWN_SINGLE`。
- 数量超过阈值，或批量写失败且 `sameErrorCount / batchSize` 严格超过批次比例阈值时，输出配置的 `TASK_RETRY`/`TASK_ERROR`，并清除记录级 `errorType`，不允许继续写 DQL。
- 保护一旦在当前窗口触发，后续同 Key 事件持续走任务级路由，直到窗口过期；窗口在过期边界重置。
- `DqlBatchContext.singleRecord()` 不参与批次比例判定，避免单条事件因比例为 `1.0` 被默认阈值误保护。
- 无法定位单条事件的未知错误直接走任务级路由，不进入窗口计数。
- 使用 `ConcurrentHashMap.compute` 对单 Key 原子更新，窗口状态采用不可变快照；提供 `clearExpired()` 防止长任务为历史 Key 持续保留内存。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DlqStormGuard.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlStormGuardConfig.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlStormGuardContext.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlStormGuardDecision.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/classifier/DqlStormGuardKey.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/classifier/DlqStormGuardTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先新增 12 个保护器失败用例并执行，测试编译阶段确认配置和上下文类型尚不存在；随后补齐实现。实现完成后增加窗口清理和结果不变更用例，C05 定向测试为：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DlqStormGuardTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 14, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试覆盖默认配置、数量阈值边界、批次比例阈值、单条比例旁路、窗口过期、窗口清理、Key 隔离、动态消息归一化、不可定位事件、已知分类旁路、输入结果不可变和并发计数。

补充执行 C01-C05 Engine 公共能力联合回归：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlTmClientTest,DqlPayloadSerializerTest,DqlEventIdentityGeneratorTest,\
DqlEventIdentityTest,DqlPayloadPreviewBuilderTest,DqlExceptionClassifierTest,\
DlqStormGuardTest -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 50, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Engine App 下游构建：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

## 设计边界与后续依赖

- C05 是纯保护工具，不读取 Settings，不写日志或告警，不接入目标写入/处理节点 catch，也不调用 TM DQL 上报；实际配置快照由 F05，捕获接入由 C07-C09，上报由 C06，告警由 F02/F04 负责。
- 当前窗口状态是 Engine 进程内存状态，任务重启后重新计数；跨进程或任务 attrs/TM 轻量状态同步属于后续生产化决策，不在本步骤擅自引入。
- C05 的 `DqlStormGuardDecision` 已提供后续告警所需的保护维度、窗口、阈值和抑制计数，但不自行决定专用告警 key。

## 提交与 review

C05 变更创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 review 和验证确认；确认后再进入 C06 或后续捕获接入步骤。
