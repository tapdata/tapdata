# E02 `TapdataDqlRecoveryEvent`

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-28
- 依赖：C02、E01
- 范围：Engine DQL recovery 事件模型、存储 Payload 快照重建和 recovery 元数据传递

## 分析结论

E01 已把 TM 的 `dqlRecovery` 消息校验和批次幂等边界建立起来；C02 已提供版本化的 `tap-record-event-json-v1` 快照序列化。因此 E02 不重复实现存储访问或自定义 DML 编解码，而是在 `iengine-common` 建立一个可通过 Jet/Hazelcast 传输的事件包装器，并把恢复事件重新放回原任务的 source-to-target 处理链。

`TapdataEvent` 已经存在父类 `eventId`。如果子类再次声明同名 DQL 字段，父类 clone 逻辑只会复制父字段，存在事件身份丢失风险。本步骤采用父类 `eventId` 承载 TM DQL 主记录 ID，并在 DML 的 `info` 中同步写入 `DQL_EVENT_ID`，避免字段遮蔽。

## 完成内容

- 新增 `TapdataDqlRecoveryEvent`，支持 `BEGIN`、`DATA`、`END` 三种 recovery 类型。
- `DATA` 工厂通过 `DqlPayloadSerializer` 从 `DqlPayloadSnapshot` 重建原始 `TapInsertRecordEvent`、`TapUpdateRecordEvent` 或 `TapDeleteRecordEvent`。
- 重建后的 DML 保留 table、before、after、time、referenceTime、原始 info、DML 类型和 `exactlyOnceId`。
- 在 DML info 中增加 `DQL_RECOVERY=true`、`DQL_EVENT_ID`、`DQL_BATCH_ID` 和 `DQL_ATTEMPT_ID`，并使用复制后的 Map，避免修改存储快照对象。
- recovery 事件保存 `batchId`、`attemptId`、`recoveryType`、`operatorId` 和 `taskVersion`，并覆盖 `isDataEvent()`、`isConcurrentWrite()`、`isRecoveryEvent(...)`。
- 覆盖 clone 复制 recovery 字段，确保事件复制后身份、批次、attempt、操作人和任务版本不丢失。

## 代码产出

- `iengine/iengine-common/src/main/java/com/tapdata/entity/TapdataDqlRecoveryEvent.java`
- `iengine/iengine-common/src/test/java/com/tapdata/entity/TapdataDqlRecoveryEventTest.java`

## TDD 与验证结果

### RED

先添加 4 个事件模型测试并执行：

```text
mvn -o -pl iengine/iengine-common -Dtest=TapdataDqlRecoveryEventTest test
```

测试编译按预期失败，失败原因是 `TapdataDqlRecoveryEvent` 类型和工厂方法不存在。

### GREEN

实现后执行：

```text
mvn -o -pl iengine/iengine-common -Dtest=TapdataDqlRecoveryEventTest,DqlPayloadSerializerTest test
```

结果为 9 个测试通过，0 失败，0 错误，`BUILD SUCCESS`。

随后执行 Engine 公共 DQL 全量定向回归：

```text
mvn -o -pl iengine/iengine-common -Dtest='io.tapdata.dql.**' test
```

结果为 61 个测试通过，0 失败，0 错误，`BUILD SUCCESS`。Maven 输出包含仓库已有的 POM 重复依赖、过时 API 和 annotation processor 警告，本步骤未引入新的编译错误。

## 后续依赖

- E03 使用本事件模型从 TM 事件详情 Payload 创建单条恢复事件，并按 `orderedEventIds` 串行注入。
- E08 使用 `isRecoveryEvent(...)` 在目标写入和处理节点捕获边界阻止 recovery 失败递归创建 DQL 主记录。
- E07 使用 recovery 事件的 `eventId`、`batchId` 和 `attemptId` 关联目标完成回调与逐事件屏障。
