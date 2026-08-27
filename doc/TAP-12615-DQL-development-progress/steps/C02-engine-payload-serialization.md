# C02 Engine Payload 序列化

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A03、A05
- 范围：Engine 侧 `TapRecordEvent` 版本化完整快照、大小判断和安全还原；不包含预览脱敏、Payload Hash、记录身份或事件身份生成

## 目标

为 DQL 捕获和后续源边界回放提供稳定的 `tap-record-event-json-v1` 数据格式。Insert、Update、Delete 必须能够从 `DqlPayloadSnapshot.payloadData` 还原为原始 DML 事件；超过配置上限的事件必须明确标记为不可重处理，且不继续携带超限完整 Payload。

## 实现内容

### 版本化快照

- 新增 `DqlPayloadSerializer`，默认 Payload 上限为 `1048576` 字节，并支持通过构造参数形成任务启动时使用的配置快照。
- 序列化时使用 `JSONUtil.mapper` 提取 `TapRecordEvent` 的公开字段，并增加 `tapEventClass` 类型判别字段。
- 快照保留 `type`、`tableId`、`time`、`referenceTime`、`before/after`、`info`、`exactlyOnceId`，以及连接器、非法日期字段、removed fields 和 replace event 等事件自身可序列化元数据。
- `payloadFormat` 固定为 `tap-record-event-json-v1`；`payloadSize` 使用完整 Payload JSON 的 UTF-8 字节长度。

### 超限语义

- Payload 大小小于或等于上限时保存完整 `payloadData`，并设置 `payloadComplete=true`。
- Payload 大于上限时保留格式与实际 `payloadSize`，移除 `payloadData` 并设置 `payloadComplete=false`。
- 不完整快照不能反序列化，后续由 TM 保存为不可重处理事件；C03 可在清除完整 Payload 前基于原事件生成哈希和身份摘要。
- 反序列化重新计算实际 JSON 字节数，并同时检查 `payloadSize` 声明值，不能仅依赖 `payloadComplete=true` 绕过大小上限。

### 受控反序列化

- 只允许 `TapInsertRecordEvent`、`TapUpdateRecordEvent` 和 `TapDeleteRecordEvent` 三种类型，不按请求中的任意类名进行反射加载。
- 同时校验 `tapEventClass` 和 `type`，两者不匹配时拒绝还原。
- 未知格式、空或非对象 Payload、不完整快照和非法事件类型统一以参数错误拒绝，避免产生不可预测的回放事件。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/serializer/DqlPayloadSerializer.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/serializer/DqlPayloadSerializerTest.java`
- 本步骤文档、开发计划和进度索引

## TDD 与验证结果

先创建 C02 测试并确认其仅因 `DqlPayloadSerializer` 尚不存在而编译失败；补齐最小实现后执行：

```text
mvn -o -pl iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlPayloadSerializerTest -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试覆盖 Insert、Update、Delete 往返，事件基础字段和 Update 专属元数据，超限 Payload 清除与不可还原语义，以及未知格式、任意类名和 class/type 不一致的拒绝路径。

同时执行 C01-C02 联合回归和 Engine 下游构建：

```text
C01-C02 focused regression: Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
mvn -o -pl iengine-app -am -DskipTests package
BUILD SUCCESS
```

## 设计边界与后续依赖

- C03 负责基于序列化结果生成安全预览、`payloadHash`、`eventKey`、`recordIdentity` 和 `eventIdentity`。
- F05 负责把 `dql.event.payload.maxBytes` Settings 读取、类型校验和默认值统一接入任务配置快照；C02 已提供可注入上限的构造入口。
- E02 后续直接使用本步骤反序列化能力构造 DQL recovery event，并继续保留原 `exactlyOnceId`。

## 提交与 review

C02 变更已创建独立本地 commit，未执行 push。提交后暂停，等待代码和序列化契约 review 确认。
