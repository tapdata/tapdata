# C03 Engine Payload 预览与身份生成

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：C02
- 范围：Engine 侧安全 Payload 预览、事件键、Payload Hash、记录身份和事件身份工具；不包含异常分类、风暴保护、捕获入口、上报编排或任务配置读取

## 目标

在 C02 清除超限完整 Payload 之前，从原始 `TapRecordEvent` 生成可供 TM 保存和查询的安全摘要。身份和摘要必须确定性、可去重，并且不能因预览展示而泄露密码、Token 或完整敏感值。

## 实现内容

### 安全 Payload 预览

- 新增 `DqlPayloadPreviewBuilder`，按 DML 类型只输出适用的 `key`、`before` 和 `after` 镜像。
- 默认限制为字段值 512 字符、嵌套深度 4、每个 Map/Collection/Array 最多 50 项；限制可通过构造参数覆盖，便于后续由 F05 注入配置快照。
- 对 `password`、`passwd`、`secret`、`token`、`access_token`、`authorization`、`credential`、`apikey` 等字段递归替换为 `******`，并记录 `maskedFields`。
- 长字符串、过深嵌套和超出条目上限的内容被截断，并记录 `truncatedFields`；原始事件 Map 不被修改。

### 记录身份与事件身份

- 新增 `DqlEventIdentityGenerator` 和 `DqlRecordIdentityType`，身份类型覆盖 `PRIMARY_KEY`、`UNIQUE_INDEX`、`FULL_FIELD_HASH` 和 `UNKNOWN`。
- 记录键优先使用表主键，主键不完整时按声明顺序选择第一个完整唯一索引；Insert/Update/Delete 分别从 after、after 优先再回退 before、before 取键值。
- 有完整业务键时生成 `key:<table>:<sha256>` 记录身份；没有键但存在记录镜像时生成 `hash:<table>:<sha256>` 全字段身份；两者都不可用时返回 `UNKNOWN`。
- 事件身份优先级为 `exactlyOnceId`、`info` 中的 source offset/LSN/oplog 位置、业务键、Payload 兜底；哈希使用排序 Map key 的 canonical JSON 和 `sha256:<hex>` 格式，数组顺序保持不变。
- 生成器在 C02 序列化前使用不受业务上限限制的完整快照计算 `payloadHash`，并提供 `applyTo(DqlEventReport)` 将身份元数据写入公共上报模型。

### 公共结果模型

- `DqlEventIdentity` 承载 `eventKey`、`eventKeyMissing`、`payloadHash`、`recordIdentity`、`recordIdentityType`、`recordIdentityFields` 和 `eventIdentity`。
- `DqlPayloadPreview` 承载安全预览及是否发生截断；调用方可以直接映射到 `DqlPayloadSnapshot.payloadPreview` 与 `payloadPreviewTruncated`。

## 代码与文档产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/identity/DqlCanonicalJson.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/identity/DqlEventIdentityGenerator.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlEventIdentity.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlRecordIdentityType.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/preview/DqlPayloadPreview.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/preview/DqlPayloadPreviewBuilder.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/identity/DqlEventIdentityGeneratorTest.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/model/DqlEventIdentityTest.java`
- `iengine/iengine-common/src/test/java/io/tapdata/dql/preview/DqlPayloadPreviewBuilderTest.java`
- 本步骤文档、开发计划、进度索引和 M1 里程碑总结

## TDD 与验证结果

先创建身份与预览测试并确认因 C03 类型和工具不存在而编译失败；补齐最小实现后，补充报告模型映射测试并再次确认 `applyTo` 缺失时编译失败，恢复实现后执行：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlEventIdentityGeneratorTest,DqlEventIdentityTest,DqlPayloadPreviewBuilderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试覆盖主键、唯一索引、全字段 hash、未知身份、事件身份优先级、I/U/D 预览、敏感字段递归脱敏、原始 Map 不变、字段/深度/条目限制和公共报告映射。

随后执行 C01-C03 联合回归：

```text
mvn -o -pl iengine/iengine-common -am -Djacoco.skip=true \
  -Dtest=DqlTmClientTest,DqlPayloadSerializerTest,DqlEventIdentityGeneratorTest,\
DqlEventIdentityTest,DqlPayloadPreviewBuilderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

同时执行 Engine 下游构建：

```text
mvn -o -pl iengine/iengine-app -am -DskipTests package
BUILD SUCCESS
```

构建输出仍包含仓库既有的 Maven model、Lombok 和过时 API 警告，本步骤未引入新的编译错误或测试失败。

## 设计边界与后续依赖

- C03 只提供纯工具和结果模型，不在本步骤把身份生成接入目标写入、处理节点或 DQL 上报链路；C04-C09 负责分类、保护、捕获和上报编排。
- `DqlEventIdentityGenerator` 的 `applyTo` 只写入公共上报模型的身份字段和 `payloadHash`，不会决定 `routeDecision` 或执行 skip。
- F05 负责读取 `dql.event.preview.*`、Payload 上限等配置并在任务启动时形成快照；C03 当前默认值与详细设计一致。
- C10 后续复用本步骤的 `eventKey`、`recordIdentity` 和 `eventTime` 语义上报后续成功写入回调。

## 提交与 review

C03 变更已创建独立本地 commit，未执行 push。提交后暂停开发，等待代码 review 和 Engine/TM 联调验证；用户确认后再进入 C04。
