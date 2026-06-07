# DuckDB 类型归一化与 CDC 阶段重构设计

## 1. 背景

`HazelcastDuckDbSqlNode` 当前存在两类重复成本：

1. **字段值转换重复**：CDC 事件进入 DuckDB 前后，类型判断分散在 `HazelcastDuckDbSqlNode`、`ArrowWriter`、`DuckDbOperatorImpl`，会重复推断值类型并产生 `DateTime -> String`、数值误转字符串等问题。
2. **CDC 合并重复遍历**：`processCdcStage()` 里需要从事件再次抽取 before/after、主键、影响键，导致同一批事件被多次扫描。

本设计的目标是把链路收敛成：

`TapRecordEvent -> SmartMerger 最小化 -> 一次字段值归一化 -> DuckDB 写入`

## 2. 目标

- 最小化字段值转换次数，每个字段只做一次归一化。
- 统一写入、建表、Arrow/Appender 的类型映射规则。
- 消除类型转换过程中的隐式 `toString()`、非法时间字符串、数值精度丢失等问题。
- `processCdcStage()` 只消费 `SmartMerger` 的最小结果集，不再二次遍历事件。
- `AffectedKeyCalculator` 直接使用 `MergedRecord` 中预计算的主键集合。

## 3. 非目标

- 不重写 DuckDB 写入引擎。
- 不改变现有 CDC 语义。
- 不引入新的外部依赖。

## 4. 总体架构

### 4.1 三层职责

1. **Schema Plan**
   - 输入：`NodeSchemaInfo`
   - 输出：字段顺序、主键、nullable、Arrow/DuckDB 类型、字段级归一化规则
   - 职责：提前决定“怎么转”

2. **Row Normalizer**
   - 输入：`TapRecordEvent.after/before`
   - 输出：标准化后的 `Map<String, Object>`
   - 职责：按 schema 做一次值归一化，不猜类型

3. **Writer**
   - 输入：已归一化的行 + schema plan
   - 输出：Arrow / Appender 写入 DuckDB
   - 职责：只写，不再做业务级类型推断

### 4.2 统一链路

`TapRecordEvent -> SmartMerger -> Row Normalizer -> DuckDbOperatorImpl / ArrowWriter -> DuckDB`

`DDL create table` 复用同一个 `TypeConverter`。

## 5. 关键改动

### 5.1 `TypeConverter`

作为唯一类型映射入口：

- `TapType / dataType -> ArrowType`
- `TapType / dataType -> DuckDB Type`
- `field value -> normalized value`

要求：
- 不再散落在 `ArrowWriter`、`DuckDbOperatorImpl` 里重复实现。
- 对时间类统一输出 DuckDB 可接受形式，禁止使用 `DateTime.toString()`。
- 对复杂对象明确策略：JSON 化或拒绝，不允许静默降级为任意字符串。

### 5.2 `NodeSchemaInfo`

补足并固定以下信息：

- 字段顺序
- 主键集合
- `TapField / TapType / dataType / nullable`
- Arrow Schema
- DuckDB 类型

要求：
- 字段顺序稳定，写入列顺序不能漂移。
- Schema 信息只在初始化阶段构建一次。

### 5.3 `HazelcastDuckDbSqlNode`

新增统一归一化入口，例如：

- `normalizeRow(...)`
- `normalizeRecordValue(...)`

改动点：
- `processInitialSyncStage()` 和 `processCdcStage()` 进入写入前先统一归一化。
- `extractAfterRowsFromEvents()` 不再直接吐原始行，而是吐已归一化行。
- `processCdcStage()` 只处理 `MergedRecord`。

### 5.4 `SmartMerger`

`MergedRecord` 固定为 6 个成员：

- `List<Map<String, Object>> beforeRows`
- `List<Map<String, Object>> afterRows`
- `Set<Object> mainTableBeforePks`
- `Set<Object> mainTableAfterPks`
- `String tableName`
- `NodeSchemaInfo schema`

职责：
- 合并同批事件，保留最小必要结果集。
- 不负责写入，不负责值归一化。

### 5.5 `processCdcStage()` 重构

新流程：

1. `SmartMerger.mergeEventsSmart(...)`
2. 直接读取 `MergedRecord.beforeRows / afterRows / mainTableBeforePks / mainTableAfterPks`
3. `AffectedKeyCalculator` 直接消费主键集合
4. DuckDB 事务内执行删除和写入
5. 宽表更新只消费最小结果集

禁止事项：

- 不再从 `TapdataEvent` 二次抽 before/after
- 不再从行数据重复推主键
- 不在多个地方重复扫描同一批事件

## 6. 转换规则

| 源值类型 | 目标策略 |
|---|---|
| 整数 / 浮点 / Decimal | 按 schema 转成数值对象，不转字符串 |
| Boolean | 保留 Boolean |
| Date / Timestamp / DateTime | 转成 DuckDB 可接受时间对象或标准时间值 |
| byte[] | 保留二进制 |
| String | 原样保留 |
| Array / Map / 复杂对象 | JSON 化或显式拒绝 |

硬规则：

- schema 已知时，不再运行时猜类型。
- 不能安全转换时，进入 DLQ / 错误处理链，不允许静默修正。

## 7. 错误处理

- 转换失败时必须带上：字段名、源类型、目标类型、值摘要、表名。
- 时间类型非法值不得被字符串化后继续写入。
- 对无法恢复的字段，优先失败而不是写错。
- `SmartMerger` 和 `Normalizer` 失败都必须可定位。

## 8. 性能原则

- Schema 只构建一次。
- 每个字段值只转换一次。
- `processCdcStage()` 不做多余遍历。
- `ArrowWriter` 和 `DuckDbOperatorImpl` 不再做重复猜测。
- `mainTableBeforePks / beforeRows` 只保留保证宽表正确性的最小集合。

## 9. 测试方案

1. **单元测试**
   - 时间类型、数值类型、二进制类型、复杂对象的归一化
   - `MergedRecord` 主键集合和行集合最小化

2. **集成测试**
   - CDC 事件从 `SmartMerger` 到 DuckDB 写入的完整链路
   - `processCdcStage()` 只消费 `MergedRecord`

3. **回归测试**
   - `DateTime` 非法字符串问题
   - 建表与写入类型不一致问题
   - 主键冲突与宽表删除/写入正确性

## 10. 验收标准

- 同一字段值在链路中最多转换一次。
- `processCdcStage()` 不再二次遍历事件抽取主键或 before/after。
- `AffectedKeyCalculator` 直接使用 `MergedRecord` 预计算主键集合。
- `ArrowWriter` / `DuckDbOperatorImpl` 不再做运行时类型猜测。
- DuckDB 建表与写入共享同一套类型映射。

## 11. 风险与约束

- 过早压缩 `beforeRows/afterRows` 可能影响宽表正确性，因此 `SmartMerger` 的最小化必须以“能正确删除旧宽表行”为边界。
- 复杂对象若未定义统一策略，可能导致不同数据源表现不一致，因此需要统一 JSON 化/拒绝规则。
- 若 schema 缺失或不完整，必须明确回退，不允许悄悄写错。
