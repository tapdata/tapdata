# AffectedKeyCalculator 子表 WITH CTE SQL 查询优化设计

> **日期:** 2026-05-26
> **状态:** 已批准
> **范围:** AffectedKeyCalculator 批量查询优化 + SmartMerger 集成

---

## 1. 目标

优化 `calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys` 方法，使其能够通过 WITH CTE SQL 查询子表对应的宽表主键，而非简单提取事件中的主键值。集成 SmartMerger 处理 JOIN KEY 多次更新场景，保证历史错误数据全被删除。

---

## 2. 架构设计

### 2.1 数据流

```
CDC Events (单表)
    ↓
SmartMerger.mergeEventsSmart() → MergedRecord 列表
    ↓
calculateAffectedBeforeKeys / calculateAffectedAfterKeys
    ↓
提取 before/after 数据行
    ↓
WithCteSqlGenerator.generateBatch(querySql, tableName, rows, fields)
    ↓
执行 SQL 查询 → 提取 wideTablePrimaryKey 字段
    ↓
返回宽表主键集合
```

### 2.2 组件关系

```
AffectedKeyCalculator
    ├── SmartMerger (事件合并)
    ├── WithCteSqlGenerator (WITH SQL 生成)
    ├── DuckDbOperator (SQL 执行)
    └── FromTableConfig (表配置)
```

---

## 3. 核心改造

### 3.1 calculateAffectedBeforeKeys 改造

**职责：** 收集所有历史状态的宽表主键，保证历史错误数据全被删除

**逻辑：**
1. 使用 SmartMerger 合并 CDC 事件为 MergedRecord 列表
2. 遍历每个 MergedRecord 的 `operations` 列表
3. 收集所有 before/after 数据（排除 finalState）
4. 使用 WithCteSqlGenerator 拼接 WITH CTE SQL
5. 将 querySql 中的子表名替换为 WITH 临时表名
6. 执行 SQL 查询，提取 wideTablePrimaryKey 字段值
7. 返回宽表主键集合

**示例：**
```
MergedRecord:
  initialPk: 1
  currentPk: 400
  operations: [
    {op: INSERT, value: {user_id: 100}},
    {op: UPDATE, before: {user_id: 100}, after: {user_id: 200}},
    {op: UPDATE, before: {user_id: 200}, after: {user_id: 300}},
    {op: UPDATE, before: {user_id: 300}, after: {user_id: 400}}
  ]
  finalState: {user_id: 400}

BeforeKeys 数据行: [{user_id: 100}, {user_id: 200}, {user_id: 300}]
WITH SQL 查询 → [100, 200, 300]
```

### 3.2 calculateAffectedAfterKeys 改造

**职责：** 收集最终状态的宽表主键，保证正确数据被插入

**逻辑：**
1. 使用 SmartMerger 合并 CDC 事件为 MergedRecord 列表
2. 遍历每个 MergedRecord 的 `finalState`
3. 收集所有 finalState 数据
4. 使用 WithCteSqlGenerator 拼接 WITH CTE SQL
5. 将 querySql 中的子表名替换为 WITH 临时表名
6. 执行 SQL 查询，提取 wideTablePrimaryKey 字段值
7. 返回宽表主键集合

**示例：**
```
MergedRecord:
  finalState: {user_id: 400}

AfterKeys 数据行: [{user_id: 400}]
WITH SQL 查询 → [400]
```

### 3.3 WITH SQL 拼接逻辑

**表名替换策略：**
- 遍历 `fromTables`，找到与 CDC 事件表名匹配的 FromTableConfig
- 使用 `FromTableConfig.getTableName()` 精确匹配
- 将 `querySql` 中的表名替换为 WITH 临时表名

**SQL 生成：**
```java
// 1. 生成 WITH CTE SQL
String withSql = withCteSqlGenerator.generateBatch(querySql, tableName, rows, fields);

// 2. 替换表名（处理别名情况）
String finalSql = replaceTableNameInQuery(withSql, tableName, fields);

// 3. 执行查询
List<Map<String, Object>> results = operator.executeQuery(finalSql);

// 4. 提取宽表主键
Set<Object> wideTablePks = results.stream()
    .map(row -> row.get(wideTablePrimaryKey))
    .filter(Objects::nonNull)
    .collect(Collectors.toSet());
```

### 3.4 SmartMerger 集成

**MergedRecord 结构：**
```java
public class MergedRecord {
    private Object initialPk;           // 原始主键
    private Object currentPk;           // 当前主键
    private List<Map<String, Object>> operations; // 所有变更操作
    private Map<String, Object> finalState;       // 最终状态
    private String finalOp;             // 最终操作类型
}
```

**操作类型处理：**
- INSERT: 收集 value 数据
- UPDATE: 收集 before 和 after 数据
- DELETE: 收集 before 数据
- DELETE_INSERT: 收集 before 和 after 数据

---

## 4. 错误处理

### 4.1 空数据处理

- 无 before 数据：返回空集合
- 无 after 数据：返回空集合
- SmartMerger 合并结果为空：返回空集合

### 4.2 SQL 执行失败

- 抛出 SQLException，由调用方处理
- 记录详细错误日志（SQL、表名、数据行数）

### 4.3 宽表主键字段不存在

- 记录警告日志
- 跳过该记录，继续处理其他记录

---

## 5. 测试策略

### 5.1 单元测试

- `testCalculateAffectedBeforeKeys_SubTableWithBeforeData` - 子表 before 数据查询宽表主键
- `testCalculateAffectedAfterKeys_SubTableWithAfterData` - 子表 after 数据查询宽表主键
- `testCalculateAffectedBeforeKeys_EmptyBeforeData` - 空 before 数据返回空集合
- `testCalculateAffectedAfterKeys_EmptyAfterData` - 空 after 数据返回空集合
- `testCalculateAffectedBeforeKeys_JoinKeyMultipleUpdates` - JOIN KEY 多次更新场景
- `testCalculateAffectedAfterKeys_JoinKeyMultipleUpdates` - JOIN KEY 多次更新场景

### 5.2 集成测试

- `testQuerySqlReplacement_SubTableWithAlias` - querySql 替换带别名的子表名
- `testSmartMergerIntegration_WithCteQuery` - SmartMerger 与 WITH CTE 集成测试

---

## 6. 文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `AffectedKeyCalculator.java` | 修改 | 改造 calculateAffectedBeforeKeys/AfterKeys 方法 |
| `AffectedKeyCalculatorTest.java` | 修改 | 新增批量查询测试 |
| `SmartMerger.java` | 修改 | 暴露 MergedRecord 操作数据 |
| `WithCteSqlGenerator.java` | 修改 | 支持表名替换 |

---

## 7. 迁移路径

1. **第一阶段（当前）：** 改造 AffectedKeyCalculator，集成 SmartMerger
2. **第二阶段：** 验证 JOIN KEY 多次更新场景正确性
3. **第三阶段：** 性能优化（如需要）
