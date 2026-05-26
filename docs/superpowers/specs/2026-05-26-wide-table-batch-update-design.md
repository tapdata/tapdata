# WideTable 批量更新与全局开关集成设计

> **日期:** 2026-05-26
> **状态:** 已批准
> **范围:** WideTableIncrementalUpdater 批量 SQL 优化 + HazelcastDuckDbSqlNode 集成

---

## 1. 目标

优化 `WideTableIncrementalUpdater.applyEventsToWideTable` 方法，从逐条执行改为批量 SQL 模式，直接刷写到 DuckDB 保证事务原子性。同时通过全局配置开关集成到 `HazelcastDuckDbSqlNode`，默认使用新组件，旧组件标记废弃。

---

## 2. 架构设计

### 2.1 组件关系

```
HazelcastDuckDbSqlNode
    ├── DuckDbSqlConfig (全局配置)
    │       └── useNewWideTableUpdater (默认 true)
    │
    ├── [新] WideTableIncrementalUpdater (默认)
    │       ├── WideTableBatchSqlBuilder (批量 SQL 生成)
    │       ├── FourStateJudge (四态判断)
    │       └── DuckDbOperator.executeUpdate (直接刷写)
    │
    └── [旧] IncrementalViewUpdater (fallback, @Deprecated)
```

### 2.2 数据流

```
CDC Events → cdcEventBuffer → flushCdcBuffer()
    ↓
AffectedKeyCalculator.calculateAffectedBeforeKeys/AfterKeys
    ↓
WideTableIncrementalUpdater.updateWideTableAsTapdataEvents()
    ↓
executeAndUpdate()
    ├── WITH CTE 查询 after 数据
    ├── FourStateJudge.judge() → TapdataEvent 列表
    └── applyEventsToWideTable()
            ├── WideTableBatchSqlBuilder.buildDeleteSql() → executeUpdate
            └── WideTableBatchSqlBuilder.buildInsertSql() → executeUpdate
```

---

## 3. 核心组件设计

### 3.1 WideTableBatchSqlBuilder

**职责：** 生成批量 DELETE/INSERT SQL，使用 VALUES 临时表 JOIN 模式

**DELETE SQL 模板：**
```sql
DELETE FROM {tableName} 
WHERE {primaryKey} IN (
    SELECT pk FROM (VALUES {valuesClause}) AS t(pk)
)
```

示例：
```sql
DELETE FROM wide_table 
WHERE id IN (
    SELECT pk FROM (VALUES (123), (456), (789)) AS t(pk)
)
```

**INSERT SQL 模板：**
```sql
INSERT INTO {tableName} ({columns}) VALUES {rowsClause}
```

示例：
```sql
INSERT INTO wide_table (id, name, email) VALUES 
(123, 'John', 'john@example.com'),
(456, 'Jane', 'jane@example.com')
```

**方法签名：**
```java
public class WideTableBatchSqlBuilder {
    public static String buildDeleteSql(String tableName, String primaryKey, List<Object> primaryKeys);
    public static String buildInsertSql(String tableName, List<String> columns, List<Map<String, Object>> rows);
}
```

### 3.2 WideTableIncrementalUpdater 改造

**applyEventsToWideTable 改造：**
- 使用 `WideTableBatchSqlBuilder` 生成批量 SQL
- 调用 `duckDbOperator.executeUpdate()` 直接执行，绕过 buffer 缓存
- 事务由 `executeInTransaction` 包裹保证原子性

**改造后逻辑：**
```java
private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException, IOException {
    // 1. 收集 DELETE 主键和 INSERT 数据
    List<Object> deletePks = new ArrayList<>();
    List<Map<String, Object>> inserts = new ArrayList<>();
    
    for (TapdataEvent event : events) {
        // 按事件类型收集
    }
    
    // 2. 批量删除（一条 SQL）
    if (!deletePks.isEmpty()) {
        String deleteSql = WideTableBatchSqlBuilder.buildDeleteSql(
            "wide_table", wideTablePrimaryKey, deletePks);
        duckDbOperator.executeUpdate(deleteSql);
    }
    
    // 3. 批量插入（一条 SQL）
    if (!inserts.isEmpty()) {
        String insertSql = WideTableBatchSqlBuilder.buildInsertSql(
            "wide_table", fields, inserts);
        duckDbOperator.executeUpdate(insertSql);
    }
}
```

### 3.3 DuckDbSqlConfig 全局配置

**职责：** 管理 DuckDB SQL 节点的全局配置开关

```java
public class DuckDbSqlConfig {
    private static volatile boolean useNewWideTableUpdater = true;
    
    public static boolean isUseNewWideTableUpdater() {
        return useNewWideTableUpdater;
    }
    
    public static void setUseNewWideTableUpdater(boolean value) {
        useNewWideTableUpdater = value;
    }
}
```

**配置方式：**
- 环境变量：`DUCKDB_USE_NEW_WIDE_TABLE_UPDATER=false`
- 代码设置：`DuckDbSqlConfig.setUseNewWideTableUpdater(false)`
- 默认值：`true`（新组件）

### 3.4 HazelcastDuckDbSqlNode 集成

**doInit 改造：**
```java
// 根据全局配置选择组件
if (DuckDbSqlConfig.isUseNewWideTableUpdater()) {
    wideTableUpdater = new WideTableIncrementalUpdater(
        "wide_table", wideTablePrimaryKey, querySql,
        fields, new WithCteSqlGenerator(), duckDbOperator, true);
    logger.info("Using NEW WideTableIncrementalUpdater");
} else {
    incrementalViewUpdater = new IncrementalViewUpdater(
        outputTableName, wideTablePrimaryKey, querySql,
        outputChangelogEnabled, duckDbOperator);
    logger.warn("Using DEPRECATED IncrementalViewUpdater");
}
```

**flushCdcBuffer 改造：**
```java
if (DuckDbSqlConfig.isUseNewWideTableUpdater()) {
    // 新组件：使用 updateWideTableAsTapdataEvents
    Set<Object> beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTable);
    Set<Object> afterKeys = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTable);
    List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
        beforeKeys, afterKeys, afterRows, tableName);
} else {
    // 旧组件：使用 updateWideTable
    Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys(tableName, events);
    incrementalViewUpdater.updateWideTable(affectedKeys);
}
```

---

## 4. 错误处理

### 4.1 SQL 值转义

- 字符串值：单引号转义 `'` → `''`
- NULL 值：使用 `NULL` 字面量
- 数值类型：直接拼接

### 4.2 事务回滚

- `executeInTransaction` 包裹整个更新流程
- 任何 SQL 执行失败自动回滚
- 异常向上抛出，由 `HazelcastDuckDbSqlNode` 的 error handler 处理

### 4.3 空集合处理

- DELETE 主键列表为空：跳过 DELETE SQL
- INSERT 数据列表为空：跳过 INSERT SQL

---

## 5. 测试策略

### 5.1 WideTableBatchSqlBuilder 单元测试

- `testBuildDeleteSql_SingleKey` - 单主键删除
- `testBuildDeleteSql_MultipleKeys` - 多主键删除
- `testBuildDeleteSql_StringKeys` - 字符串主键转义
- `testBuildInsertSql_SingleRow` - 单行插入
- `testBuildInsertSql_MultipleRows` - 多行插入
- `testBuildInsertSql_NullValues` - NULL 值处理

### 5.2 WideTableIncrementalUpdater 集成测试

- `testApplyEventsToWideTable_BatchDeleteAndInsert` - 批量删除+插入
- `testApplyEventsToWideTable_TransactionRollback` - 事务回滚
- `testApplyEventsToWideTable_EmptyEvents` - 空事件列表

### 5.3 HazelcastDuckDbSqlNode 开关测试

- `testNewComponent_DefaultEnabled` - 默认使用新组件
- `testOldComponent_Fallback` - 开关关闭时使用旧组件

---

## 6. 文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `WideTableBatchSqlBuilder.java` | 新增 | 批量 SQL 生成器 |
| `WideTableBatchSqlBuilderTest.java` | 新增 | 批量 SQL 生成器测试 |
| `DuckDbSqlConfig.java` | 新增 | 全局配置开关 |
| `WideTableIncrementalUpdater.java` | 修改 | 改造 applyEventsToWideTable 为批量模式 |
| `WideTableIncrementalUpdaterTest.java` | 修改 | 新增批量模式测试 |
| `HazelcastDuckDbSqlNode.java` | 修改 | 集成全局开关，兼容新旧组件 |
| `IncrementalViewUpdater.java` | 修改 | 标记 @Deprecated |

---

## 7. 迁移路径

1. **第一阶段（当前）：** 新组件默认启用，旧组件保留
2. **第二阶段：** 观察线上运行情况，收集性能数据
3. **第三阶段：** 确认新组件稳定后，移除旧组件代码
