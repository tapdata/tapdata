# WideTable 批量更新与全局开关集成实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 优化 WideTableIncrementalUpdater 为批量 SQL 模式，新增全局配置开关集成到 HazelcastDuckDbSqlNode，默认使用新组件。

**Architecture:** 新增 WideTableBatchSqlBuilder 生成批量 DELETE/INSERT SQL，改造 applyEventsToWideTable 使用 executeUpdate 直接刷写。新增 DuckDbSqlConfig 全局开关控制新旧组件选择。

**Tech Stack:** Java, DuckDB, Mockito, JUnit 5, TapdataEvent

---

## File Structure

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

### Task 1: 创建 WideTableBatchSqlBuilder 批量 SQL 生成器

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableBatchSqlBuilder.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableBatchSqlBuilderTest.java`

- [ ] **Step 1: 创建 WideTableBatchSqlBuilder 类**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 宽表批量 SQL 生成器
 * 
 * 职责：生成批量 DELETE/INSERT SQL，使用 VALUES 临时表 JOIN 模式
 */
public class WideTableBatchSqlBuilder {

    /**
     * 构建批量删除 SQL
     * 
     * 模板：
     * DELETE FROM {tableName} 
     * WHERE {primaryKey} IN (
     *     SELECT pk FROM (VALUES {valuesClause}) AS t(pk)
     * )
     */
    public static String buildDeleteSql(String tableName, String primaryKey, List<Object> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("primaryKeys cannot be empty");
        }

        String valuesClause = primaryKeys.stream()
                .map(WideTableBatchSqlBuilder::formatValue)
                .collect(Collectors.joining(", "));

        return String.format(
                "DELETE FROM %s WHERE %s IN (SELECT pk FROM (VALUES %s) AS t(pk))",
                tableName,
                primaryKey,
                valuesClause
        );
    }

    /**
     * 构建批量插入 SQL
     * 
     * 模板：
     * INSERT INTO {tableName} ({columns}) VALUES {rowsClause}
     */
    public static String buildInsertSql(String tableName, List<String> columns, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("rows cannot be empty");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns cannot be empty");
        }

        String columnsClause = String.join(", ", columns);

        String rowsClause = rows.stream()
                .map(row -> buildRowValues(columns, row))
                .collect(Collectors.joining(", "));

        return String.format(
                "INSERT INTO %s (%s) VALUES %s",
                tableName,
                columnsClause,
                rowsClause
        );
    }

    /**
     * 构建单行 VALUES 子句
     */
    private static String buildRowValues(List<String> columns, Map<String, Object> row) {
        String values = columns.stream()
                .map(col -> formatValue(row.get(col)))
                .collect(Collectors.joining(", "));
        return "(" + values + ")";
    }

    /**
     * 格式化 SQL 值
     */
    static String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        return value.toString();
    }
}
```

- [ ] **Step 2: 创建 WideTableBatchSqlBuilderTest 测试类**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class WideTableBatchSqlBuilderTest {

    @Test
    void testBuildDeleteSql_SingleKey() {
        String sql = WideTableBatchSqlBuilder.buildDeleteSql(
                "wide_table", "id", Collections.singletonList(123));

        assertTrue(sql.contains("DELETE FROM wide_table"));
        assertTrue(sql.contains("WHERE id IN"));
        assertTrue(sql.contains("(VALUES (123))"));
    }

    @Test
    void testBuildDeleteSql_MultipleKeys() {
        List<Object> keys = Arrays.asList(123, 456, 789);
        String sql = WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", keys);

        assertTrue(sql.contains("(VALUES (123), (456), (789))"));
    }

    @Test
    void testBuildDeleteSql_StringKeys() {
        List<Object> keys = Arrays.asList("abc", "def'ghi");
        String sql = WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", keys);

        // 验证字符串转义
        assertTrue(sql.contains("'abc'"));
        assertTrue(sql.contains("'def''ghi'"));
    }

    @Test
    void testBuildDeleteSql_EmptyKeys_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", Collections.emptyList()));
    }

    @Test
    void testBuildInsertSql_SingleRow() {
        List<String> columns = Arrays.asList("id", "name", "email");
        List<Map<String, Object>> rows = Collections.singletonList(createRow(1, "John", "john@example.com"));

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, rows);

        assertTrue(sql.contains("INSERT INTO wide_table (id, name, email)"));
        assertTrue(sql.contains("VALUES (1, 'John', 'john@example.com')"));
    }

    @Test
    void testBuildInsertSql_MultipleRows() {
        List<String> columns = Arrays.asList("id", "name");
        List<Map<String, Object>> rows = Arrays.asList(
                createRow(1, "John"),
                createRow(2, "Jane")
        );

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, rows);

        assertTrue(sql.contains("VALUES (1, 'John'), (2, 'Jane')"));
    }

    @Test
    void testBuildInsertSql_NullValues() {
        List<String> columns = Arrays.asList("id", "name");
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        row.put("name", null);

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, Collections.singletonList(row));

        assertTrue(sql.contains("(1, NULL)"));
    }

    @Test
    void testBuildInsertSql_EmptyRows_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                WideTableBatchSqlBuilder.buildInsertSql("wide_table", Arrays.asList("id"), Collections.emptyList()));
    }

    @Test
    void testFormatValue_StringWithQuotes() {
        assertEquals("'it''s'", WideTableBatchSqlBuilder.formatValue("it's"));
    }

    @Test
    void testFormatValue_Boolean() {
        assertEquals("TRUE", WideTableBatchSqlBuilder.formatValue(true));
        assertEquals("FALSE", WideTableBatchSqlBuilder.formatValue(false));
    }

    @Test
    void testFormatValue_Null() {
        assertEquals("NULL", WideTableBatchSqlBuilder.formatValue(null));
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createRow(Object id, String name, String email) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        return row;
    }

    private Map<String, Object> createRow(Object id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }
}
```

- [ ] **Step 3: 编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 运行测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=WideTableBatchSqlBuilderTest 2>&1 | tail -15
```

Expected: All tests pass

- [ ] **Step 5: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableBatchSqlBuilder.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableBatchSqlBuilderTest.java
git commit -m "feat: add WideTableBatchSqlBuilder for batch DELETE/INSERT SQL generation"
```

---

### Task 2: 改造 WideTableIncrementalUpdater 为批量模式

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`

- [ ] **Step 1: 修改 applyEventsToWideTable 方法**

读取当前 [WideTableIncrementalUpdater.java](file:///Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java) 文件，替换 `applyEventsToWideTable` 和 `deleteRowByPk` 方法：

```java
    /**
     * 将事件应用到宽表（批量模式：直接刷写，不走 buffer 缓存）
     */
    private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException, IOException {
        // 1. 收集 DELETE 主键和 INSERT 数据
        List<Object> deletePks = new ArrayList<>();
        List<Map<String, Object>> inserts = new ArrayList<>();

        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapInsertRecordEvent) {
                inserts.add(((TapInsertRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapUpdateRecordEvent) {
                // UPDATE = DELETE old + INSERT new
                Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
                inserts.add(((TapUpdateRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                Map<String, Object> before = ((TapDeleteRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
            }
        }

        // 2. 批量删除（一条 SQL）
        if (!deletePks.isEmpty()) {
            String deleteSql = WideTableBatchSqlBuilder.buildDeleteSql(
                    "wide_table", wideTablePrimaryKey, deletePks);
            logger.debug("Batch delete SQL: {}", deleteSql);
            duckDbOperator.executeUpdate(deleteSql);
        }

        // 3. 批量插入（一条 SQL）
        if (!inserts.isEmpty()) {
            String insertSql = WideTableBatchSqlBuilder.buildInsertSql(
                    "wide_table", fields, inserts);
            logger.debug("Batch insert SQL: {}", insertSql);
            duckDbOperator.executeUpdate(insertSql);
        }
    }
```

删除 `deleteRowByPk` 方法（不再需要）。

- [ ] **Step 2: 添加 WideTableBatchSqlBuilder 导入**

在文件顶部添加导入：
```java
import java.io.IOException;
```

（如果已有则跳过）

- [ ] **Step 3: 编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 运行现有测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=WideTableIncrementalUpdaterTest 2>&1 | tail -15
```

Expected: All tests pass

- [ ] **Step 5: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git commit -m "refactor: convert applyEventsToWideTable to batch SQL mode with direct flush"
```

---

### Task 3: 创建 DuckDbSqlConfig 全局配置开关

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbSqlConfig.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbSqlConfigTest.java`

- [ ] **Step 1: 创建 DuckDbSqlConfig 类**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * DuckDB SQL 节点全局配置
 * 
 * 用于控制节点行为的全局开关，支持环境变量覆盖
 */
public class DuckDbSqlConfig {

    private static final String ENV_USE_NEW_UPDATER = "DUCKDB_USE_NEW_WIDE_TABLE_UPDATER";

    /** 是否使用新的 WideTableIncrementalUpdater（默认 true） */
    private static volatile boolean useNewWideTableUpdater = readFromEnv();

    /**
     * 从环境变量读取配置
     */
    private static boolean readFromEnv() {
        String envValue = System.getenv(ENV_USE_NEW_UPDATER);
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        return true; // 默认启用新组件
    }

    /**
     * 获取是否使用新组件
     */
    public static boolean isUseNewWideTableUpdater() {
        return useNewWideTableUpdater;
    }

    /**
     * 设置是否使用新组件
     * 
     * @param value true=使用新组件，false=使用旧组件（fallback）
     */
    public static void setUseNewWideTableUpdater(boolean value) {
        useNewWideTableUpdater = value;
    }

    /**
     * 重置为环境变量默认值（用于测试）
     */
    static void resetToDefault() {
        useNewWideTableUpdater = readFromEnv();
    }
}
```

- [ ] **Step 2: 创建 DuckDbSqlConfigTest 测试类**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbSqlConfigTest {

    @AfterEach
    void tearDown() {
        // 每个测试后重置配置
        DuckDbSqlConfig.resetToDefault();
    }

    @Test
    void testDefault_IsTrue() {
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testSetToFalse() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(false);
        assertFalse(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testSetToTrue() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(true);
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }

    @Test
    void testResetToDefault() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(false);
        DuckDbSqlConfig.resetToDefault();
        // 默认应该为 true（除非设置了环境变量）
        assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
    }
}
```

- [ ] **Step 3: 编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 运行测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=DuckDbSqlConfigTest 2>&1 | tail -15
```

Expected: All tests pass

- [ ] **Step 5: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbSqlConfig.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbSqlConfigTest.java
git commit -m "feat: add DuckDbSqlConfig global switch for new/old wide table updater"
```

---

### Task 4: 集成全局开关到 HazelcastDuckDbSqlNode

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/IncrementalViewUpdater.java`

- [ ] **Step 1: 标记 IncrementalViewUpdater 为 @Deprecated**

读取 [IncrementalViewUpdater.java](file:///Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/IncrementalViewUpdater.java)，在类定义前添加：

```java
/**
 * @deprecated Use {@link WideTableIncrementalUpdater} instead.
 *             This class will be removed in a future version.
 */
@Deprecated
public class IncrementalViewUpdater {
```

- [ ] **Step 2: 修改 HazelcastDuckDbSqlNode 字段声明**

在字段声明区域添加新组件字段：

```java
// 新增: 宽表增量更新器（新组件）
private WideTableIncrementalUpdater wideTableUpdater;
```

保留旧组件字段（标记为 deprecated）：
```java
// @Deprecated: 使用 wideTableUpdater 替代
private IncrementalViewUpdater incrementalViewUpdater;
```

- [ ] **Step 3: 修改 doInit 方法中的组件初始化逻辑**

找到 `// ========== 新增: 初始化实时增量物化视图组件 ==========` 部分，替换为：

```java
// ========== 新增: 初始化实时增量物化视图组件 ==========
if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
    // 初始化 AffectedKeyCalculator
    affectedKeyCalculator = new AffectedKeyCalculator(
            wideTablePrimaryKey,
            mainTableName,
            mainTablePrimaryKey,
            fromTables,
            customJoinQueries,
            duckDbOperator
    );

    // 根据全局配置选择新组件或旧组件
    if (DuckDbSqlConfig.isUseNewWideTableUpdater()) {
        // 新组件：WideTableIncrementalUpdater（事务模式）
        wideTableUpdater = new WideTableIncrementalUpdater(
                "wide_table",
                wideTablePrimaryKey,
                querySql,
                extractFieldsFromQuery(),
                new WithCteSqlGenerator(),
                duckDbOperator,
                true // 启用事务模式
        );

        // 注册 Changelog 监听器
        if (outputChangelogEnabled) {
            wideTableUpdater.addChangelogListener(event -> {
                logger.debug("Generated changelog event: {}", event);
                // TODO: 将 TapdataEvent 转换为 TapRecordEvent 并输出
            });
        }

        logger.info("Using NEW WideTableIncrementalUpdater (batch SQL mode)");
    } else {
        // 旧组件：IncrementalViewUpdater（fallback）
        incrementalViewUpdater = new IncrementalViewUpdater(
                outputTableName,
                wideTablePrimaryKey,
                querySql,
                outputChangelogEnabled,
                duckDbOperator
        );

        if (outputChangelogEnabled) {
            incrementalViewUpdater.addChangelogListener(changelogEvent -> {
                logger.debug("Generated changelog event (legacy): {}", changelogEvent);
            });
        }

        logger.warn("Using DEPRECATED IncrementalViewUpdater - consider switching to new component");
    }

    logger.info("Materialized view components initialized: affectedKeyCalculator={}, wideTableUpdater={}, incrementalViewUpdater={}",
            affectedKeyCalculator != null, wideTableUpdater != null, incrementalViewUpdater != null);
}
```

- [ ] **Step 4: 添加 extractFieldsFromQuery 辅助方法**

在类中添加新方法：

```java
/**
 * 从查询 SQL 中提取字段列表
 * 简单实现：解析 SELECT 后的字段名
 */
private List<String> extractFieldsFromQuery() {
    // 简单解析：SELECT field1, field2 FROM ...
    String upperSql = querySql.toUpperCase();
    int selectIndex = upperSql.indexOf("SELECT");
    int fromIndex = upperSql.indexOf("FROM");
    
    if (selectIndex >= 0 && fromIndex > selectIndex) {
        String fieldsPart = querySql.substring(selectIndex + 6, fromIndex).trim();
        // 按逗号分割，去除空格
        return Arrays.stream(fieldsPart.split(","))
                .map(String::trim)
                .filter(f -> !f.isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }
    
    // fallback: 返回空列表，由 WithCteSqlGenerator 处理
    return Collections.emptyList();
}
```

- [ ] **Step 5: 修改 flushCdcBuffer 方法**

找到 `flushCdcBuffer()` 方法，替换为：

```java
// ========== 新增: 刷新 CDC 事件缓冲区并更新宽表 ==========
private void flushCdcBuffer() {
    if (cdcEventBuffer.isEmpty()) {
        return;
    }

    try {
        logger.info("Flushing CDC buffer: {} events", cdcEventBuffer.size());

        // 按表名分组
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();
        for (Map<String, Object> event : cdcEventBuffer) {
            String table = (String) event.get("table");
            eventsByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(event);
        }

        if (DuckDbSqlConfig.isUseNewWideTableUpdater() && wideTableUpdater != null) {
            // 新组件：使用 updateWideTableAsTapdataEvents
            flushCdcBufferWithNewComponent(eventsByTable);
        } else if (incrementalViewUpdater != null) {
            // 旧组件：使用 updateWideTable
            flushCdcBufferWithOldComponent(eventsByTable);
        }

        // 清空缓冲区
        cdcEventBuffer.clear();
    } catch (Exception e) {
        logger.error("Failed to flush CDC buffer: {}", e.getMessage(), e);
    }
}

/**
 * 使用新组件刷新 CDC 缓冲区
 */
private void flushCdcBufferWithNewComponent(Map<String, List<Map<String, Object>>> eventsByTable) {
    try {
        // 计算 before/after 主键
        Set<Object> beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTable);

        // 提取 after 数据行
        List<Map<String, Object>> afterRows = extractAfterRowsFromBuffer(eventsByTable);

        // 执行宽表更新
        List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, afterRows, "users");

        logger.info("Updated wide table with {} events (new component)", events.size());
    } catch (Exception e) {
        logger.error("Failed to flush CDC buffer with new component: {}", e.getMessage(), e);
    }
}

/**
 * 使用旧组件刷新 CDC 缓冲区
 */
private void flushCdcBufferWithOldComponent(Map<String, List<Map<String, Object>>> eventsByTable) {
    try {
        // 对每个表计算受影响的主键并更新宽表
        for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> events = entry.getValue();

            // 计算受影响的宽表主键
            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys(tableName, events);

            if (!affectedKeys.isEmpty()) {
                // 批量更新宽表
                int updatedRows = incrementalViewUpdater.updateWideTable(affectedKeys);
                logger.info("Updated {} rows in wide table for table {} and {} affected keys (legacy)",
                        updatedRows, tableName, affectedKeys.size());
            }
        }
    } catch (Exception e) {
        logger.error("Failed to flush CDC buffer with old component: {}", e.getMessage(), e);
    }
}

/**
 * 从 CDC 缓冲区提取 after 数据行
 */
private List<Map<String, Object>> extractAfterRowsFromBuffer(Map<String, List<Map<String, Object>>> eventsByTable) {
    List<Map<String, Object>> afterRows = new ArrayList<>();
    for (List<Map<String, Object>> events : eventsByTable.values()) {
        for (Map<String, Object> event : events) {
            Object record = event.get("record");
            if (record instanceof Map) {
                afterRows.add((Map<String, Object>) record);
            }
        }
    }
    return afterRows;
}
```

- [ ] **Step 6: 添加必要导入**

在文件顶部添加：
```java
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbSqlConfig;
import io.tapdata.flow.engine.V2.node.duckdb.WideTableIncrementalUpdater;
```

- [ ] **Step 7: 编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 8: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/IncrementalViewUpdater.java
git commit -m "feat: integrate global switch into HazelcastDuckDbSqlNode, deprecate old component"
```

---

### Task 5: 全量测试验证与收尾

**Files:**
- All test files

- [ ] **Step 1: 运行所有相关测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest="WideTableBatchSqlBuilderTest,DuckDbSqlConfigTest,WideTableIncrementalUpdaterTest,FourStateJudgeIntegrationTest,WithCteIntegrationTest,BatchWideTableUpdateIntegrationTest" 2>&1 | tail -20
```

Expected: All tests pass

- [ ] **Step 2: 验证全局开关功能**

创建开关集成测试：

```java
// 在 WideTableIncrementalUpdaterTest 中添加
@Test
void testGlobalSwitch_NewComponentEnabled() {
    DuckDbSqlConfig.setUseNewWideTableUpdater(true);
    assertTrue(DuckDbSqlConfig.isUseNewWideTableUpdater());
}

@Test
void testGlobalSwitch_OldComponentFallback() {
    DuckDbSqlConfig.setUseNewWideTableUpdater(false);
    assertFalse(DuckDbSqlConfig.isUseNewWideTableUpdater());
    DuckDbSqlConfig.resetToDefault();
}
```

- [ ] **Step 3: 全量编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: 提交所有测试变更**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/test/
git commit -m "test: add comprehensive tests for batch update and global switch"
```

- [ ] **Step 5: 推送代码**

```bash
cd /Users/hj/workspace/tapdata
git push origin develop-hj
```

---

## 执行顺序

1. Task 1: 创建 WideTableBatchSqlBuilder（基础组件）
2. Task 2: 改造 WideTableIncrementalUpdater（核心逻辑）
3. Task 3: 创建 DuckDbSqlConfig（配置开关）
4. Task 4: 集成到 HazelcastDuckDbSqlNode（节点集成）
5. Task 5: 全量测试验证（质量保障）
