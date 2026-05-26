# WITH CTE SQL Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create WithCteSqlGenerator to embed CDC data into WITH CTE clauses, replacing the simple IN clause approach in WideTableIncrementalUpdater

**Architecture:** WithCteSqlGenerator generates SQL that embeds CDC event data as VALUES in a WITH clause, creating a temporary table that overrides the source table. This allows complex SQL (JOIN/CTE/UNION) to work without parsing. WideTableIncrementalUpdater integrates the generator for after SQL generation.

**Tech Stack:** Java 17, JUnit 5, Mockito, DuckDB

---

## File Structure

| File | Operation | Responsibility |
|------|-----------|----------------|
| `WithCteSqlGenerator.java` | Create | Generate WITH CTE SQL from CDC data |
| `WithCteSqlGeneratorTest.java` | Create | Unit tests for SQL generation |
| `WideTableIncrementalUpdater.java` | Modify | Integrate WithCteSqlGenerator, replace IN clause approach |
| `WideTableIncrementalUpdaterTest.java` | Modify | Update tests for WITH CTE integration |

---

### Task 1: Create WithCteSqlGenerator Class

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteSqlGenerator.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteSqlGeneratorTest.java`

- [ ] **Step 1: Write failing tests - VALUES clause generation**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WithCteSqlGeneratorTest {

    private WithCteSqlGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new WithCteSqlGenerator();
    }

    @Test
    void testBuildValuesClause_SingleRow() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", "John");
        rowData.put("status", 1);

        String result = generator.buildValuesClause(rowData, Arrays.asList("id", "name", "status"));

        assertEquals("VALUES (123, 'John', 1)", result);
    }

    @Test
    void testBuildValuesClause_WithNull() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", null);
        rowData.put("status", 1);

        String result = generator.buildValuesClause(rowData, Arrays.asList("id", "name", "status"));

        assertEquals("VALUES (123, NULL, 1)", result);
    }

    @Test
    void testBuildValuesClause_WithSingleQuote() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("name", "O'Brien");

        String result = generator.buildValuesClause(rowData, Arrays.asList("name"));

        assertEquals("VALUES ('O''Brien')", result);
    }

    @Test
    void testBuildValuesClause_WithDoubleValue() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("amount", 99.9);

        String result = generator.buildValuesClause(rowData, Arrays.asList("amount"));

        assertEquals("VALUES (99.9)", result);
    }

    @Test
    void testBuildValuesClause_WithBoolean() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("active", true);
        rowData.put("deleted", false);

        String result = generator.buildValuesClause(rowData, Arrays.asList("active", "deleted"));

        assertEquals("VALUES (TRUE, FALSE)", result);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WithCteSqlGeneratorTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: FAIL with "WithCteSqlGenerator not defined"

- [ ] **Step 3: Implement WithCteSqlGenerator**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * WITH CTE SQL 生成器
 * 将 CDC 数据嵌入 WITH 子句，生成完整的 SQL 语句
 * 
 * 生成格式：
 * WITH table_name AS (VALUES (v1, v2), (v3, v4)) AS t(field1, field2)
 * SELECT ... FROM table_name ...
 */
public class WithCteSqlGenerator {

    private static final Logger logger = LoggerFactory.getLogger(WithCteSqlGenerator.class);

    /**
     * 构建单行 VALUES 子句
     * @param rowData 行数据
     * @param fields 字段列表（按顺序）
     * @return VALUES (v1, v2, ...) 格式字符串
     */
    public String buildValuesClause(Map<String, Object> rowData, List<String> fields) {
        List<String> values = new ArrayList<>();
        for (String field : fields) {
            values.add(formatValue(rowData.get(field)));
        }
        return "VALUES (" + String.join(", ", values) + ")";
    }

    /**
     * 生成单条 WITH CTE SQL
     * @param sqlTemplate SQL 模板（用户原始 SQL）
     * @param tableName 源表名
     * @param rowData 行数据
     * @param fields 字段列表
     * @return 完整的 WITH CTE SQL
     */
    public String generateSingle(String sqlTemplate, String tableName,
                                 Map<String, Object> rowData, List<String> fields) {
        String valuesClause = buildValuesClause(rowData, fields);
        String sql = String.format("WITH %s AS (%s) AS t(%s) %s",
                tableName, valuesClause, String.join(", ", fields), sqlTemplate);
        logger.debug("Generated single WITH CTE SQL for table {}: {}", tableName, sql);
        return sql;
    }

    /**
     * 生成批量 WITH CTE SQL
     * @param sqlTemplate SQL 模板
     * @param tableName 源表名
     * @param rows 多行数据
     * @param fields 字段列表
     * @return 完整的批量 WITH CTE SQL
     */
    public String generateBatch(String sqlTemplate, String tableName,
                                List<Map<String, Object>> rows, List<String> fields) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("rows cannot be null or empty");
        }

        List<String> valueRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            valueRows.add(buildValuesClause(row, fields));
        }
        String valuesClause = String.join(", ", valueRows);
        String sql = String.format("WITH %s AS (%s) AS t(%s) %s",
                tableName, valuesClause, String.join(", ", fields), sqlTemplate);
        logger.debug("Generated batch WITH CTE SQL for table {} ({} rows)", tableName, rows.size());
        return sql;
    }

    /**
     * 格式化值（处理字符串转义、NULL、数字、布尔等）
     */
    protected String formatValue(Object value) {
        if (value == null) return "NULL";
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WithCteSqlGeneratorTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 5: Add WITH CTE generation tests**

```java
    @Test
    void testGenerateSingle_WithCte() {
        String sqlTemplate = "SELECT u.id, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id";
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", "John");
        rowData.put("status", 1);

        String result = generator.generateSingle(sqlTemplate, "users", rowData,
                Arrays.asList("id", "name", "status"));

        assertTrue(result.startsWith("WITH users AS (VALUES (123, 'John', 1)) AS t(id, name, status)"));
        assertTrue(result.contains("SELECT u.id, o.order_id FROM users u"));
    }

    @Test
    void testGenerateBatch_WithCte() {
        String sqlTemplate = "SELECT u.id FROM users u";
        List<Map<String, Object>> rows = new ArrayList<>();

        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("name", "Alice");
        rows.add(row1);

        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("name", "Bob");
        rows.add(row2);

        String result = generator.generateBatch(sqlTemplate, "users", rows,
                Arrays.asList("id", "name"));

        assertTrue(result.contains("VALUES (1, 'Alice'), (2, 'Bob')"));
        assertTrue(result.contains("AS t(id, name)"));
    }

    @Test
    void testGenerateBatch_EmptyRowsThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            generator.generateBatch("SELECT * FROM users", "users", new ArrayList<>(), Arrays.asList("id"))
        );
    }
```

- [ ] **Step 6: Run all tests**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WithCteSqlGeneratorTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteSqlGenerator.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteSqlGeneratorTest.java
git commit -m "feat: add WithCteSqlGenerator for embedding CDC data into WITH CTE clauses"
```

---

### Task 2: Integrate WithCteSqlGenerator into WideTableIncrementalUpdater

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`

- [ ] **Step 1: Write failing test - WITH CTE integration**

Add to `WideTableIncrementalUpdaterTest.java`:

```java
    @Test
    void testUpdateWideTable_WithCteIntegration() throws SQLException {
        // Create updater with WithCteSqlGenerator
        List<String> fields = Arrays.asList("id", "name", "email");
        WithCteSqlGenerator sqlGenerator = new WithCteSqlGenerator();
        WideTableIncrementalUpdater cteUpdater = new WideTableIncrementalUpdater(
                "id", "users", fields, sqlGenerator, mockDuckDbOperator);

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));

        // Mock after data rows (simulating CDC after data)
        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(456, "John", "john@example.com")
        );

        List<Map<String, Object>> queryResult = Arrays.asList(
                createRow(456, "John", "john@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = cteUpdater.updateWideTable(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());
        assertEquals(DELETE, result.get(0).getOpType());
        assertEquals(123, result.get(0).getPrimaryKey());
        assertEquals(INSERT, result.get(1).getOpType());
        assertEquals(456, result.get(1).getPrimaryKey());

        // Verify WITH CTE SQL was generated
        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("VALUES")
        ));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WideTableIncrementalUpdaterTest#testUpdateWideTable_WithCteIntegration -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: FAIL with "method not defined"

- [ ] **Step 3: Update WideTableIncrementalUpdater constructor**

Read current file: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

Replace constructor and add fields:

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 宽表增量更新器
 * 基于 before/after 主键集合实现批量宽表更新
 * 
 * 核心逻辑：
 * 1. 计算纯 DELETE 主键（在 before 中但不在 after 中）
 * 2. 对 after 主键执行宽表查询获取最新数据（使用 WITH CTE）
 * 3. 生成 DELETE/INSERT/UPDATE 事件
 */
public class WideTableIncrementalUpdater {

    private static final Logger logger = LoggerFactory.getLogger(WideTableIncrementalUpdater.class);

    private final String wideTablePrimaryKey;
    private final String querySql;
    private final List<String> fields;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;

    /**
     * 构造函数（向后兼容，使用 IN 子句方式）
     * @deprecated Use constructor with WithCteSqlGenerator
     */
    @Deprecated
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, DuckDbOperator duckDbOperator) {
        this(wideTablePrimaryKey, querySql, fields, new WithCteSqlGenerator(), duckDbOperator);
    }

    /**
     * 构造函数（使用 WITH CTE 方式）
     */
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
    }
```

- [ ] **Step 4: Add new updateWideTable method with WITH CTE support**

Add after existing `updateWideTable` method:

```java
    /**
     * 批量更新宽表（使用 WITH CTE）
     * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
     * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return 宽表 CDC 事件列表
     */
    public List<WideTableCdcEvent> updateWideTable(Set<Object> affectedBeforeKeys,
                                                    Set<Object> affectedAfterKeys,
                                                    List<Map<String, Object>> afterRows,
                                                    String tableName) throws SQLException {
        List<WideTableCdcEvent> events = new ArrayList<>();

        // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
        Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
        pureDeleteKeys.removeAll(affectedAfterKeys);

        // 2. 生成 DELETE 事件
        for (Object pk : pureDeleteKeys) {
            events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.DELETE, pk, null));
            logger.debug("Generated DELETE event for pk={}", pk);
        }

        // 3. 使用 WITH CTE 执行 after 查询
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            List<Map<String, Object>> results = duckDbOperator.executeQuery(afterSql);

            // 4. 生成 INSERT/UPDATE 事件
            for (Map<String, Object> row : results) {
                Object pk = row.get(wideTablePrimaryKey);
                if (pk != null) {
                    if (affectedBeforeKeys.contains(pk)) {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.UPDATE, pk, row));
                        logger.debug("Generated UPDATE event for pk={}", pk);
                    } else {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.INSERT, pk, row));
                        logger.debug("Generated INSERT event for pk={}", pk);
                    }
                }
            }
        }

        logger.info("Generated {} wide table CDC events: {} DELETE, {} INSERT/UPDATE",
                events.size(), pureDeleteKeys.size(), events.size() - pureDeleteKeys.size());

        return events;
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WideTableIncrementalUpdaterTest#testUpdateWideTable_WithCteIntegration -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 6: Run all WideTableIncrementalUpdater tests**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WideTableIncrementalUpdaterTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS (existing tests should still work via deprecated constructor)

- [ ] **Step 7: Commit**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java
git commit -m "feat: integrate WithCteSqlGenerator into WideTableIncrementalUpdater"
```

---

### Task 3: Integration Test - End-to-End WITH CTE Flow

**Files:**
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java`

- [ ] **Step 1: Write integration test**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.*;

import static io.tapdata.flow.engine.V2.node.duckdb.WideTableCdcEvent.OpType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 集成测试 - WITH CTE 端到端流程
 * 验证 AffectedKeyCalculator + WithCteSqlGenerator + WideTableIncrementalUpdater 联合工作
 */
@ExtendWith(MockitoExtension.class)
class WithCteIntegrationTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private AffectedKeyCalculator calculator;
    private WithCteSqlGenerator sqlGenerator;
    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() throws SQLException {
        List<FromTableConfig> fromTables = Arrays.asList(
                new FromTableConfig("users", "id")
        );

        calculator = new AffectedKeyCalculator(
                "id", "users", "id", fromTables, new HashMap<>(), mockDuckDbOperator
        );

        sqlGenerator = new WithCteSqlGenerator();

        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("id",
                "SELECT id, name, email FROM users",
                fields, sqlGenerator, mockDuckDbOperator);
    }

    @Test
    void testEndToEnd_WithCteFlow() throws SQLException {
        // 1. CDC 事件
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();
        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createUpdateEvent("id", 123, 456));
        userEvents.add(createUpdateEvent("id", 789, 789));
        eventsByTable.put("users", userEvents);

        // 2. 计算 before/after 主键
        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        assertEquals(2, beforeKeys.size());
        assertTrue(beforeKeys.contains(123));
        assertTrue(beforeKeys.contains(789));

        assertEquals(2, afterKeys.size());
        assertTrue(afterKeys.contains(456));
        assertTrue(afterKeys.contains(789));

        // 3. 提取 after 数据行
        List<Map<String, Object>> afterRows = new ArrayList<>();
        Map<String, Object> afterRow1 = new HashMap<>();
        afterRow1.put("id", 456);
        afterRow1.put("name", "John");
        afterRow1.put("email", "john@example.com");
        afterRows.add(afterRow1);

        Map<String, Object> afterRow2 = new HashMap<>();
        afterRow2.put("id", 789);
        afterRow2.put("name", "Jane Updated");
        afterRow2.put("email", "jane@example.com");
        afterRows.add(afterRow2);

        // 4. Mock DuckDB 查询结果
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        // 5. 执行宽表更新
        List<WideTableCdcEvent> events = updater.updateWideTable(
                beforeKeys, afterKeys, afterRows, "users");

        // 6. 验证事件
        List<WideTableCdcEvent> deleteEvents = filterByOp(events, DELETE);
        assertEquals(1, deleteEvents.size());
        assertEquals(123, deleteEvents.get(0).getPrimaryKey());

        List<WideTableCdcEvent> insertEvents = filterByOp(events, INSERT);
        assertEquals(1, insertEvents.size());
        assertEquals(456, insertEvents.get(0).getPrimaryKey());

        List<WideTableCdcEvent> updateEvents = filterByOp(events, UPDATE);
        assertEquals(1, updateEvents.size());
        assertEquals(789, updateEvents.get(0).getPrimaryKey());

        // 7. 验证 WITH CTE SQL 被生成
        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("VALUES")
        ));
    }

    @Test
    void testEndToEnd_ComplexJoinSql() throws SQLException {
        // 使用复杂 SQL（JOIN）
        String complexSql = "SELECT u.id, u.name, o.order_id, o.amount " +
                "FROM users u INNER JOIN orders o ON u.id = o.user_id " +
                "WHERE u.status = 1";

        WideTableIncrementalUpdater complexUpdater = new WideTableIncrementalUpdater(
                "id", complexSql, Arrays.asList("id", "name"), sqlGenerator, mockDuckDbOperator);

        Set<Object> beforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> afterKeys = new LinkedHashSet<>(Collections.singletonList(456));

        List<Map<String, Object>> afterRows = Collections.singletonList(createRow(456, "John"));
        List<Map<String, Object>> queryResult = Collections.singletonList(createRow(456, "John"));
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> events = complexUpdater.updateWideTable(
                beforeKeys, afterKeys, afterRows, "users");

        assertEquals(2, events.size());

        // 验证 WITH CTE SQL 包含复杂 JOIN
        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("INNER JOIN")
        ));
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createUpdateEvent(String pkField, Object beforePk, Object afterPk) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, beforePk);
        event.put("before", before);
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, afterPk);
        event.put("after", after);
        return event;
    }

    private Map<String, Object> createRow(Object id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    private List<WideTableCdcEvent> filterByOp(List<WideTableCdcEvent> events, WideTableCdcEvent.OpType opType) {
        List<WideTableCdcEvent> filtered = new ArrayList<>();
        for (WideTableCdcEvent event : events) {
            if (event.getOpType() == opType) {
                filtered.add(event);
            }
        }
        return filtered;
    }
}
```

- [ ] **Step 2: Run integration test**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WithCteIntegrationTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java
git commit -m "test: add end-to-end integration test for WITH CTE flow"
```

---

### Task 4: Run All Tests and Final Cleanup

**Files:**
- No file changes expected

- [ ] **Step 1: Run all duckdb tests**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest="AffectedKeyCalculatorTest,SchemaResolverTest,WideTableIncrementalUpdaterTest,BatchWideTableUpdateIntegrationTest,WithCteSqlGeneratorTest,WithCteIntegrationTest" -pl iengine/iengine-app --fail-at-end 2>&1 | grep -E "(Tests run|BUILD|FAILURE)" | tail -5`
Expected: All tests PASS (90+ tests)

- [ ] **Step 2: Verify no regression**

Run: `cd /Users/hj/workspace/tapdata && git status`
Expected: Clean working tree (all changes committed)

- [ ] **Step 3: Final commit if needed**

```bash
cd /Users/hj/workspace/tapdata
git log --oneline -5
```

---

## Self-Review

**1. Spec coverage:**
- WITH CTE SQL generation ✓ (Task 1)
- Integration with WideTableIncrementalUpdater ✓ (Task 2)
- End-to-end flow test ✓ (Task 3)
- Complex SQL support ✓ (Task 3, testEndToEnd_ComplexJoinSql)
- Backward compatibility ✓ (Task 2, deprecated constructor)

**2. Placeholder scan:** No TBD/TODO found. All steps contain actual code.

**3. Type consistency:**
- `WideTableCdcEvent.OpType` used consistently
- `WithCteSqlGenerator` injected via constructor
- Method signatures match across tasks
- `List<Map<String, Object>>` for rows, `Set<Object>` for keys

**4. Scope check:** Focused on WITH CTE SQL generation and integration. No unrelated refactoring.
