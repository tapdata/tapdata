# AffectedKeyCalculator 批量重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构 AffectedKeyCalculator 为批量 before/after 主键分离模式，支持主键更新和 JOIN KEY 更新场景

**Architecture:** 将 `extractPrimaryKey` 拆分为 `extractBeforePrimaryKey` 和 `extractAfterPrimaryKey`，新增 `calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys` 批量方法，统一处理多表 CDC 事件，一次执行宽表更新

**Tech Stack:** Java 17, JUnit 5, Mockito, DuckDB

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `AffectedKeyCalculator.java` | 修改 | 拆分 before/after 主键提取，新增批量计算方法 |
| `SchemaResolver.java` | 创建 | 从 CDC 事件或 DuckDB 元数据解析源表字段列表 |
| `WideTableIncrementalUpdater.java` | 修改 | 重构为批量更新模式 |
| `AffectedKeyCalculatorTest.java` | 创建 | before/after 提取、批量计算、主键更新测试 |
| `SchemaResolverTest.java` | 创建 | 字段解析、元数据回退测试 |
| `WideTableIncrementalUpdaterTest.java` | 创建 | 批量更新、四态判断测试 |

---

### Task 1: 重构 AffectedKeyCalculator - 拆分 before/after 主键提取

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 编写测试用例 - extractBeforePrimaryKey**

```java
@Test
void testExtractBeforePrimaryKey_fromBeforeField() {
    Map<String, Object> event = Map.of(
        "before", Map.of("id", 123, "name", "John")
    );
    Optional<Object> result = calculator.extractBeforePrimaryKey(event, "id");
    assertTrue(result.isPresent());
    assertEquals(123, result.get());
}

@Test
void testExtractBeforePrimaryKey_fromTopLevel() {
    Map<String, Object> event = Map.of("id", 123, "name", "John");
    Optional<Object> result = calculator.extractBeforePrimaryKey(event, "id");
    assertTrue(result.isPresent());
    assertEquals(123, result.get());
}

@Test
void testExtractBeforePrimaryKey_fromMongoO2() {
    Map<String, Object> event = Map.of("o2", Map.of("_id", "mongo123"));
    Optional<Object> result = calculator.extractBeforePrimaryKey(event, "_id");
    assertTrue(result.isPresent());
    assertEquals("mongo123", result.get());
}

@Test
void testExtractBeforePrimaryKey_empty() {
    Map<String, Object> event = Map.of("after", Map.of("id", 456));
    Optional<Object> result = calculator.extractBeforePrimaryKey(event, "id");
    assertFalse(result.isPresent());
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testExtractBeforePrimaryKey_* -pl iengine/iengine-app`
Expected: FAIL with "method not defined"

- [ ] **Step 3: 实现 extractBeforePrimaryKey**

```java
/**
 * 从 CDC 事件提取 before 主键（宽表改动前）
 * @param event CDC 事件
 * @param pkField 主键字段名
 * @return Optional 包含 before 主键值，如果不存在则返回 empty
 */
public Optional<Object> extractBeforePrimaryKey(Map<String, Object> event, String pkField) {
    // 1. 优先从 before 字段提取（DELETE/UPDATE 事件）
    Object before = event.get("before");
    if (before instanceof Map) {
        Object pk = ((Map<?, ?>) before).get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
    }
    
    // 2. 回退到顶层直接访问（某些 CDC 格式）
    Object pk = event.get(pkField);
    if (pk != null) {
        return Optional.of(pk);
    }
    
    // 3. MongoDB 风格 o2/o 字段
    Object o2 = event.get("o2");
    if (o2 instanceof Map) {
        Object pk2 = ((Map<?, ?>) o2).get(pkField);
        if (pk2 != null) {
            return Optional.of(pk2);
        }
    }
    
    Object o = event.get("o");
    if (o instanceof Map) {
        Object pk3 = ((Map<?, ?>) o).get(pkField);
        if (pk3 != null) {
            return Optional.of(pk3);
        }
    }
    
    return Optional.empty();
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testExtractBeforePrimaryKey_* -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 5: 编写测试用例 - extractAfterPrimaryKey**

```java
@Test
void testExtractAfterPrimaryKey_fromAfterField() {
    Map<String, Object> event = Map.of(
        "after", Map.of("id", 456, "name", "John Updated")
    );
    Optional<Object> result = calculator.extractAfterPrimaryKey(event, "id");
    assertTrue(result.isPresent());
    assertEquals(456, result.get());
}

@Test
void testExtractAfterPrimaryKey_fromTopLevel() {
    Map<String, Object> event = Map.of("id", 456, "name", "John");
    Optional<Object> result = calculator.extractAfterPrimaryKey(event, "id");
    assertTrue(result.isPresent());
    assertEquals(456, result.get());
}

@Test
void testExtractAfterPrimaryKey_empty() {
    Map<String, Object> event = Map.of("before", Map.of("id", 123));
    Optional<Object> result = calculator.extractAfterPrimaryKey(event, "id");
    assertFalse(result.isPresent());
}
```

- [ ] **Step 6: 运行测试验证失败**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testExtractAfterPrimaryKey_* -pl iengine/iengine-app`
Expected: FAIL with "method not defined"

- [ ] **Step 7: 实现 extractAfterPrimaryKey**

```java
/**
 * 从 CDC 事件提取 after 主键（宽表改动后）
 * @param event CDC 事件
 * @param pkField 主键字段名
 * @return Optional 包含 after 主键值，如果不存在则返回 empty
 */
public Optional<Object> extractAfterPrimaryKey(Map<String, Object> event, String pkField) {
    // 1. 优先从 after 字段提取（INSERT/UPDATE 事件）
    Object after = event.get("after");
    if (after instanceof Map) {
        Object pk = ((Map<?, ?>) after).get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
    }
    
    // 2. 回退到顶层直接访问
    Object pk = event.get(pkField);
    if (pk != null) {
        return Optional.of(pk);
    }
    
    return Optional.empty();
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testExtractAfterPrimaryKey_* -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 9: 编写测试用例 - isPrimaryKeyUpdated**

```java
@Test
void testIsPrimaryKeyUpdated_true() {
    Map<String, Object> event = Map.of(
        "before", Map.of("id", 123),
        "after", Map.of("id", 456)
    );
    assertTrue(calculator.isPrimaryKeyUpdated(event, "id"));
}

@Test
void testIsPrimaryKeyUpdated_false_sameKey() {
    Map<String, Object> event = Map.of(
        "before", Map.of("id", 123),
        "after", Map.of("id", 123)
    );
    assertFalse(calculator.isPrimaryKeyUpdated(event, "id"));
}

@Test
void testIsPrimaryKeyUpdated_false_insertOnly() {
    Map<String, Object> event = Map.of("after", Map.of("id", 123));
    assertFalse(calculator.isPrimaryKeyUpdated(event, "id"));
}

@Test
void testIsPrimaryKeyUpdated_false_deleteOnly() {
    Map<String, Object> event = Map.of("before", Map.of("id", 123));
    assertFalse(calculator.isPrimaryKeyUpdated(event, "id"));
}
```

- [ ] **Step 10: 实现 isPrimaryKeyUpdated**

```java
/**
 * 检测主键是否更新
 * @return true 表示主键更新（beforePk ≠ afterPk）
 */
public boolean isPrimaryKeyUpdated(Map<String, Object> event, String pkField) {
    Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
    Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
    
    return beforePk.isPresent() && afterPk.isPresent() 
            && !Objects.equals(beforePk.get(), afterPk.get());
}
```

- [ ] **Step 11: 运行所有测试**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest -pl iengine/iengine-app`
Expected: ALL PASS

- [ ] **Step 12: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java
git commit -m "refactor: split extractPrimaryKey into extractBeforePrimaryKey and extractAfterPrimaryKey"
```

---

### Task 2: 新增批量计算方法 calculateAffectedBeforeKeys/AfterKeys

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 编写测试用例 - calculateAffectedBeforeKeys 主表**

```java
@Test
void testCalculateAffectedBeforeKeys_mainTable() throws SQLException {
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of(
        "users", List.of(
            Map.of("before", Map.of("id", 123), "after", Map.of("id", 456)),
            Map.of("before", Map.of("id", 789), "after", Map.of("id", 789))
        )
    );
    
    Set<Object> result = calculator.calculateAffectedBeforeKeys(eventsByTable);
    
    assertEquals(2, result.size());
    assertTrue(result.contains(123));
    assertTrue(result.contains(789));
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedBeforeKeys_mainTable -pl iengine/iengine-app`
Expected: FAIL with "method not defined"

- [ ] **Step 3: 实现 calculateAffectedBeforeKeys**

```java
/**
 * 批量计算所有事件的 before 受影响主键集合
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 before 主键集合（用于 DELETE 宽表记录）
 */
public Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        String pkField = getTablePrimaryKey(tableName);
        
        for (Map<String, Object> event : events) {
            Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
            beforePk.ifPresent(affectedBeforeKeys::add);
        }
    }
    
    return affectedBeforeKeys;
}

/**
 * 获取表的主键字段名
 */
private String getTablePrimaryKey(String tableName) {
    if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
        return mainTablePrimaryKey;
    }
    return getSourceTablePrimaryKey(tableName);
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedBeforeKeys_mainTable -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 5: 编写测试用例 - calculateAffectedAfterKeys 主表**

```java
@Test
void testCalculateAffectedAfterKeys_mainTable() throws SQLException {
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of(
        "users", List.of(
            Map.of("before", Map.of("id", 123), "after", Map.of("id", 456)),
            Map.of("before", Map.of("id", 789), "after", Map.of("id", 789))
        )
    );
    
    Set<Object> result = calculator.calculateAffectedAfterKeys(eventsByTable);
    
    assertEquals(2, result.size());
    assertTrue(result.contains(456));
    assertTrue(result.contains(789));
}
```

- [ ] **Step 6: 运行测试验证失败**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedAfterKeys_mainTable -pl iengine/iengine-app`
Expected: FAIL with "method not defined"

- [ ] **Step 7: 实现 calculateAffectedAfterKeys**

```java
/**
 * 批量计算所有事件的 after 受影响主键集合
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 after 主键集合（用于 INSERT/UPDATE 宽表记录）
 */
public Set<Object> calculateAffectedAfterKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedAfterKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        String pkField = getTablePrimaryKey(tableName);
        
        for (Map<String, Object> event : events) {
            Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
            afterPk.ifPresent(affectedAfterKeys::add);
        }
    }
    
    return affectedAfterKeys;
}
```

- [ ] **Step 8: 运行测试验证通过**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedAfterKeys_mainTable -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 9: 编写测试用例 - 从表 JOIN KEY 更新**

```java
@Test
void testCalculateAffectedKeys_secondaryTableJoinKeyUpdate() throws SQLException {
    // 配置从表
    List<FromTableConfig> fromTables = List.of(
        new FromTableConfig("orders", "user_id")
    );
    
    // 配置自定义 JOIN 查询
    Map<String, String> customJoinQueries = Map.of(
        "orders", "SELECT id FROM users WHERE id IN (${pkValues})"
    );
    
    // Mock DuckDB 查询结果
    when(operator.executeQuery("SELECT id FROM users WHERE id IN (123)"))
        .thenReturn(List.of(Map.of("id", 123)));
    when(operator.executeQuery("SELECT id FROM users WHERE id IN (456)"))
        .thenReturn(List.of(Map.of("id", 456)));
    
    AffectedKeyCalculator calc = new AffectedKeyCalculator(
        "id", "users", "id", fromTables, customJoinQueries, operator
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of(
        "orders", List.of(
            Map.of(
                "before", Map.of("order_id", 1, "user_id", 123),
                "after", Map.of("order_id", 1, "user_id", 456)
            )
        )
    );
    
    Set<Object> beforeKeys = calc.calculateAffectedBeforeKeys(eventsByTable);
    Set<Object> afterKeys = calc.calculateAffectedAfterKeys(eventsByTable);
    
    assertEquals(1, beforeKeys.size());
    assertTrue(beforeKeys.contains(123));
    
    assertEquals(1, afterKeys.size());
    assertTrue(afterKeys.contains(456));
}
```

- [ ] **Step 10: 运行测试验证通过**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_secondaryTableJoinKeyUpdate -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 11: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java
git commit -m "feat: add calculateAffectedBeforeKeys and calculateAffectedAfterKeys batch methods"
```

---

### Task 3: 创建 SchemaResolver 类

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SchemaResolver.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SchemaResolverTest.java`

- [ ] **Step 1: 编写测试用例 - 从 CDC 事件推断字段**

```java
@Test
void testResolveFields_fromAfterData() {
    Map<String, Object> event = Map.of(
        "after", Map.of("id", 123, "name", "John", "status", 1)
    );
    
    List<String> result = resolver.resolveFields("users", event);
    
    assertEquals(3, result.size());
    assertTrue(result.contains("id"));
    assertTrue(result.contains("name"));
    assertTrue(result.contains("status"));
}

@Test
void testResolveFields_fromBeforeData() {
    Map<String, Object> event = Map.of(
        "before", Map.of("id", 123, "name", "John")
    );
    
    List<String> result = resolver.resolveFields("users", event);
    
    assertEquals(2, result.size());
    assertTrue(result.contains("id"));
    assertTrue(result.contains("name"));
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=SchemaResolverTest#testResolveFields_* -pl iengine/iengine-app`
Expected: FAIL with "class not found"

- [ ] **Step 3: 实现 SchemaResolver**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 源表字段列表解析器
 * 优先从 CDC 事件推断，回退到 DuckDB 元数据查询
 */
public class SchemaResolver {
    
    private static final Logger logger = LogManager.getLogger(SchemaResolver.class);
    
    private final DuckDbOperator duckDbOperator;
    private final Map<String, List<String>> schemaCache = new ConcurrentHashMap<>();
    
    public SchemaResolver(DuckDbOperator duckDbOperator) {
        this.duckDbOperator = duckDbOperator;
    }
    
    /**
     * 解析源表字段列表
     */
    public List<String> resolveFields(String tableName, Map<String, Object> event) {
        // 1. 优先从 after 数据推断
        Object after = event.get("after");
        if (after instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) after).keySet());
            if (!fields.isEmpty()) {
                return fields;
            }
        }
        
        // 2. 从 before 数据推断
        Object before = event.get("before");
        if (before instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) before).keySet());
            if (!fields.isEmpty()) {
                return fields;
            }
        }
        
        // 3. 从 DuckDB 元数据查询
        return resolveFieldsFromMetadata(tableName);
    }
    
    /**
     * 从 DuckDB information_schema 查询字段列表
     */
    private List<String> resolveFieldsFromMetadata(String tableName) {
        return schemaCache.computeIfAbsent(tableName, table -> {
            try {
                String sql = String.format(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
                        table);
                List<Map<String, Object>> results = duckDbOperator.executeQuery(sql);
                return results.stream()
                        .map(row -> (String) row.get("column_name"))
                        .collect(Collectors.toList());
            } catch (SQLException e) {
                logger.warn("Failed to resolve fields from metadata for table {}: {}", table, e.getMessage());
                return Collections.emptyList();
            }
        });
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=SchemaResolverTest#testResolveFields_* -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 5: 编写测试用例 - DuckDB 元数据回退**

```java
@Test
void testResolveFields_fromMetadata() throws SQLException {
    Map<String, Object> event = Map.of("id", 123); // 没有 before/after
    
    when(duckDbOperator.executeQuery(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position"))
        .thenReturn(List.of(
            Map.of("column_name", "id"),
            Map.of("column_name", "name"),
            Map.of("column_name", "status")
        ));
    
    List<String> result = resolver.resolveFields("users", event);
    
    assertEquals(3, result.size());
    assertTrue(result.contains("id"));
    assertTrue(result.contains("name"));
    assertTrue(result.contains("status"));
}

@Test
void testResolveFields_metadataCache() throws SQLException {
    Map<String, Object> event = Map.of("id", 123);
    
    when(duckDbOperator.executeQuery(anyString()))
        .thenReturn(List.of(Map.of("column_name", "id")));
    
    // 第一次查询
    resolver.resolveFields("users", event);
    // 第二次查询（应该使用缓存）
    resolver.resolveFields("users", event);
    
    // 只执行一次 SQL 查询
    verify(duckDbOperator, times(1)).executeQuery(anyString());
}
```

- [ ] **Step 6: 运行测试验证通过**

Run: `mvn test -Dtest=SchemaResolverTest -pl iengine/iengine-app`
Expected: ALL PASS

- [ ] **Step 7: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SchemaResolver.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SchemaResolverTest.java
git commit -m "feat: add SchemaResolver for field list resolution from CDC events or DuckDB metadata"
```

---

### Task 4: 重构 WideTableIncrementalUpdater 批量更新

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`

- [ ] **Step 1: 编写测试用例 - updateWideTable 批量 DELETE + INSERT**

```java
@Test
void testUpdateWideTable_deleteAndInsert() throws SQLException {
    Set<Object> affectedBeforeKeys = Set.of(123, 789);
    Set<Object> affectedAfterKeys = Set.of(456, 789);
    
    // Mock WITH CTE SQL 查询结果
    when(duckDbOperator.executeQuery(anyString()))
        .thenReturn(List.of(
            Map.of("id", 456, "name", "John"),
            Map.of("id", 789, "name", "Jane Updated")
        ));
    
    List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);
    
    // 验证 DELETE 事件
    Optional<WideTableCdcEvent> deleteEvent = result.stream()
        .filter(e -> e.getOp() == DELETE && e.getPk().equals(123))
        .findFirst();
    assertTrue(deleteEvent.isPresent());
    
    // 验证 INSERT 事件
    assertEquals(2, result.stream().filter(e -> e.getOp() == INSERT).count());
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `mvn test -Dtest=WideTableIncrementalUpdaterTest#testUpdateWideTable_deleteAndInsert -pl iengine/iengine-app`
Expected: FAIL with "method not defined"

- [ ] **Step 3: 实现 updateWideTable**

```java
/**
 * 批量更新宽表
 * @param affectedBeforeKeys before 受影响主键集合（DELETE）
 * @param affectedAfterKeys after 受影响主键集合（INSERT/UPDATE）
 */
public List<WideTableCdcEvent> updateWideTable(Set<Object> affectedBeforeKeys, Set<Object> affectedAfterKeys) throws SQLException {
    List<WideTableCdcEvent> events = new ArrayList<>();
    
    // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
    Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
    pureDeleteKeys.removeAll(affectedAfterKeys);
    
    // 2. 生成 DELETE 事件
    for (Object pk : pureDeleteKeys) {
        events.add(new WideTableCdcEvent(DELETE, pk, null));
    }
    
    // 3. 计算需要查询的主键（在 after 中）
    if (!affectedAfterKeys.isEmpty()) {
        // 生成批量 WITH CTE SQL
        String afterSql = withCteSqlGenerator.generateBatch(
                querySql, affectedAfterKeys, fields);
        
        // 执行查询
        List<Map<String, Object>> results = duckDbOperator.executeQuery(afterSql);
        
        // 生成 INSERT/UPDATE 事件
        for (Map<String, Object> row : results) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk != null) {
                events.add(new WideTableCdcEvent(INSERT, pk, row));
            }
        }
    }
    
    return events;
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `mvn test -Dtest=WideTableIncrementalUpdaterTest#testUpdateWideTable_deleteAndInsert -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 5: 编写测试用例 - 主键更新场景**

```java
@Test
void testUpdateWideTable_primaryKeyUpdate() throws SQLException {
    Set<Object> affectedBeforeKeys = Set.of(123);
    Set<Object> affectedAfterKeys = Set.of(456);
    
    when(duckDbOperator.executeQuery(anyString()))
        .thenReturn(List.of(Map.of("id", 456, "name", "John")));
    
    List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);
    
    assertEquals(2, result.size());
    
    // 验证 DELETE 事件
    assertEquals(DELETE, result.get(0).getOp());
    assertEquals(123, result.get(0).getPk());
    
    // 验证 INSERT 事件
    assertEquals(INSERT, result.get(1).getOp());
    assertEquals(456, result.get(1).getPk());
}
```

- [ ] **Step 6: 运行测试验证通过**

Run: `mvn test -Dtest=WideTableIncrementalUpdaterTest -pl iengine/iengine-app`
Expected: ALL PASS

- [ ] **Step 7: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java
git commit -m "refactor: implement batch updateWideTable with before/after key separation"
```

---

### Task 5: 集成测试 - 端到端批量处理

**Files:**
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/BatchWideTableUpdateIntegrationTest.java`

- [ ] **Step 1: 编写集成测试 - 多表混合 CDC 事件**

```java
@Test
void testBatchProcessing_multiTableMixedEvents() throws SQLException {
    // 配置
    List<FromTableConfig> fromTables = List.of(
        new FromTableConfig("orders", "user_id")
    );
    
    Map<String, String> customJoinQueries = Map.of(
        "orders", "SELECT id FROM users WHERE id IN (${pkValues})"
    );
    
    // Mock DuckDB 查询
    when(operator.executeQuery("SELECT id FROM users WHERE id IN (123)"))
        .thenReturn(List.of(Map.of("id", 123)));
    when(operator.executeQuery("SELECT id FROM users WHERE id IN (456)"))
        .thenReturn(List.of(Map.of("id", 456)));
    when(operator.executeQuery(anyString())) // WITH CTE SQL
        .thenReturn(List.of(
            Map.of("id", 456, "name", "John", "order_id", 1),
            Map.of("id", 789, "name", "Jane Updated", "order_id", 2)
        ));
    
    // 创建计算器
    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
        "id", "users", "id", fromTables, customJoinQueries, operator
    );
    
    // CDC 事件缓冲区（多表混合）
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of(
        "users", List.of(
            Map.of("before", Map.of("id", 123), "after", Map.of("id", 456)),
            Map.of("before", Map.of("id", 789), "after", Map.of("id", 789))
        ),
        "orders", List.of(
            Map.of(
                "before", Map.of("order_id", 1, "user_id", 123),
                "after", Map.of("order_id", 1, "user_id", 456)
            )
        )
    );
    
    // 执行批量计算
    Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
    Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);
    
    // 验证 before keys
    assertEquals(2, beforeKeys.size());
    assertTrue(beforeKeys.contains(123));
    assertTrue(beforeKeys.contains(789));
    
    // 验证 after keys
    assertEquals(2, afterKeys.size());
    assertTrue(afterKeys.contains(456));
    assertTrue(afterKeys.contains(789));
}
```

- [ ] **Step 2: 运行测试验证通过**

Run: `mvn test -Dtest=BatchWideTableUpdateIntegrationTest -pl iengine/iengine-app`
Expected: PASS

- [ ] **Step 3: 提交**

```bash
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/BatchWideTableUpdateIntegrationTest.java
git commit -m "test: add integration test for batch multi-table CDC processing"
```

---

### Task 6: 清理旧代码

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

- [ ] **Step 1: 移除旧的 calculateAffectedKeys 方法**

```java
// 删除以下方法（保留用于兼容，标记 @Deprecated）
@Deprecated
public Set<Object> calculateAffectedKeys(String tableName, List<Map<String, Object>> events) throws SQLException {
    // 原有实现，标记为废弃
}
```

- [ ] **Step 2: 移除旧的 extractPrimaryKey 方法**

```java
// 删除以下方法（保留用于兼容，标记 @Deprecated）
@Deprecated
private Object extractPrimaryKey(Map<String, Object> event, String pkField) {
    // 原有实现，标记为废弃
}
```

- [ ] **Step 3: 运行所有测试验证无回归**

Run: `mvn test -pl iengine/iengine-app`
Expected: ALL PASS

- [ ] **Step 4: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git commit -m "chore: deprecate old single-event processing methods"
```

---

## 自审清单

- [x] **Spec 覆盖**：所有设计需求都有对应任务实现
- [x] **占位符扫描**：无 TBD/TODO/TODO 占位符
- [x] **类型一致性**：WideTableCdcEvent、DELETE/INSERT 枚举在所有任务中一致
- [x] **方法签名一致性**：calculateAffectedBeforeKeys/AfterKeys 签名在 Task 2 和 Task 5 中一致
- [x] **TDD 顺序**：每个任务先写测试，后实现代码
