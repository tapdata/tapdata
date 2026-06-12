# Merge Core Features from IncrementalViewUpdater to WideTableIncrementalUpdater

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将事务支持和无变化跳过两个核心功能合并到 WideTableIncrementalUpdater，提升数据一致性和性能。

**Architecture:** 在 `updateWideTableAsTapdataEvents` 方法中增加事务包裹和旧值对比逻辑，在 FourStateJudge 生成事件前读取宽表旧值，跳过未变化行。

**Tech Stack:** Java, DuckDB, Mockito, JUnit 5

---

## File Structure

| 文件 | 操作 | 说明 |
|------|------|------|
| `WideTableIncrementalUpdater.java` | 修改 | 新增事务支持、旧值读取、无变化跳过 |
| `WideTableIncrementalUpdaterTest.java` | 修改 | 新增事务和无变化跳过测试用例 |

---

### Task 1: 新增事务支持和无变化跳过到 WideTableIncrementalUpdater

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

- [ ] **Step 1: 新增 readOldValues 方法**

```java
/**
 * 从宽表读取当前值（用于无变化跳过）
 */
private Map<Object, Map<String, Object>> readOldValues(Set<Object> affectedKeys) throws SQLException {
    if (affectedKeys == null || affectedKeys.isEmpty()) {
        return Collections.emptyMap();
    }

    String inClause = buildInClause(affectedKeys);
    String query = String.format(
            "SELECT * FROM wide_table WHERE %s IN %s",
            wideTablePrimaryKey,
            inClause
    );

    logger.debug("Reading old values: {}", query);
    List<Map<String, Object>> results = duckDbOperator.executeQuery(query);

    Map<Object, Map<String, Object>> oldValues = new LinkedHashMap<>();
    for (Map<String, Object> row : results) {
        Object pk = row.get(wideTablePrimaryKey);
        if (pk != null) {
            oldValues.put(pk, row);
        }
    }
    return oldValues;
}
```

- [ ] **Step 2: 新增 areRowsEqual 方法**

```java
/**
 * 检查两行数据是否相等（深度对比）
 */
private boolean areRowsEqual(Map<String, Object> row1, Map<String, Object> row2) {
    if (row1 == row2) return true;
    if (row1 == null || row2 == null) return false;
    if (row1.size() != row2.size()) return false;

    for (Map.Entry<String, Object> entry : row1.entrySet()) {
        String key = entry.getKey();
        Object val1 = entry.getValue();
        Object val2 = row2.get(key);

        if (!Objects.equals(val1, val2)) {
            return false;
        }
    }

    return true;
}
```

- [ ] **Step 3: 修改 updateWideTableAsTapdataEvents 方法，增加事务和无变化跳过**

```java
/**
 * 批量更新宽表（使用 FourStateJudge 输出 TapdataEvent）
 * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
 * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
 * @param afterRows after 数据行（从 CDC 事件提取）
 * @param tableName 源表名（用于 WITH CTE 临时表名）
 * @return TapdataEvent 事件列表
 */
public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                          Set<Object> affectedAfterKeys,
                                                          List<Map<String, Object>> afterRows,
                                                          String tableName) throws SQLException {
    final List<TapdataEvent>[] resultEvents = new List[]{Collections.emptyList()};

    duckDbOperator.executeInTransaction(() -> {
        // 1. 使用 WITH CTE 执行 after 查询
        List<Map<String, Object>> results = Collections.emptyList();
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 2. 读取宽表旧值（用于无变化跳过）
        Map<Object, Map<String, Object>> oldValues = readOldValues(affectedAfterKeys);

        // 3. 过滤掉未变化的行
        List<Map<String, Object>> changedResults = new ArrayList<>();
        for (Map<String, Object> row : results) {
            Object pk = row.get(wideTablePrimaryKey);
            Map<String, Object> oldValue = oldValues.get(pk);
            if (oldValue == null || !areRowsEqual(oldValue, row)) {
                changedResults.add(row);
            }
        }

        // 4. 使用 FourStateJudge 进行四态判断（传入完整的 affectedBeforeKeys）
        resultEvents[0] = fourStateJudge.judge(affectedBeforeKeys, changedResults);
    });

    return resultEvents[0];
}
```

- [ ] **Step 4: 添加必要的 import**

```java
import java.util.Objects;
```

---

### Task 2: 编写测试用例

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`

- [ ] **Step 1: 新增测试 - 事务回滚**

```java
@Test
void testUpdateWideTableAsTapdataEvents_TransactionRollbackOnError() throws Exception {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(1));
    Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(2));
    List<Map<String, Object>> afterRows = Arrays.asList(createRow(2, "John", "john@example.com"));

    when(mockDuckDbOperator.executeQuery(anyString()))
            .thenThrow(new SQLException("Query failed"));

    doAnswer(invocation -> {
        DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
        action.accept();
        return null;
    }).when(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

    assertThrows(SQLException.class, () -> 
            updater.updateWideTableAsTapdataEvents(affectedBeforeKeys, affectedAfterKeys, afterRows, "users"));
}
```

- [ ] **Step 2: 新增测试 - 无变化跳过**

```java
@Test
void testUpdateWideTableAsTapdataEvents_SkipUnchangedRows() throws Exception {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(1));
    List<Map<String, Object>> afterRows = Arrays.asList(createRow(1, "John", "john@example.com"));

    // 模拟宽表旧值与新值相同
    Map<String, Object> unchangedRow = createRow(1, "John", "john@example.com");
    when(mockDuckDbOperator.executeQuery(contains("SELECT * FROM wide_table")))
            .thenReturn(Arrays.asList(unchangedRow));
    when(mockDuckDbOperator.executeQuery(contains("WITH")))
            .thenReturn(Arrays.asList(unchangedRow));

    List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(
            affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

    // 无变化行不应生成事件
    assertEquals(0, events.size());
}
```

- [ ] **Step 3: 新增测试 - 有变化生成事件**

```java
@Test
void testUpdateWideTableAsTapdataEvents_GenerateEventsForChangedRows() throws Exception {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(1));
    Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(1));
    List<Map<String, Object>> afterRows = Arrays.asList(createRow(1, "John Updated", "john@example.com"));

    // 模拟宽表旧值
    Map<String, Object> oldRow = createRow(1, "John", "john@example.com");
    when(mockDuckDbOperator.executeQuery(contains("SELECT * FROM wide_table")))
            .thenReturn(Arrays.asList(oldRow));
    // 模拟宽表新值
    Map<String, Object> newRow = createRow(1, "John Updated", "john@example.com");
    when(mockDuckDbOperator.executeQuery(contains("WITH")))
            .thenReturn(Arrays.asList(newRow));

    List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(
            affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

    // 有变化行应生成 UPDATE 事件
    assertEquals(1, events.size());
    assertTrue(events.get(0).getTapEvent() instanceof TapUpdateRecordEvent);
}
```

---

### Task 3: 运行测试并提交

- [ ] **Step 1: 运行所有相关测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -Dtest="WideTableIncrementalUpdaterTest,FourStateJudgeTest,FourStateJudgeIntegrationTest" -pl iengine/iengine-app --fail-at-end
```

Expected: All tests pass

- [ ] **Step 2: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java
git commit -m "feat: add transaction support and skip unchanged rows to WideTableIncrementalUpdater"
```

---

## Self-Review

1. **Spec coverage:** 事务支持 ✓、无变化跳过 ✓、测试覆盖 ✓
2. **Placeholder scan:** 无 TBD/TODO
3. **Type consistency:** 使用现有 `TapdataEvent`、`TapUpdateRecordEvent`、`DuckDbOperator.ThrowingConsumer` 等类型，与现有代码一致
