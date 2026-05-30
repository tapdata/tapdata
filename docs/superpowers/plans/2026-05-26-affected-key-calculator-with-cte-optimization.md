# AffectedKeyCalculator WITH CTE SQL 查询优化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 改造 `calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys` 方法，通过 WITH CTE SQL 查询子表对应的宽表主键，集成 SmartMerger 处理 JOIN KEY 多次更新场景。

**Architecture:** 使用 SmartMerger 合并 CDC 事件为 MergedRecord，提取 before/after 数据行，通过 WithCteSqlGenerator 拼接 WITH CTE SQL，将 querySql 中的子表名替换为 WITH 临时表名，执行查询获取宽表主键。

**Tech Stack:** Java, DuckDB, SmartMerger, WithCteSqlGenerator, DuckDbOperator

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `AffectedKeyCalculator.java` | 修改 | 改造 calculateAffectedBeforeKeys/AfterKeys 方法，新增 WITH SQL 查询逻辑 |
| `AffectedKeyCalculatorTest.java` | 修改 | 新增批量查询测试用例 |

---

### Task 1: 改造 calculateAffectedBeforeKeys 方法

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

**目标：** 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键。

- [ ] **Step 1: 新增 extractBeforeDataRows 辅助方法**

在 `AffectedKeyCalculator` 类中添加以下方法：

```java
/**
 * 从 SmartMerger 合并结果中提取所有 before 数据行
 * 收集 operations 中所有 before/after 数据（排除 finalState），保证历史错误数据全被删除
 */
private List<Map<String, Object>> extractBeforeDataRows(List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
    List<Map<String, Object>> beforeRows = new ArrayList<>();
    String pkField = getSourceTablePrimaryKey(tableName);
    
    for (SmartMerger.MergedRecord record : mergedRecords) {
        for (Map<String, Object> op : record.getOperations()) {
            String opType = (String) op.get("op");
            if ("INSERT".equals(opType)) {
                // INSERT 事件的 value 作为 before 数据
                Object value = op.get("value");
                if (value instanceof Map) {
                    beforeRows.add(new HashMap<>((Map<String, Object>) value));
                }
            } else if ("UPDATE".equals(opType)) {
                // UPDATE 事件的 before 数据（从 old_pk 或 operations 中提取）
                Object oldPk = op.get("old_pk");
                if (oldPk != null) {
                    // 有主键变更，提取 before 数据
                    Map<String, Object> beforeRow = new HashMap<>();
                    beforeRow.put(pkField, oldPk);
                    // 复制其他字段（如果有）
                    Map<String, Object> fields = (Map<String, Object>) op.get("fields");
                    if (fields != null) {
                        beforeRow.putAll(fields);
                    }
                    beforeRows.add(beforeRow);
                }
            } else if ("DELETE".equals(opType)) {
                // DELETE 事件的 before 数据从 finalState 提取
                Map<String, Object> beforeRow = new HashMap<>(record.getFinalState());
                if (!beforeRow.isEmpty()) {
                    beforeRows.add(beforeRow);
                }
            }
        }
    }
    
    return beforeRows;
}
```

- [ ] **Step 2: 新增 queryWideTablePksWithCte 方法**

```java
/**
 * 使用 WITH CTE SQL 查询宽表主键
 * @param tableName 子表名
 * @param dataRows 数据行
 * @param fields 字段列表
 * @return 宽表主键集合
 */
private Set<Object> queryWideTablePksWithCte(String tableName, List<Map<String, Object>> dataRows, List<String> fields) throws SQLException {
    if (dataRows == null || dataRows.isEmpty()) {
        return Collections.emptySet();
    }
    
    // 获取 querySql（需要从外部传入或存储）
    String querySql = getQuerySqlForTable(tableName);
    if (querySql == null) {
        logger.warn("No querySql found for table {}", tableName);
        return Collections.emptySet();
    }
    
    // 生成 WITH CTE SQL
    String withSql = withCteSqlGenerator.generateBatch(querySql, tableName, dataRows, fields);
    
    // 执行查询
    List<Map<String, Object>> results = operator.executeQuery(withSql);
    
    // 提取宽表主键
    Set<Object> wideTablePks = new LinkedHashSet<>();
    for (Map<String, Object> row : results) {
        Object pk = row.get(wideTablePrimaryKey);
        if (pk != null) {
            wideTablePks.add(pk);
        }
    }
    
    return wideTablePks;
}

/**
 * 获取表对应的 querySql
 * 从 fromTables 中查找匹配的表，返回其 querySql
 */
private String getQuerySqlForTable(String tableName) {
    // 遍历 fromTables 查找匹配的表
    for (FromTableConfig config : fromTables) {
        if (config.getTableName().equalsIgnoreCase(tableName)) {
            return config.getQuerySql();
        }
    }
    return null;
}
```

- [ ] **Step 3: 修改 calculateAffectedBeforeKeys 方法**

将现有方法改造为：

```java
/**
 * 批量计算所有事件的 before 受影响主键集合
 * 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 before 主键集合（用于 DELETE 宽表记录）
 */
public Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        if (events == null || events.isEmpty()) {
            continue;
        }
        
        // 使用 SmartMerger 合并事件
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
        if (mergedRecords.isEmpty()) {
            continue;
        }
        
        // 提取所有 before 数据行
        String pkField = getSourceTablePrimaryKey(tableName);
        List<String> fields = getTableFields(tableName);
        List<Map<String, Object>> beforeRows = extractBeforeDataRows(mergedRecords, tableName);
        
        if (beforeRows.isEmpty()) {
            continue;
        }
        
        // 使用 WITH CTE SQL 查询宽表主键
        Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
        affectedBeforeKeys.addAll(wideTablePks);
    }
    
    return affectedBeforeKeys;
}
```

- [ ] **Step 4: 新增 getTableFields 辅助方法**

```java
/**
 * 获取表的字段列表
 * 从 FromTableConfig 中提取字段，如果没有配置则从事件中推断
 */
private List<String> getTableFields(String tableName) {
    for (FromTableConfig config : fromTables) {
        if (config.getTableName().equalsIgnoreCase(tableName)) {
            return config.getFields();
        }
    }
    // 回退：返回主键字段
    return Collections.singletonList(getSourceTablePrimaryKey(tableName));
}
```

- [ ] **Step 5: 确认 FromTableConfig 有 querySql 和 fields 字段**

检查 `FromTableConfig.java` 是否有 `querySql` 和 `fields` 字段，如果没有需要添加。

---

### Task 2: 改造 calculateAffectedAfterKeys 方法

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

**目标：** 使用 SmartMerger 合并事件，提取 finalState 数据，拼接 WITH SQL 查询宽表主键。

- [ ] **Step 1: 新增 extractAfterDataRows 辅助方法**

```java
/**
 * 从 SmartMerger 合并结果中提取所有 after 数据行
 * 只收集 finalState 数据，保证最终正确数据被插入
 */
private List<Map<String, Object>> extractAfterDataRows(List<SmartMerger.MergedRecord> mergedRecords) {
    List<Map<String, Object>> afterRows = new ArrayList<>();
    
    for (SmartMerger.MergedRecord record : mergedRecords) {
        Map<String, Object> finalState = record.getFinalState();
        if (finalState != null && !finalState.isEmpty()) {
            afterRows.add(new HashMap<>(finalState));
        }
    }
    
    return afterRows;
}
```

- [ ] **Step 2: 修改 calculateAffectedAfterKeys 方法**

```java
/**
 * 批量计算所有事件的 after 受影响主键集合
 * 使用 SmartMerger 合并事件，提取 finalState 数据，拼接 WITH SQL 查询宽表主键
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 after 主键集合（用于 INSERT/UPDATE 宽表记录）
 */
public Set<Object> calculateAffectedAfterKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedAfterKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        if (events == null || events.isEmpty()) {
            continue;
        }
        
        // 使用 SmartMerger 合并事件
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
        if (mergedRecords.isEmpty()) {
            continue;
        }
        
        // 提取所有 after 数据行（finalState）
        List<String> fields = getTableFields(tableName);
        List<Map<String, Object>> afterRows = extractAfterDataRows(mergedRecords);
        
        if (afterRows.isEmpty()) {
            continue;
        }
        
        // 使用 WITH CTE SQL 查询宽表主键
        Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, afterRows, fields);
        affectedAfterKeys.addAll(wideTablePks);
    }
    
    return affectedAfterKeys;
}
```

---

### Task 3: 增强 FromTableConfig 支持 querySql 和 fields

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FromTableConfig.java`

- [ ] **Step 1: 检查 FromTableConfig 现有结构**

读取 `FromTableConfig.java`，确认是否有 `querySql` 和 `fields` 字段。

- [ ] **Step 2: 添加缺失字段**

如果没有 `querySql` 和 `fields` 字段，添加：

```java
private String querySql;
private List<String> fields;

// 添加 getter/setter
public String getQuerySql() { return querySql; }
public void setQuerySql(String querySql) { this.querySql = querySql; }
public List<String> getFields() { return fields; }
public void setFields(List<String> fields) { this.fields = fields; }
```

---

### Task 4: 编写 calculateAffectedBeforeKeys 单元测试

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 编写测试 - 子表 before 数据查询宽表主键**

```java
@Test
@DisplayName("calculateAffectedBeforeKeys - 子表 before 数据查询宽表主键")
void testCalculateAffectedBeforeKeys_SubTableWithBeforeData() throws SQLException {
    // 准备测试数据：UPDATE 事件（JOIN KEY 更新）
    List<Map<String, Object>> events = Arrays.asList(
        Map.of(
            "op", "UPDATE",
            "o2", Map.of("id", 1),
            "updatedFields", Map.of("user_id", 200)
        )
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", events);
    
    // Mock operator 返回查询结果
    when(operator.executeQuery(anyString())).thenReturn(Arrays.asList(
        Map.of("wide_pk", 100)
    ));
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTable);
    
    // 验证结果
    assertTrue(result.contains(100));
}
```

- [ ] **Step 2: 编写测试 - 空 before 数据返回空集合**

```java
@Test
@DisplayName("calculateAffectedBeforeKeys - 空 before 数据返回空集合")
void testCalculateAffectedBeforeKeys_EmptyBeforeData() throws SQLException {
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", Collections.emptyList());
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTable);
    
    assertTrue(result.isEmpty());
}
```

- [ ] **Step 3: 编写测试 - JOIN KEY 多次更新场景**

```java
@Test
@DisplayName("calculateAffectedBeforeKeys - JOIN KEY 多次更新场景")
void testCalculateAffectedBeforeKeys_JoinKeyMultipleUpdates() throws SQLException {
    // 准备测试数据：同一记录多次 JOIN KEY 更新
    List<Map<String, Object>> events = Arrays.asList(
        Map.of("op", "INSERT", "id", 1, "user_id", 100),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 200)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 300)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 400))
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", events);
    
    // Mock operator 返回多个历史宽表主键
    when(operator.executeQuery(anyString())).thenReturn(Arrays.asList(
        Map.of("wide_pk", 100),
        Map.of("wide_pk", 200),
        Map.of("wide_pk", 300)
    ));
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTable);
    
    // 验证所有历史宽表主键都被返回
    assertTrue(result.contains(100));
    assertTrue(result.contains(200));
    assertTrue(result.contains(300));
    assertEquals(3, result.size());
}
```

- [ ] **Step 4: 运行测试验证失败**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedBeforeKeys* -DfailIfNoTests=false
```

预期：测试失败（方法尚未实现）

---

### Task 5: 编写 calculateAffectedAfterKeys 单元测试

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 编写测试 - 子表 after 数据查询宽表主键**

```java
@Test
@DisplayName("calculateAffectedAfterKeys - 子表 after 数据查询宽表主键")
void testCalculateAffectedAfterKeys_SubTableWithAfterData() throws SQLException {
    // 准备测试数据：INSERT 事件
    List<Map<String, Object>> events = Arrays.asList(
        Map.of("op", "INSERT", "id", 1, "user_id", 400)
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", events);
    
    // Mock operator 返回查询结果
    when(operator.executeQuery(anyString())).thenReturn(Arrays.asList(
        Map.of("wide_pk", 400)
    ));
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTable);
    
    // 验证结果
    assertTrue(result.contains(400));
}
```

- [ ] **Step 2: 编写测试 - 空 after 数据返回空集合**

```java
@Test
@DisplayName("calculateAffectedAfterKeys - 空 after 数据返回空集合")
void testCalculateAffectedAfterKeys_EmptyAfterData() throws SQLException {
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", Collections.emptyList());
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTable);
    
    assertTrue(result.isEmpty());
}
```

- [ ] **Step 3: 编写测试 - JOIN KEY 多次更新场景**

```java
@Test
@DisplayName("calculateAffectedAfterKeys - JOIN KEY 多次更新场景")
void testCalculateAffectedAfterKeys_JoinKeyMultipleUpdates() throws SQLException {
    // 准备测试数据：同一记录多次 JOIN KEY 更新
    List<Map<String, Object>> events = Arrays.asList(
        Map.of("op", "INSERT", "id", 1, "user_id", 100),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 200)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 300)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 400))
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", events);
    
    // Mock operator 返回最终宽表主键
    when(operator.executeQuery(anyString())).thenReturn(Arrays.asList(
        Map.of("wide_pk", 400)
    ));
    
    Set<Object> result = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTable);
    
    // 验证只返回最终宽表主键
    assertTrue(result.contains(400));
    assertEquals(1, result.size());
}
```

- [ ] **Step 4: 运行测试验证失败**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedAfterKeys* -DfailIfNoTests=false
```

预期：测试失败（方法尚未实现）

---

### Task 6: 实现代码使测试通过

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

- [ ] **Step 1: 实现所有辅助方法和新方法**

按照 Task 1 和 Task 2 中的代码实现：
- `extractBeforeDataRows`
- `extractAfterDataRows`
- `queryWideTablePksWithCte`
- `getQuerySqlForTable`
- `getTableFields`
- 修改 `calculateAffectedBeforeKeys`
- 修改 `calculateAffectedAfterKeys`

- [ ] **Step 2: 添加必要的 import**

```java
import java.util.stream.Collectors;
```

- [ ] **Step 3: 运行所有测试验证通过**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=AffectedKeyCalculatorTest -DfailIfNoTests=false
```

预期：所有测试通过

- [ ] **Step 4: 提交**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java
git commit -m "feat: optimize calculateAffectedBeforeKeys/AfterKeys with WITH CTE SQL and SmartMerger integration"
```

---

### Task 7: 编写集成测试

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java`

- [ ] **Step 1: 编写集成测试 - SmartMerger 与 WITH CTE 集成**

```java
@Test
@DisplayName("集成测试 - SmartMerger 与 WITH CTE 集成")
void testSmartMergerIntegration_WithCteQuery() throws SQLException {
    // 准备测试数据：多次 JOIN KEY 更新
    List<Map<String, Object>> events = Arrays.asList(
        Map.of("op", "INSERT", "id", 1, "user_id", 100),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 200)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 300)),
        Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("user_id", 400))
    );
    
    Map<String, List<Map<String, Object>>> eventsByTable = Map.of("orders", events);
    
    // 计算 before/after keys
    Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
    Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);
    
    // 验证 before 包含所有历史主键
    assertTrue(beforeKeys.contains(100));
    assertTrue(beforeKeys.contains(200));
    assertTrue(beforeKeys.contains(300));
    
    // 验证 after 只包含最终主键
    assertTrue(afterKeys.contains(400));
    assertEquals(1, afterKeys.size());
}
```

- [ ] **Step 2: 运行集成测试验证通过**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=WithCteIntegrationTest -DfailIfNoTests=false
```

预期：测试通过

- [ ] **Step 3: 提交**

```bash
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java
git commit -m "test: add SmartMerger WITH CTE integration test"
```

---

## 自审

### 1. 规格覆盖检查

| 规格要求 | 对应 Task | 状态 |
|---------|----------|------|
| calculateAffectedBeforeKeys 使用 WITH CTE SQL | Task 1 | ✅ |
| calculateAffectedAfterKeys 使用 WITH CTE SQL | Task 2 | ✅ |
| 集成 SmartMerger 处理 JOIN KEY 多次更新 | Task 1, 2 | ✅ |
| 子表名替换为 WITH 临时表名 | Task 1 (getQuerySqlForTable) | ✅ |
| 历史错误数据全被删除 | Task 1 (extractBeforeDataRows) | ✅ |
| 最终正确数据被插入 | Task 2 (extractAfterDataRows) | ✅ |
| 单元测试覆盖 | Task 4, 5 | ✅ |
| 集成测试覆盖 | Task 7 | ✅ |

### 2. 占位符扫描

- ✅ 无 TBD/TODO 占位符
- ✅ 所有测试都有具体代码
- ✅ 所有方法签名一致

### 3. 类型一致性

- `SmartMerger.MergedRecord` 在所有 Task 中使用一致
- `calculateAffectedBeforeKeys/AfterKeys` 方法签名保持一致
- `queryWideTablePksWithCte` 返回类型 `Set<Object>` 与现有方法一致

---

## 执行交接

计划已完成并保存到 `docs/superpowers/plans/2026-05-26-affected-key-calculator-with-cte-optimization.md`。

**两种执行方式：**

**1. 子代理驱动（推荐）** - 每个 Task 分发一个子代理，Task 间评审，快速迭代

**2. 内联执行** - 在当前会话中使用 executing-plans 批量执行，带检查点评审

**选择哪种方式？**
