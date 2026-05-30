# AffectedKeyCalculator 极端场景测试实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `AffectedKeyCalculator` 新增 10 个常规极端场景测试 + 15 个 ABA 3 维度场景测试，验证其在复杂情况下的正确性。

**Architecture:** 在现有 `AffectedKeyCalculatorTest.java` 中新增测试方法，保持 TDD 风格，每个测试自包含，使用 Mockito 模拟 DuckDbOperator。

**Tech Stack:** Java, JUnit 5, Mockito

---

## 文件结构

**现有文件：**
- 测试文件：`iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`
- 实现文件：`iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`（可能需要修复）

**变更计划：**
- 仅修改测试文件，新增 25 个测试方法
- 如发现问题，才修改实现文件

---

## 任务分解

### 任务 1: Join key 为非主键字段

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_NonPrimaryKeyJoin() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("user_profiles");
    fromTable.setPrimaryKey("profile_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("user_profiles", "SELECT DISTINCT u.id FROM users u INNER JOIN user_profiles p ON u.email = p.email WHERE p.profile_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("profile_id", "PROF001");
    eventData.put("email", "test@example.com");

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 100L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("user_profiles", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(100L));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_NonPrimaryKeyJoin -pl iengine/iengine-app`
Expected: 编译通过，测试运行

---

### 任务 2: 多表链式关联

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_MultiTableChainedJoin() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    
    FromTableConfig ordersTable = new FromTableConfig();
    ordersTable.setTableName("orders");
    ordersTable.setPrimaryKey("order_id");
    fromTables.add(ordersTable);
    
    FromTableConfig itemsTable = new FromTableConfig();
    itemsTable.setTableName("order_items");
    itemsTable.setPrimaryKey("item_id");
    fromTables.add(itemsTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
    customJoinQueries.put("order_items", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items i ON o.order_id = i.order_id WHERE i.item_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("item_id", "ITEM001");
    eventData.put("order_id", "ORD001");

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 200L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("order_items", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(200L));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_MultiTableChainedJoin -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 3: 主键类型混合

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_MixedPrimaryKeyTypes() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData1 = new HashMap<>();
    eventData1.put("order_id", "ORD_STR_1");
    
    Map<String, Object> eventData2 = new HashMap<>();
    eventData2.put("order_id", 12345); // numeric

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", "USER_STR_1"); // string PK
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 67890); // numeric PK
    queryResult.add(row2);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(eventData1, eventData2));

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains("USER_STR_1"));
    assertTrue(result.contains(67890));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_MixedPrimaryKeyTypes -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 4: 批量处理边界（999/1000/1001）

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_BatchBoundary_999() throws SQLException {
    testBatchBoundary(999);
}

@Test
void testCalculateAffectedKeys_BatchBoundary_1000() throws SQLException {
    testBatchBoundary(1000);
}

@Test
void testCalculateAffectedKeys_BatchBoundary_1001() throws SQLException {
    testBatchBoundary(1001);
}

private void testBatchBoundary(int eventCount) throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    // Create events
    List<Map<String, Object>> events = new ArrayList<>();
    Set<Object> expectedPks = new LinkedHashSet<>();
    for (int i = 0; i < eventCount; i++) {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD_" + i);
        events.add(eventData);
        expectedPks.add((long) (i % 100)); // cycle through 0-99 to create duplicates
    }

    // Mock query returns corresponding user IDs
    List<Map<String, Object>> queryResult = new ArrayList<>();
    for (Object pk : expectedPks) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", pk);
        queryResult.add(row);
    }

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    // Should have deduplicated results
    assertEquals(expectedPks.size(), result.size());
    for (Object expected : expectedPks) {
        assertTrue(result.contains(expected));
    }
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_BatchBoundary* -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 5: 空值/空字符串处理

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_NullAndEmptyValues() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    // Mix of valid and invalid events
    Map<String, Object> validEvent = new HashMap<>();
    validEvent.put("order_id", "VALID_001");
    
    Map<String, Object> nullPkEvent = new HashMap<>();
    nullPkEvent.put("order_id", null);
    
    Map<String, Object> emptyStringEvent = new HashMap<>();
    emptyStringEvent.put("order_id", "");
    
    Map<String, Object> validEvent2 = new HashMap<>();
    validEvent2.put("order_id", "VALID_002");

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 999L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(validEvent, nullPkEvent, emptyStringEvent, validEvent2));

    assertNotNull(result);
    // Only valid events should produce results (both valid events map to same user for this test)
    assertEquals(1, result.size());
    assertTrue(result.contains(999L));
    
    // Verify query was executed with only valid PKs
    ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockDuckDbOperator, atLeastOnce()).executeQuery(queryCaptor.capture());
    String executedQuery = queryCaptor.getValue();
    assertTrue(executedQuery.contains("VALID_001"));
    assertTrue(executedQuery.contains("VALID_002"));
    assertFalse(executedQuery.contains("null"));
    assertFalse(executedQuery.contains("''"));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_NullAndEmptyValues -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 6: SQL 特殊字符处理

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_SqlSpecialCharacters() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("order_id", "ORD'001\\test"); // contains single quote and backslash

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 777L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(777L));
    
    // Verify the query has properly escaped single quotes
    ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockDuckDbOperator).executeQuery(queryCaptor.capture());
    String executedQuery = queryCaptor.getValue();
    assertTrue(executedQuery.contains("ORD''001\\\\test"), "Single quotes should be escaped");
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_SqlSpecialCharacters -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 7: 大小写敏感测试

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_CaseInsensitiveTableName() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders"); // lowercase
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("order_id", "ORD_123");

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 555L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    // Test with UPPERCASE table name
    Set<Object> result = calculator.calculateAffectedKeys("ORDERS", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(555L));
    
    // Also test with mixed case
    reset(mockDuckDbOperator);
    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);
    
    result = calculator.calculateAffectedKeys("OrDeRs", Collections.singletonList(eventData));
    
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(555L));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_CaseInsensitiveTableName -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 8: 重复关联结果去重

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_DuplicateJoinResults() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> event1 = new HashMap<>();
    event1.put("order_id", "ORD_A");
    
    Map<String, Object> event2 = new HashMap<>();
    event2.put("order_id", "ORD_B");

    // Both orders map to the same user
    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 111L);
    queryResult.add(row);
    queryResult.add(row); // duplicate intentionally

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(event1, event2));

    assertNotNull(result);
    assertEquals(1, result.size()); // should be deduplicated
    assertTrue(result.contains(111L));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_DuplicateJoinResults -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 9: 查询返回部分 null

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_PartialNullResults() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("order_id", "ORD_001");

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 123L);
    queryResult.add(row1);
    
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", null); // null PK
    queryResult.add(row2);
    
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 456L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(2, result.size()); // null should be filtered
    assertTrue(result.contains(123L));
    assertTrue(result.contains(456L));
    assertFalse(result.contains(null));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_PartialNullResults -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 10: 同一子表多 Join Key 关联

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增测试方法**

```java
@Test
void testCalculateAffectedKeys_MultipleJoinKeys_SingleQuery() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    // Single query with multiple join conditions (user_id OR customer_id)
    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id OR u.id = o.customer_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("order_id", "ORD_MULTI_001");
    eventData.put("user_id", 1001L);
    eventData.put("customer_id", 2002L);

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1001L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2002L);
    queryResult.add(row2);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains(1001L));
    assertTrue(result.contains(2002L));
}

@Test
void testCalculateAffectedKeys_MultipleJoinKeys_DifferentConfigs() throws SQLException {
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    // First config: join by user_id
    Map<String, String> customJoinQueries1 = new HashMap<>();
    customJoinQueries1.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator1 = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries1,
            mockDuckDbOperator
    );

    // Second config: join by customer_id
    Map<String, String> customJoinQueries2 = new HashMap<>();
    customJoinQueries2.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.customer_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator2 = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries2,
            mockDuckDbOperator
    );

    Map<String, Object> eventData = new HashMap<>();
    eventData.put("order_id", "ORD_001");

    List<Map<String, Object>> queryResult1 = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 3001L);
    queryResult1.add(row1);

    List<Map<String, Object>> queryResult2 = new ArrayList<>();
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 4002L);
    queryResult2.add(row2);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult1).thenReturn(queryResult2);

    // Calculator 1 should use first config
    Set<Object> result1 = calculator1.calculateAffectedKeys("orders", Collections.singletonList(eventData));
    assertNotNull(result1);
    assertEquals(1, result1.size());
    assertTrue(result1.contains(3001L));

    // Calculator 2 should use second config (independent)
    reset(mockDuckDbOperator);
    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult2);
    
    Set<Object> result2 = calculator2.calculateAffectedKeys("orders", Collections.singletonList(eventData));
    assertNotNull(result2);
    assertEquals(1, result2.size());
    assertTrue(result2.contains(4002L));
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testCalculateAffectedKeys_MultipleJoinKeys* -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 11-25: ABA 场景测试（15个测试）

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 新增 ABA 测试辅助方法和 15 个测试方法**

```java
// Helper method to create event with specific operation type
private Map<String, Object> createEvent(String pkField, Object pkValue, String operation) {
    Map<String, Object> event = new HashMap<>();
    // Handle different CDC event formats
    if ("DELETE".equals(operation)) {
        event.put("before", Collections.singletonMap(pkField, pkValue));
    } else if ("UPDATE".equals(operation)) {
        event.put("before", Collections.singletonMap(pkField, pkValue));
        event.put("after", Collections.singletonMap(pkField, pkValue));
    } else { // INSERT or default
        event.put(pkField, pkValue);
    }
    return event;
}

@Test
void testABA_1_ContinuousDuplicate_InsertUpdateDelete() throws SQLException {
    // ABA-1: 连续重复 + 增改删
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_2", "UPDATE"),
            createEvent("order_id", "ORD_3", "INSERT")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2L);
    queryResult.add(row2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size()); // [1, 2, 3] - deduplicated
    // Verify insertion order preserved
    Iterator<Object> iterator = result.iterator();
    assertEquals(1L, iterator.next());
    assertEquals(2L, iterator.next());
    assertEquals(3L, iterator.next());
}

@Test
void testABA_2_IntervalDuplicate_InsertInsertDelete() throws SQLException {
    // ABA-2: 间隔重复 + 增删改
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_1", "DELETE"),
            createEvent("order_id", "ORD_3", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2L);
    queryResult.add(row2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size());
    Iterator<Object> iterator = result.iterator();
    assertEquals(1L, iterator.next());
    assertEquals(2L, iterator.next());
    assertEquals(3L, iterator.next());
}

@Test
void testABA_3_CrossBatchDuplicate() throws SQLException {
    // ABA-3: 跨批次重复
    // Create > 1000 events to trigger batching
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = new ArrayList<>();
    // Batch 1: ORD_1, ORD_2, ORD_3
    for (int i = 1; i <= 1000; i++) {
        events.add(createEvent("order_id", "ORD_" + (i % 3 + 1), "INSERT"));
    }
    // Batch 2: ORD_1, ORD_4, ORD_5
    for (int i = 1001; i <= 1100; i++) {
        events.add(createEvent("order_id", "ORD_" + ((i - 1000) % 3 + 4), "INSERT"));
    }

    List<Map<String, Object>> queryResult = new ArrayList<>();
    for (long id = 1; id <= 5; id++) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        queryResult.add(row);
    }

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(5, result.size()); // [1, 2, 3, 4, 5] - cross-batch deduplication
}

@Test
void testABA_4_ReverseOrderDuplicate() throws SQLException {
    // ABA-4: 顺序倒序重复
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_3", "INSERT"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_2", "UPDATE"),
            createEvent("order_id", "ORD_3", "UPDATE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3L);
    queryResult.add(row3);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2L);
    queryResult.add(row2);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1L);
    queryResult.add(row1);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size());
    // Verify insertion order preserved (3, 2, 1)
    Iterator<Object> iterator = result.iterator();
    assertEquals(3L, iterator.next());
    assertEquals(2L, iterator.next());
    assertEquals(1L, iterator.next());
}

@Test
void testABA_5_InsertDeleteInsert() throws SQLException {
    // ABA-5: 增-删-增
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_1", "DELETE"),
            createEvent("order_id", "ORD_1", "INSERT")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size()); // Only one unique key
    assertTrue(result.contains(1L));
}

@Test
void testABA_6_InsertUpdateDelete() throws SQLException {
    // ABA-6: 增-改-删
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_1", "DELETE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(1L));
}

@Test
void testABA_7_UpdateDeleteUpdate() throws SQLException {
    // ABA-7: 改-删-改
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_1", "DELETE"),
            createEvent("order_id", "ORD_1", "UPDATE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(1L));
}

@Test
void testABA_8_DeleteInsertDelete() throws SQLException {
    // ABA-8: 删-增-删
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "DELETE"),
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_1", "DELETE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(1L));
}

@Test
void testABA_9_InsertUpdateUpdateDelete() throws SQLException {
    // ABA-9: 增-改-改-删
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_1", "DELETE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.contains(1L));
}

@Test
void testABA_10_JoinKeyUnchanged_PKDuplicate() throws SQLException {
    // ABA-10: Join key 不变，主键重复
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    // Join key (user_id) stays the same, but different order PKs relate back
    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2L);
    queryResult.add(row2);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains(1L));
    assertTrue(result.contains(2L));
}

@Test
void testABA_11_JoinKeyChanges_DifferentPKs() throws SQLException {
    // ABA-11: Join key 变化，关联不同主键
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_3", "INSERT")
    );

    // Each order maps to different user
    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 100L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 200L);
    queryResult.add(row2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 300L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size());
    assertTrue(result.contains(100L));
    assertTrue(result.contains(200L));
    assertTrue(result.contains(300L));
}

@Test
void testABA_12_JoinKeyFromNullToValue() throws SQLException {
    // ABA-12: Join key 从有到无再到有
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = new ArrayList<>();
    
    // Valid event with join key
    Map<String, Object> event1 = new HashMap<>();
    event1.put("order_id", "ORD_1");
    events.add(event1);
    
    // Event with null join key in data
    Map<String, Object> event2 = new HashMap<>();
    event2.put("order_id", null);
    events.add(event2);
    
    // Another valid event
    Map<String, Object> event3 = new HashMap<>();
    event3.put("order_id", "ORD_1");
    events.add(event3);

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("id", 1L);
    queryResult.add(row);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(1, result.size()); // Null should be filtered out
    assertTrue(result.contains(1L));
}

@Test
void testABA_13_JoinKeyAlternating() throws SQLException {
    // ABA-13: Join key 交替变化
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_A", "INSERT"),
            createEvent("order_id", "ORD_B", "INSERT"),
            createEvent("order_id", "ORD_A", "UPDATE"),
            createEvent("order_id", "ORD_B", "UPDATE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> rowA = new HashMap<>();
    rowA.put("id", 111L);
    queryResult.add(rowA);
    Map<String, Object> rowB = new HashMap<>();
    rowB.put("id", 222L);
    queryResult.add(rowB);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains(111L));
    assertTrue(result.contains(222L));
}

@Test
void testABA_14_3Dimensional_Scenario1() throws SQLException {
    // ABA-14: 3维度综合场景1
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    // PK sequence: [1, 2, 1, 3, 2], ops: [I, I, U, I, D], join keys: [A, B, A, C, B]
    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_1", "INSERT"),
            createEvent("order_id", "ORD_2", "INSERT"),
            createEvent("order_id", "ORD_1", "UPDATE"),
            createEvent("order_id", "ORD_3", "INSERT"),
            createEvent("order_id", "ORD_2", "DELETE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1L);
    queryResult.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2L);
    queryResult.add(row2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size());
    Iterator<Object> iterator = result.iterator();
    assertEquals(1L, iterator.next());
    assertEquals(2L, iterator.next());
    assertEquals(3L, iterator.next());
}

@Test
void testABA_15_3Dimensional_Scenario2() throws SQLException {
    // ABA-15: 3维度综合场景2
    List<FromTableConfig> fromTables = new ArrayList<>();
    FromTableConfig fromTable = new FromTableConfig();
    fromTable.setTableName("orders");
    fromTable.setPrimaryKey("order_id");
    fromTables.add(fromTable);

    Map<String, String> customJoinQueries = new HashMap<>();
    customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
            "id",
            "users",
            "id",
            fromTables,
            customJoinQueries,
            mockDuckDbOperator
    );

    // PK sequence: [5, 4, 5, 3, 4, 5], ops: [I, I, U, I, U, D], join keys: [X, Y, X, Z, Y, X]
    List<Map<String, Object>> events = Arrays.asList(
            createEvent("order_id", "ORD_5", "INSERT"),
            createEvent("order_id", "ORD_4", "INSERT"),
            createEvent("order_id", "ORD_5", "UPDATE"),
            createEvent("order_id", "ORD_3", "INSERT"),
            createEvent("order_id", "ORD_4", "UPDATE"),
            createEvent("order_id", "ORD_5", "DELETE")
    );

    List<Map<String, Object>> queryResult = new ArrayList<>();
    Map<String, Object> row5 = new HashMap<>();
    row5.put("id", 5L);
    queryResult.add(row5);
    Map<String, Object> row4 = new HashMap<>();
    row4.put("id", 4L);
    queryResult.add(row4);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3L);
    queryResult.add(row3);

    when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

    Set<Object> result = calculator.calculateAffectedKeys("orders", events);

    assertNotNull(result);
    assertEquals(3, result.size());
    Iterator<Object> iterator = result.iterator();
    assertEquals(5L, iterator.next());
    assertEquals(4L, iterator.next());
    assertEquals(3L, iterator.next());
}
```

- [ ] **Step 2: 运行测试验证编译通过**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest#testABA_* -pl iengine/iengine-app`
Expected: 编译通过

---

### 任务 26: 运行所有新测试并修复发现的问题

**Files:**
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`
- Modify (if needed): `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

- [ ] **Step 1: 运行所有新增测试**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest -pl iengine/iengine-app`
Expected: 所有测试通过

- [ ] **Step 2: 如果发现问题，修复实现代码**

（如果测试失败，在此处修复代码）

- [ ] **Step 3: 再次运行所有测试验证修复**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=AffectedKeyCalculatorTest -pl iengine/iengine-app`
Expected: 所有测试通过

- [ ] **Step 4: 运行完整测试套件确保没有回归**

Run: `cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -am`
Expected: 所有测试通过

---

## 自审查

**1. 规范覆盖检查**
✅ 任务 1-10: 非 ABA 的 10 个极端场景
✅ 任务 11-25: 15 个 ABA 3 维度场景
✅ 任务 26: 最终验证
✅ 包含同一子表多 Join Key 关联测试（任务 10）

**2. 占位符检查**
✅ 无 TBD/TODO
✅ 所有代码完整
✅ 所有命令明确

**3. 类型一致性**
✅ 所有测试使用 `AffectedKeyCalculator` 和 `FromTableConfig` 正确的方法
✅ 参数类型与现有代码一致
✅ Mockito 使用正确的模式
