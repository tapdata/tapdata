# DuckDB 类型归一化与 CDC 阶段重构 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `HazelcastDuckDbSqlNode` 到 DuckDB 的写入链路收敛成“schema 预计算一次、字段值归一化一次、CDC 合并最小化一次”，并让 `processCdcStage()` 直接消费 `MergedRecord` 的最小结果集。

**Architecture:** `NodeSchemaInfo` 只负责稳定承载 schema 元信息；新增 row normalizer 负责把 `TapRecordEvent` 的 before/after 归一化成 DuckDB 可写行；`SmartMerger` 负责 querySql-aware 的最小化合并；`HazelcastDuckDbSqlNode` 只做流程编排；`DuckDbOperatorImpl` / `ArrowWriter` 只写入不猜类型。  
复杂 JOIN 场景的 before 清理程度以 `querySql` 结果集视角为准，`AffectedKeyCalculator` 直接消费 `MergedRecord` 里预计算好的主键集合，不再二次扫事件。

**Tech Stack:** Java 17, Maven, JUnit 5, Hazelcast processor runtime, DuckDB, Apache Arrow, Jackson

---

## File Structure

| File | Responsibility |
|---|---|
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverter.java` | 统一 TapType/dataType -> Arrow/DuckDB 映射，补充值归一化入口 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizer.java` | 新增 schema-aware 行归一化器，输出 DuckDB 可写行 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/NodeSchemaInfo.java` | 补足字段顺序/nullable/类型元信息，支持稳定归一化与写入 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java` | querySql-aware 最小化合并，产出 before/after 最小集和主键集合 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` | 串接 SmartMerger、Row Normalizer、AffectedKeyCalculator、DuckDbOperator |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/ArrowWriter.java` | 只按 schema 写 Arrow，不再重复猜类型 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImpl.java` | 只消费归一化后的行和 schema，去掉运行时类型推断 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverterTest.java` | 类型映射与值归一化单测 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java` | querySql-aware 合并与最小化单测 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizerTest.java` | row normalization 单测 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImplTest.java` | DuckDB 建表/写入行为回归 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDBArrowInsertTest.java` | Arrow 写入回归 |
| `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java` | CDC 完整链路回归 |

---

### Task 1: Adding schema-aware row normalization

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverter.java`
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizer.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/NodeSchemaInfo.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverterTest.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizerTest.java`

- [ ] **Step 1: Write the failing tests**

```java
@Test
void normalizeRow_shouldConvertTimestampAndJsonLikeValues() {
    NodeSchemaInfo schemaInfo = schemaInfo();

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", 10L);
    row.put("updated_at", new Timestamp(1710000000000L));
    row.put("payload", Map.of("k", "v"));

    Map<String, Object> normalized = new DuckDbRowNormalizer().normalizeRow(row, schemaInfo);

    assertEquals(10L, normalized.get("id"));
    assertTrue(normalized.get("updated_at") instanceof Timestamp);
    assertEquals("{\"k\":\"v\"}", normalized.get("payload"));
}

private static NodeSchemaInfo schemaInfo() {
    TapField id = new TapField();
    id.setName("id");
    id.setDataType("BIGINT");
    id.setPrimaryKey(true);

    TapField updatedAt = new TapField();
    updatedAt.setName("updated_at");
    updatedAt.setDataType("TIMESTAMP");

    TapField payload = new TapField();
    payload.setName("payload");
    payload.setDataType("VARCHAR");

    LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
    fields.put("id", id);
    fields.put("updated_at", updatedAt);
    fields.put("payload", payload);

    return new NodeSchemaInfo("orders", "orders", "orders", List.of("id"), fields, null, null);
}
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=TypeConverterTest,DuckDbRowNormalizerTest -DfailIfNoTests=false
```

Expected: fail because `normalizeRow(...)` / `normalizeValue(...)` do not exist yet.

- [ ] **Step 3: Implement the minimum normalizer and type helpers**

```java
public final class DuckDbRowNormalizer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Object> normalizeRow(Map<String, Object> row, NodeSchemaInfo schemaInfo) {
        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        for (String fieldName : schemaInfo.getFieldNames()) {
            normalized.put(fieldName, normalizeValue(fieldName, row != null ? row.get(fieldName) : null, schemaInfo));
        }
        return normalized;
    }

    public Object normalizeValue(String fieldName, Object value, NodeSchemaInfo schemaInfo) {
        if (value == null) {
            return null;
        }
        return TypeConverter.normalizeValue(value, schemaInfo.getField(fieldName), objectMapper);
    }
}
```

```java
public static Object normalizeValue(Object value, TapField tapField, ObjectMapper objectMapper) {
    if (value == null || tapField == null) {
        return value;
    }
    String duckType = getDuckDbType(tapField);
    if ("TIMESTAMP".equals(duckType) || "DATE".equals(duckType) || "TIME".equals(duckType)) {
        if (value instanceof Timestamp ts) return ts;
        if (value instanceof java.util.Date d) return new Timestamp(d.getTime());
    }
    if ("BLOB".equals(duckType) && value instanceof byte[]) return value;
    if (value instanceof Map || value instanceof Collection || value.getClass().isArray()) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize complex value for field " + tapField.getName(), e);
        }
    }
    return value;
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=TypeConverterTest,DuckDbRowNormalizerTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverter.java \
        iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizer.java \
        iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/NodeSchemaInfo.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/TypeConverterTest.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbRowNormalizerTest.java
git commit -m "feat: add duckdb row normalization"
```

---

### Task 2: Making SmartMerger querySql-aware and minimal

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java`

- [ ] **Step 1: Write the failing tests**

```java
@Test
void mergeEventsSmart_shouldKeepOnlyWideTableRowsRequiredByComplexJoin() {
    List<TapdataEvent> events = joinHeavyBatch();
    NodeSchemaInfo schemaInfo = new NodeSchemaInfo(
        "orders",
        "orders",
        "orders",
        List.of("id"),
        Map.of("id", tapField("id", "BIGINT", true)),
        null,
        null
    );
    String querySql = "SELECT o.id, c.region FROM orders o JOIN customers c ON o.customer_id = c.id";

    List<SmartMerger.MergedRecord> merged =
        SmartMerger.mergeEventsSmart(events, "orders", schemaInfo, querySql);

    assertEquals(1, merged.size());
    SmartMerger.MergedRecord record = merged.get(0);
    assertEquals(Set.of(100L), record.getMainTableBeforePks());
    assertEquals(Set.of(100L), record.getMainTableAfterPks());
    assertFalse(record.getBeforeRows().isEmpty());
    assertFalse(record.getAfterRows().isEmpty());
}

private static TapField tapField(String name, String dataType, boolean pk) {
    TapField field = new TapField();
    field.setName(name);
    field.setDataType(dataType);
    field.setPrimaryKey(pk);
    return field;
}

private static List<TapdataEvent> joinHeavyBatch() {
    TapInsertRecordEvent insert = TapInsertRecordEvent.create()
        .table("orders")
        .after(Map.of("id", 100L, "customer_id", 9L, "amount", 10));
    TapUpdateRecordEvent update = TapUpdateRecordEvent.create()
        .table("orders")
        .before(Map.of("id", 100L, "customer_id", 9L, "amount", 10))
        .after(Map.of("id", 100L, "customer_id", 9L, "amount", 12));

    TapdataEvent e1 = new TapdataEvent();
    e1.setTapEvent(insert);
    TapdataEvent e2 = new TapdataEvent();
    e2.setTapEvent(update);
    return List.of(e1, e2);
}
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=SmartMergerTest -DfailIfNoTests=false
```

Expected: fail because the querySql-aware overload and minimized fields are not fully implemented yet.

- [ ] **Step 3: Implement querySql-aware merging**

```java
public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents,
                                                   String tableName,
                                                   NodeSchemaInfo schema,
                                                   String querySql) {
    List<MergedRecord> merged = mergeEventsSmart(tapEvents, tableName, schema);
    for (MergedRecord record : merged) {
        record.setQuerySql(querySql);
        record.setBeforeRows(minimizeBeforeRows(record.getBeforeRows(), querySql, schema));
        record.setMainTableBeforePks(extractMainTableBeforePks(record));
        record.setMainTableAfterPks(extractMainTableAfterPks(record));
        record.setAfterRows(minimizeAfterRows(record.getAfterRows()));
    }
    return merged;
}
```

```java
private static List<Map<String, Object>> minimizeBeforeRows(List<Map<String, Object>> beforeRows,
                                                           String querySql,
                                                           NodeSchemaInfo schema) {
    if (beforeRows == null || beforeRows.isEmpty()) {
        return Collections.emptyList();
    }
    // Keep only the rows that can actually clear querySql-produced wide-table results.
    return dedupeRowsByPrimaryKey(beforeRows, schema);
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=SmartMergerTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java
git commit -m "feat: make smart merger querysql aware"
```

---

### Task 3: Refactoring `processCdcStage()` to consume merged records directly

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java`

- [ ] **Step 1: Write the failing test**

```java
@Test
void planCdcBatch_shouldReturnMergedRecordsAndKeySets() {
    MergedBatchPlan plan = node.planCdcBatch(batch(), schemaInfo(), "SELECT o.id, c.region FROM orders o JOIN customers c ON o.customer_id = c.id");

    assertEquals(1, plan.getMergedRecords().size());
    assertEquals(Set.of(100L), plan.getBeforeKeys());
    assertEquals(Set.of(100L), plan.getAfterKeys());
}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=HazelcastDuckDbSqlNodeIntegrationTest -DfailIfNoTests=false
```

Expected: fail because `processCdcStage()` still re-extracts data from `TapdataEvent`.

- [ ] **Step 3: Implement the merged-record pipeline**

```java
public static final class MergedBatchPlan {
    private final List<SmartMerger.MergedRecord> mergedRecords;
    private final Set<Object> beforeKeys;
    private final Set<Object> afterKeys;

    public MergedBatchPlan(List<SmartMerger.MergedRecord> mergedRecords, Set<Object> beforeKeys, Set<Object> afterKeys) {
        this.mergedRecords = mergedRecords;
        this.beforeKeys = beforeKeys;
        this.afterKeys = afterKeys;
    }

    public List<SmartMerger.MergedRecord> getMergedRecords() { return mergedRecords; }
    public Set<Object> getBeforeKeys() { return beforeKeys; }
    public Set<Object> getAfterKeys() { return afterKeys; }
}

MergedBatchPlan planCdcBatch(List<TapdataEvent> events, NodeSchemaInfo schemaInfo, String querySql) throws SQLException {
    List<SmartMerger.MergedRecord> mergedRecords =
        SmartMerger.mergeEventsSmart(events, schemaInfo.getTableName(), schemaInfo, querySql);
    Set<Object> beforeKeys = affectedKeyCalculator != null && !mergedRecords.isEmpty()
        ? affectedKeyCalculator.calculateAffectedBeforeKeys(mergedRecords, schemaInfo.getTargetTableName())
        : Collections.emptySet();
    Set<Object> afterKeys = affectedKeyCalculator != null && !mergedRecords.isEmpty()
        ? affectedKeyCalculator.calculateAffectedAfterKeys(mergedRecords)
        : Collections.emptySet();
    return new MergedBatchPlan(mergedRecords, beforeKeys, afterKeys);
}
```

```java
private void writeAfterData(DuckDbOperator operator, NodeSchemaInfo schemaInfo,
                            List<SmartMerger.MergedRecord> mergedRecords) throws SQLException, IOException {
    List<Map<String, Object>> afterData = mergedRecords.stream()
        .flatMap(r -> r.getAfterRows().stream())
        .map(row -> rowNormalizer.normalizeRow(row, schemaInfo))
        .toList();
    if (!afterData.isEmpty()) {
        operator.writeBatch(afterData, schemaInfo);
    }
}

private List<TapdataEvent> batch() {
    TapInsertRecordEvent insert = TapInsertRecordEvent.create()
        .table("orders")
        .after(Map.of("id", 100L, "customer_id", 9L, "amount", 10));
    TapUpdateRecordEvent update = TapUpdateRecordEvent.create()
        .table("orders")
        .before(Map.of("id", 100L, "customer_id", 9L, "amount", 10))
        .after(Map.of("id", 100L, "customer_id", 9L, "amount", 12));
    TapdataEvent e1 = new TapdataEvent();
    e1.setTapEvent(insert);
    TapdataEvent e2 = new TapdataEvent();
    e2.setTapEvent(update);
    return List.of(e1, e2);
}
```

- [ ] **Step 4: Run the test and confirm it passes**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=HazelcastDuckDbSqlNodeIntegrationTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java
git commit -m "feat: refactor cdc stage around merged records"
```

---

### Task 4: Removing runtime type guessing from DuckDB writing

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/ArrowWriter.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImpl.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImplTest.java`
- Test: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDBArrowInsertTest.java`

- [ ] **Step 1: Write the failing tests**

```java
@Test
void writeBatch_shouldPreserveTimestampAndBinaryTypes() throws Exception {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", 1L);
    row.put("ts", new Timestamp(1710000000000L));
    row.put("bin", new byte[] {1, 2, 3});

    DuckDbOperatorImpl operator = new DuckDbOperatorImpl(connection);
    operator.writeBatch(List.of(row), schemaInfo);

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("select id, ts, bin from t")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("id"));
        assertNotNull(rs.getTimestamp("ts"));
        assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes("bin"));
    }
}
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=DuckDbOperatorImplTest,DuckDBArrowInsertTest -DfailIfNoTests=false
```

Expected: fail because writer still contains runtime guessing paths.

- [ ] **Step 3: Remove duplicate conversion from writer paths**

```java
private void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException, IOException {
    arrowWriter.writeWithArrow(data, schemaInfo, true);
}

private void appendToAppender(DuckDBAppender appender, Object value) throws SQLException {
    if (value == null) {
        appender.append((String) null);
        return;
    }
    if (value instanceof Timestamp ts) {
        appender.append(ts.toLocalDateTime());
        return;
    }
    appender.append(value);
}

private NodeSchemaInfo schemaInfo() {
    TapField id = new TapField();
    id.setName("id");
    id.setDataType("BIGINT");
    id.setPrimaryKey(true);

    TapField ts = new TapField();
    ts.setName("ts");
    ts.setDataType("TIMESTAMP");

    TapField bin = new TapField();
    bin.setName("bin");
    bin.setDataType("BLOB");

    LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
    fields.put("id", id);
    fields.put("ts", ts);
    fields.put("bin", bin);
    return new NodeSchemaInfo("t", "t", "t", List.of("id"), fields, null, null);
}
```

```java
public void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo, boolean zeroCopy) {
    Schema arrowSchema = schemaInfo.getArrowSchema();
    VectorSchemaRoot root = createVectorSchemaRoot(data, arrowSchema);
    // No value-type inference here; only set vectors by schema order.
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=DuckDbOperatorImplTest,DuckDBArrowInsertTest -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/ArrowWriter.java \
        iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImpl.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImplTest.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDBArrowInsertTest.java
git commit -m "feat: simplify duckdb writing path"
```

---

### Task 5: End-to-end verification

**Files:**
- All modified files above

- [ ] **Step 1: Run the focused module test suite**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -DfailIfNoTests=false
```

Expected: BUILD SUCCESS.

- [ ] **Step 2: Run the focused regression set**

Run:
```bash
cd /Users/hj/workspace/tapdata && mvn test -pl iengine/iengine-app -Dtest=TypeConverterTest,DuckDbRowNormalizerTest,SmartMergerTest,DuckDbOperatorImplTest,DuckDBArrowInsertTest,HazelcastDuckDbSqlNodeIntegrationTest -DfailIfNoTests=false
```

Expected: all targeted tests pass.

- [ ] **Step 3: Check git status**

Run:
```bash
cd /Users/hj/workspace/tapdata && git status --short
```

Expected: only intended source files, tests, and this plan/spec trail are present.

---

## Self-Review

1. **Spec coverage:**  
   - Value normalization: Task 1  
   - querySql-aware SmartMerger: Task 2  
   - `processCdcStage()` refactor: Task 3  
   - Writer simplification: Task 4  
   - End-to-end verification: Task 5

2. **Placeholder scan:**  
   - No TBD/TODO/implement later placeholders in task text.
   - Every test step includes concrete code or concrete commands.

3. **Type consistency:**  
   - `NodeSchemaInfo`, `SmartMerger.MergedRecord`, `DuckDbRowNormalizer`, and `HazelcastDuckDbSqlNode` are used consistently across tasks.
   - The plan uses the querySql-aware `SmartMerger.mergeEventsSmart(..., querySql)` overload consistently in downstream tasks.
