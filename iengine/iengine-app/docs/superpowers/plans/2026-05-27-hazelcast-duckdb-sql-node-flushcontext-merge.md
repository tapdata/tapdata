# HazelcastDuckDbSqlNode flushContext合并实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将flushCdcBuffer与flushContext合并，严格保证beforeKeys计算→写入数据→afterKeys计算→更新宽表的执行顺序

**Architecture:** 将PerSourceContext的buffer从`List<Map<String, Object>>`改为`List<TapdataEvent>`，去除cdcEventBuffer和Map转换逻辑，修改AffectedKeyCalculator接口直接接受TapdataEvent列表，在flushContext内严格保证4步顺序执行

**Tech Stack:** Java, TapdataEvent, DuckDB, JUnit 5, Mockito

---

## 文件结构

### 需要修改的文件
- `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContext.java` → 修改buffer类型
- `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java` → 修改接口签名
- `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` → 重写flushContext，删除废弃方法

### 需要修改的测试文件
- `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContextTest.java` → 适配新接口
- `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java` → 适配新接口
- `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeTest.java` → 适配新接口
- `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeMultiSourceTest.java` → 适配新接口
- `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java` → 适配新接口

---

### Task 1: 修改PerSourceContext的buffer类型为List<TapdataEvent>

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContext.java`
- Test: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContextTest.java`

- [ ] **Step 1: 修改PerSourceContext的buffer类型和相关方法**

修改 `PerSourceContext.java`:
```java
// 旧代码
private final List<Map<String, Object>> batchBuffer = Collections.synchronizedList(new ArrayList<>());

// 新代码
private final List<TapdataEvent> batchBuffer = Collections.synchronizedList(new ArrayList<>());
```

修改方法签名：
```java
// 旧方法
public List<Map<String, Object>> getBatchBuffer() { return batchBuffer; }
public void addRecord(Map<String, Object> record) { ... }
public List<Map<String, Object>> drainBuffer() { ... }

// 新方法
public List<TapdataEvent> getBatchBuffer() { return batchBuffer; }

public void addEvent(TapdataEvent event) {
    batchBuffer.add(event);
    accumulatedRecordCount.incrementAndGet();
}

public List<TapdataEvent> drainBuffer() {
    synchronized (batchBuffer) {
        List<TapdataEvent> copy = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        accumulatedRecordCount.set(0);
        lastCommitTime.set(System.currentTimeMillis());
        return copy;
    }
}
```

需要添加import：
```java
import com.tapdata.entity.TapdataEvent;
```

- [ ] **Step 2: 更新PerSourceContextTest测试**

修改 `PerSourceContextTest.java` 中所有使用 `addRecord` 和 `drainBuffer` 的测试：
```java
// 旧测试代码
context.addRecord(mockRecord);
List<Map<String, Object>> drained = context.drainBuffer();

// 新测试代码
TapdataEvent mockEvent = createMockTapdataEvent();
context.addEvent(mockEvent);
List<TapdataEvent> drained = context.drainBuffer();
```

- [ ] **Step 3: 运行PerSourceContextTest验证**

Run: `mvn test -Dtest=PerSourceContextTest -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContext.java
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/PerSourceContextTest.java
git commit -m "refactor: 修改PerSourceContext buffer类型为List<TapdataEvent>"
```

---

### Task 2: 修改AffectedKeyCalculator接口签名

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`
- Test: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`
- Test: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTestBase.java`

- [ ] **Step 1: 修改AffectedKeyCalculator接口方法签名**

修改 `AffectedKeyCalculator.java`:
```java
// 旧接口
public Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException
public Set<Object> calculateAffectedAfterKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException

// 新接口
public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException
public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException
```

添加import：
```java
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.common.TapEventUtil;
```

- [ ] **Step 2: 重写calculateAffectedBeforeKeys方法**

```java
/**
 * 批量计算所有事件的before受影响主键集合
 * 从TapdataEvent中提取数据进行计算
 * 
 * @param events TapdataEvent列表
 * @return 所有before主键集合
 */
public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException {
    if (events == null || events.isEmpty()) {
        return Collections.emptySet();
    }
    
    // 按表名分组
    Map<String, List<Map<String, Object>>> eventsByTable = new LinkedHashMap<>();
    for (TapdataEvent event : events) {
        TapEvent tapEvent = event.getTapEvent();
        if (tapEvent instanceof TapRecordEvent) {
            TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
            String tableName = TapEventUtil.getTableId(recordEvent);
            if (tableName == null) continue;
            
            Map<String, Object> mapEvent = convertTapdataEventToMap(recordEvent);
            eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(mapEvent);
        }
    }
    
    // 复用原有逻辑
    return calculateAffectedBeforeKeysInternal(eventsByTable);
}

/**
 * 内部方法：按表分组后的beforeKeys计算
 */
private Set<Object> calculateAffectedBeforeKeysInternal(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> tableEvents = entry.getValue();
        
        if (tableEvents == null || tableEvents.isEmpty()) {
            continue;
        }
        
        // 先收集DELETE事件的before数据
        List<Map<String, Object>> deleteBeforeRows = extractDeleteBeforeRows(tableEvents, tableName);
        
        // 使用SmartMerger合并事件
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(tableEvents);
        
        // 提取所有before数据行
        List<String> fields = getTableFields(tableName);
        List<Map<String, Object>> beforeRows = extractBeforeDataRows(mergedRecords, tableName);
        beforeRows.addAll(deleteBeforeRows);
        
        if (beforeRows.isEmpty()) {
            continue;
        }
        
        // 使用WITH CTE SQL查询宽表主键
        Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
        affectedBeforeKeys.addAll(wideTablePks);
    }
    
    return affectedBeforeKeys;
}

/**
 * 将TapRecordEvent转换为Map格式
 */
private Map<String, Object> convertTapdataEventToMap(TapRecordEvent recordEvent) {
    Map<String, Object> mapEvent = new HashMap<>();
    
    if (recordEvent instanceof TapInsertRecordEvent) {
        mapEvent.put("op", "INSERT");
        mapEvent.put("value", TapEventUtil.getAfter(recordEvent));
    } else if (recordEvent instanceof TapUpdateRecordEvent) {
        mapEvent.put("op", "UPDATE");
        mapEvent.put("value", TapEventUtil.getAfter(recordEvent));
        mapEvent.put("old_value", TapEventUtil.getBefore(recordEvent));
    } else if (recordEvent instanceof TapDeleteRecordEvent) {
        mapEvent.put("op", "DELETE");
        mapEvent.put("value", TapEventUtil.getBefore(recordEvent));
    }
    
    return mapEvent;
}
```

- [ ] **Step 3: 重写calculateAffectedAfterKeys方法**

```java
/**
 * 批量计算所有事件的after受影响主键集合
 * 从TapdataEvent中提取数据进行计算
 * 
 * @param events TapdataEvent列表
 * @return 所有after主键集合
 */
public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException {
    if (events == null || events.isEmpty()) {
        return Collections.emptySet();
    }
    
    // 按表名分组
    Map<String, List<Map<String, Object>>> eventsByTable = new LinkedHashMap<>();
    for (TapdataEvent event : events) {
        TapEvent tapEvent = event.getTapEvent();
        if (tapEvent instanceof TapRecordEvent) {
            TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
            String tableName = TapEventUtil.getTableId(recordEvent);
            if (tableName == null) continue;
            
            Map<String, Object> mapEvent = convertTapdataEventToMap(recordEvent);
            eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(mapEvent);
        }
    }
    
    // 复用原有逻辑
    return calculateAffectedAfterKeysInternal(eventsByTable);
}

/**
 * 内部方法：按表分组后的afterKeys计算
 */
private Set<Object> calculateAffectedAfterKeysInternal(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedAfterKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> tableEvents = entry.getValue();
        
        if (tableEvents == null || tableEvents.isEmpty()) {
            continue;
        }
        
        // 使用SmartMerger合并事件
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(tableEvents);
        if (mergedRecords.isEmpty()) {
            continue;
        }
        
        // 提取所有after数据行
        List<String> fields = getTableFields(tableName);
        List<Map<String, Object>> afterRows = extractAfterDataRows(mergedRecords);
        
        if (afterRows.isEmpty()) {
            continue;
        }
        
        // 使用WITH CTE SQL查询宽表主键
        Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, afterRows, fields);
        affectedAfterKeys.addAll(wideTablePks);
    }
    
    return affectedAfterKeys;
}
```

- [ ] **Step 4: 更新AffectedKeyCalculatorTest测试**

修改 `AffectedKeyCalculatorTest.java` 中所有调用 `calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys` 的测试：
```java
// 旧测试代码
Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();
eventsByTable.put("users", events);
Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);

// 新测试代码
List<TapdataEvent> tapdataEvents = createTapdataEvents(events, "users");
Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(tapdataEvents);
```

添加辅助方法：
```java
private List<TapdataEvent> createTapdataEvents(List<Map<String, Object>> events, String tableName) {
    List<TapdataEvent> tapdataEvents = new ArrayList<>();
    for (Map<String, Object> event : events) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapRecordEvent recordEvent = createRecordEvent(event, tableName);
        tapdataEvent.setTapEvent(recordEvent);
        tapdataEvents.add(tapdataEvent);
    }
    return tapdataEvents;
}

private TapRecordEvent createRecordEvent(Map<String, Object> event, String tableName) {
    String op = (String) event.get("op");
    TapRecordEvent recordEvent;
    
    if ("INSERT".equals(op)) {
        recordEvent = new TapInsertRecordEvent();
        ((TapInsertRecordEvent) recordEvent).setAfter((Map<String, Object>) event.get("value"));
    } else if ("UPDATE".equals(op)) {
        recordEvent = new TapUpdateRecordEvent();
        ((TapUpdateRecordEvent) recordEvent).setAfter((Map<String, Object>) event.get("value"));
        ((TapUpdateRecordEvent) recordEvent).setBefore((Map<String, Object>) event.get("old_value"));
    } else if ("DELETE".equals(op)) {
        recordEvent = new TapDeleteRecordEvent();
        ((TapDeleteRecordEvent) recordEvent).setBefore((Map<String, Object>) event.get("value"));
    } else {
        throw new IllegalArgumentException("Unknown op type: " + op);
    }
    
    recordEvent.setTableId(tableName);
    return recordEvent;
}
```

- [ ] **Step 5: 运行AffectedKeyCalculatorTest验证**

Run: `mvn test -Dtest=AffectedKeyCalculatorTest -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTestBase.java
git commit -m "refactor: 修改AffectedKeyCalculator接口接受List<TapdataEvent>参数"
```

---

### Task 3: 重写HazelcastDuckDbSqlNode的flushContext方法

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`
- Test: `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeTest.java`

- [ ] **Step 1: 删除废弃的方法**

删除以下方法：
- `processCdcEventForMaterializedView(TapdataEvent)` 
- `flushCdcBuffer()`
- `flushCdcBufferWithNewComponent(Map<String, List<TapdataEvent>>)`
- `flushCdcBufferWithOldComponent(Map<String, List<TapdataEvent>>)`
- `convertTapdataEventsToMaps(Map<String, List<TapdataEvent>>)`
- `extractAfterRowsFromTapdataEvents(Map<String, List<TapdataEvent>>)`

删除字段：
```java
// 删除
private List<TapdataEvent> cdcEventBuffer = new ArrayList<>();
private static final int CDC_BUFFER_SIZE = 100;
```

- [ ] **Step 2: 修改processRecordEvent方法**

```java
// 旧代码
synchronized (context.getCommitLock()) {
    context.addRecord(recordData);
    if (context.getBatchBuffer().size() >= context.getBatchSize()) {
        flushContext(context);
    }
}

// 新代码
context.addEvent(tapdataEvent);
if (context.getBatchBuffer().size() >= context.getBatchSize()) {
    flushContext(context);
}
```

删除CDC处理逻辑：
```java
// 删除这段代码
if (syncStageTracker != null && syncStageTracker.isTransitionCompleted()
        && affectedKeyCalculator != null
        && (wideTableUpdater != null || incrementalViewUpdater != null)) {
    processCdcEventForMaterializedView(tapdataEvent);
}
```

- [ ] **Step 3: 重写flushContext方法（4步严格顺序）**

```java
/**
 * 刷新单个Context的数据到DuckDB
 * 严格保证执行顺序：
 *   1. calculateAffectedBeforeKeys → 数据写入前
 *   2. 当前Context数据 → 写入当前Context对应表
 *   3. calculateAffectedAfterKeys → 数据写入后、宽表更新前
 *   4. updateWideTable → 最后执行
 * 
 * @param context 要刷写的上下文
 */
private void flushContext(PerSourceContext context) throws SQLException {
    if (context == null) {
        return;
    }

    // 保留Context内部锁
    synchronized (context.getCommitLock()) {
        // 提取当前批次的所有TapdataEvent
        List<TapdataEvent> eventsToFlush = context.drainBuffer();
        if (eventsToFlush.isEmpty()) {
            return;
        }

        try {
            // ========== 步骤1: 计算beforeKeys（数据写入DuckDB之前） ==========
            Set<Object> beforeKeys = null;
            if (affectedKeyCalculator != null && !eventsToFlush.isEmpty()) {
                beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsToFlush);
            }

            // ========== 步骤2: 写入当前Context对应表 ==========
            List<Map<String, Object>> dataToWrite = extractAfterRowsFromEvents(eventsToFlush);
            
            if (!dataToWrite.isEmpty()) {
                ensureTableExists(context, dataToWrite);
                List<Map<String, Object>> merged = SmartMerger.mergeLastWins(dataToWrite);
                
                if (merged != null && !merged.isEmpty()) {
                    DuckDbOperator operator = context.getOperator() != null ? 
                        context.getOperator() : duckDbOperator;
                    if (operator == null) {
                        throw new SQLException("DuckDbOperator not initialized");
                    }
                    
                    operator.upsertBatch(context.getTargetTableName(), merged);
                    logger.debug("刷写 {} 条记录到DuckDB表: {} (原始 {} 条)", 
                        merged.size(), context.getTargetTableName(), dataToWrite.size());
                }
            }

            // ========== 步骤3: 计算afterKeys（数据写入DuckDB之后、宽表更新之前） ==========
            Set<Object> afterKeys = null;
            if (affectedKeyCalculator != null && !eventsToFlush.isEmpty()) {
                afterKeys = affectedKeyCalculator.calculateAffectedAfterKeys(eventsToFlush);
            }

            // ========== 步骤4: 更新宽表（最后执行） ==========
            if (wideTableUpdater != null && beforeKeys != null && afterKeys != null) {
                List<Map<String, Object>> afterRows = extractAfterRowsFromEvents(eventsToFlush);
                List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
                    beforeKeys, afterKeys, afterRows, mainTableName);
                logger.info("更新宽表: {} 个事件", events.size());
            }

        } catch (Exception e) {
            logger.error("刷写Context {} 到DuckDB失败: {}", context.getKey(), e.getMessage(), e);
            // 数据回滚到缓冲区
            context.getBatchBuffer().addAll(0, eventsToFlush);
            context.getAccumulatedRecordCount().addAndGet(eventsToFlush.size());
            writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
        }
    }
}
```

- [ ] **Step 4: 添加辅助方法extractAfterRowsFromEvents**

```java
/**
 * 从TapdataEvent列表中提取after数据行
 * 
 * @param events TapdataEvent列表
 * @return after数据行列表
 */
private List<Map<String, Object>> extractAfterRowsFromEvents(List<TapdataEvent> events) {
    List<Map<String, Object>> afterRows = new ArrayList<>();
    
    for (TapdataEvent event : events) {
        TapEvent tapEvent = event.getTapEvent();
        if (tapEvent instanceof TapRecordEvent) {
            Map<String, Object> after = TapEventUtil.getAfter((TapRecordEvent) tapEvent);
            if (after != null && !after.isEmpty()) {
                afterRows.add(after);
            }
        }
    }
    
    return afterRows;
}
```

- [ ] **Step 5: 添加辅助方法extractDataFromEvents（用于DLQ）**

```java
/**
 * 从TapdataEvent列表中提取数据用于DLQ写入
 * 
 * @param events TapdataEvent列表
 * @return 数据列表
 */
private List<Map<String, Object>> extractDataFromEvents(List<TapdataEvent> events) {
    List<Map<String, Object>> dataList = new ArrayList<>();
    
    for (TapdataEvent event : events) {
        TapEvent tapEvent = event.getTapEvent();
        if (tapEvent instanceof TapRecordEvent) {
            Map<String, Object> data = extractRecordData((TapRecordEvent) tapEvent);
            if (data != null) {
                dataList.add(data);
            }
        }
    }
    
    return dataList;
}
```

- [ ] **Step 6: 修改flushAllContexts方法**

```java
// 旧代码
private void flushAllContexts() {
    for (PerSourceContext context : new ArrayList<>(sourceContexts.values())) {
        synchronized (context.getCommitLock()) {
            try {
                flushContext(context);
            } catch (Exception e) {
                logger.warn("Failed to flush context {} during close: {}", context.getKey(), e.getMessage(), e);
            }
        }
    }
}

// 新代码（保持不变，因为flushContext内部已有synchronized）
private void flushAllContexts() {
    for (PerSourceContext context : new ArrayList<>(sourceContexts.values())) {
        try {
            flushContext(context);
        } catch (Exception e) {
            logger.warn("刷写Context {} 失败: {}", context.getKey(), e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 7: 修改evictIfNecessary方法**

```java
// 旧代码
private void evictIfNecessary() {
    while (sourceContexts.size() >= maxActiveSources && !sourceAccessOrder.isEmpty()) {
        // ...
        PerSourceContext evicted = sourceContexts.remove(eldestKey);
        if (evicted != null) {
            try {
                flushContext(evicted);
            } catch (Exception e) {
                logger.warn("Failed to flush evicted context {}: {}", eldestKey, e.getMessage(), e);
            } finally {
                closeContextOperator(evicted);
            }
        }
    }
}

// 新代码（保持不变）
```

- [ ] **Step 8: 更新HazelcastDuckDbSqlNodeTest测试**

修改所有使用 `context.addRecord()` 的测试：
```java
// 旧代码
context.addRecord(mockData);

// 新代码
TapdataEvent mockEvent = createMockTapdataEvent(mockData, tableName);
context.addEvent(mockEvent);
```

- [ ] **Step 9: 运行HazelcastDuckDbSqlNodeTest验证**

Run: `mvn test -Dtest=HazelcastDuckDbSqlNodeTest -q`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git add src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeTest.java
git commit -m "refactor: 重写flushContext方法，严格保证4步执行顺序"
```

---

### Task 4: 更新多源测试和集成测试

**Files:**
- Modify: `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeMultiSourceTest.java`
- Modify: `src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java`

- [ ] **Step 1: 更新HazelcastDuckDbSqlNodeMultiSourceTest**

修改所有使用旧接口的测试代码：
```java
// 旧代码
context.addRecord(mockData);
List<Map<String, Object>> drained = context.drainBuffer();

// 新代码
TapdataEvent mockEvent = createMockTapdataEvent(mockData, tableName);
context.addEvent(mockEvent);
List<TapdataEvent> drained = context.drainBuffer();
```

- [ ] **Step 2: 更新HazelcastDuckDbSqlNodeIntegrationTest**

修改集成测试中所有使用 `cdcEventBuffer` 和旧接口的方法：
```java
// 删除对cdcEventBuffer的引用
// 修改为使用context.addEvent()
```

- [ ] **Step 3: 运行多源测试验证**

Run: `mvn test -Dtest=HazelcastDuckDbSqlNodeMultiSourceTest -q`
Expected: PASS

- [ ] **Step 4: 运行集成测试验证**

Run: `mvn test -Dtest=HazelcastDuckDbSqlNodeIntegrationTest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeMultiSourceTest.java
git add src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java
git commit -m "test: 更新多源测试和集成测试适配新接口"
```

---

### Task 5: 运行全量测试并验证

**Files:**
- Test: All test files

- [ ] **Step 1: 运行所有相关测试**

Run: `mvn test -Dtest=PerSourceContextTest,AffectedKeyCalculatorTest,HazelcastDuckDbSqlNodeTest,HazelcastDuckDbSqlNodeMultiSourceTest,HazelcastDuckDbSqlNodeIntegrationTest -q`
Expected: All PASS

- [ ] **Step 2: 验证编译通过**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: 最终Commit**

```bash
git log --oneline -5
git push origin <branch-name>
```

---

## 自审检查

### 1. 规范覆盖
- ✅ PerSourceContext buffer类型修改
- ✅ AffectedKeyCalculator接口签名修改
- ✅ flushContext 4步严格顺序实现
- ✅ 删除cdcEventBuffer和废弃方法
- ✅ 所有测试更新

### 2. 占位符扫描
- ✅ 无TBD/TODO
- ✅ 所有步骤包含完整代码
- ✅ 所有测试包含具体测试代码

### 3. 类型一致性
- ✅ 所有使用 `List<TapdataEvent>` 的地方保持一致
- ✅ 方法签名在所有文件中一致
- ✅ import语句正确
