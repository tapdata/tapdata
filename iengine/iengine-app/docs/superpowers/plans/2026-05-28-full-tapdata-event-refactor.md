# 全链路 TapdataEvent 重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 统一使用 TapdataEvent/TapRecordEvent 全链路处理 CDC 事件，完全移除 Map<String, Object> 中间格式，只保留实际被调用的方法

**Architecture:** 分阶段重构核心组件（SmartMerger → AffectedKeyCalculator → WideTableIncrementalUpdater → HazelcastDuckDbSqlNode），同步更新测试文件

**Tech Stack:** Java 8+, Tapdata Event Framework, JUnit

---

## 文件映射

| 文件 | 变更类型 | 描述 |
|------|----------|------|
| `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java` | 重构 | 核心合并逻辑重构 |
| `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java` | 重构 | 受影响键计算逻辑重构 |
| `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java` | 调整 | 保持接口，适配新数据结构 |
| `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DlqWriter.java` | 调整 | 适配新的 MergedRecord 结构 |
| `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` | 重构 | 更新调用链 |
| `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java` | 重构 | 更新测试用例 |
| `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java` | 重构 | 更新测试用例 |

---

## Task 1: SmartMerger.MergedRecord 重构

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java`

- [ ] **Step 1: 重构 MergedRecord 内部结构**

查看当前 MergedRecord 定义，修改：

```java
public static class MergedRecord {
    private final Object initialPk;
    private Object currentPk;
    // 从 List<Map<String, Object>> 改为 List<TapdataEvent>
    private final List<TapdataEvent> operations;
    // 从 Map<String, Object> 改为 TapRecordEvent
    private TapRecordEvent finalStateEvent;
    private String finalOp;
    
    // 重构构造函数
    public MergedRecord(Object initialPk, List<TapdataEvent> operations, TapRecordEvent finalStateEvent, String finalOp) {
        this.initialPk = initialPk;
        this.currentPk = initialPk;
        this.operations = operations;
        this.finalStateEvent = finalStateEvent;
        this.finalOp = finalOp;
    }
    
    // 添加便捷方法
    public Map<String, Object> getFinalState() {
        return finalStateEvent != null ? TapEventUtil.getAfter(finalStateEvent) : null;
    }
    
    // 保留其他 getters/setters
    public Object getInitialPk() { return initialPk; }
    public Object getCurrentPk() { return currentPk; }
    public void setCurrentPk(Object currentPk) { this.currentPk = currentPk; }
    public List<TapdataEvent> getOperations() { return operations; }
    public TapRecordEvent getFinalStateEvent() { return finalStateEvent; }
    public void setFinalStateEvent(TapRecordEvent finalStateEvent) { this.finalStateEvent = finalStateEvent; }
    public String getFinalOp() { return finalOp; }
    public void setFinalOp(String finalOp) { this.finalOp = finalOp; }
}
```

- [ ] **Step 2: 重构 mergeEventsSmart 核心逻辑**

修改 `mergeEventsSmart(List<TapdataEvent>)` 方法，确保它使用新的 MergedRecord 结构。核心逻辑保持不变，但数据结构变更。

- [ ] **Step 3: 重构 mergeLastWins 方法**

将 `mergeLastWins(List<Map<String, Object>>)` 重构为：

```java
public static List<Map<String, Object>> mergeLastWins(List<TapdataEvent> events) {
    if (events == null || events.isEmpty()) {
        return Collections.emptyList();
    }
    
    Map<Object, Map<String, Object>> lastByKey = new LinkedHashMap<>();
    
    for (TapdataEvent tapEvent : events) {
        TapEvent tapEventInner = tapEvent.getTapEvent();
        if (tapEventInner instanceof TapRecordEvent recordEvent) {
            Object pk = extractPrimaryKey(recordEvent);
            if (pk != null) {
                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null) {
                    lastByKey.put(pk, after);
                }
            }
        }
    }
    
    return new ArrayList<>(lastByKey.values());
}
```

- [ ] **Step 4: 移除所有旧的 Map API**

删除以下方法：
- `mergeEventsSmart(List<Map<String, Object>>)`
- `mergeLastWins(List<Map<String, Object>>)` (旧版本)
- `mergedRecordsToMaps(List<MergedRecord>)`
- `computeKeyForMap(Map<String, Object>)`
- `extractPkFromMap(Map<String, Object>)`
- `extractBeforePkFromMap(Map<String, Object>)`
- `extractAfterFromMap(Map<String, Object>)`
- `getRecordKey(Object, Map<Object, Object>)`

- [ ] **Step 5: 运行编译检查**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn compile -DskipTests
```
Expected: 编译通过（可能有其他类引用旧 API 的错误，后面会修复）

- [ ] **Step 6: 提交**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMerger.java
git commit -m "refactor: SmartMerger full TapdataEvent refactor"
```

---

## Task 2: AffectedKeyCalculator 重构

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

- [ ] **Step 1: 重构 extractBeforeDataRowsFromEvents 方法**

修改方法签名和实现：

```java
private List<Map<String, Object>> extractBeforeDataRowsFromEvents(
        List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
    List<Map<String, Object>> beforeRows = new ArrayList<>();
    String pkField = getSourceTablePrimaryKey(tableName);
    
    for (SmartMerger.MergedRecord mergedRecord : mergedRecords) {
        List<TapdataEvent> operations = mergedRecord.getOperations();
        if (operations.isEmpty()) {
            continue;
        }
        
        int opCount = operations.size();
        
        for (int i = 0; i < opCount; i++) {
            TapdataEvent tapEvent = operations.get(i);
            TapEvent tapEventInner = tapEvent.getTapEvent();
            
            if (!(tapEventInner instanceof TapRecordEvent recordEvent)) {
                continue;
            }
            
            boolean isLastOp = (i == opCount - 1);
            
            if (tapEventInner instanceof TapInsertRecordEvent) {
                if (!isLastOp) {
                    Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                    if (after != null) {
                        beforeRows.add(new HashMap<>(after));
                    }
                }
            } else if (tapEventInner instanceof TapUpdateRecordEvent) {
                Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
                if (before != null) {
                    beforeRows.add(new HashMap<>(before));
                }
            }
        }
        
        if (opCount > 0) {
            TapdataEvent lastEvent = operations.get(opCount - 1);
            TapEvent lastEventInner = lastEvent.getTapEvent();
            
            if (lastEventInner instanceof TapDeleteRecordEvent) {
                Map<String, Object> finalState = mergedRecord.getFinalState();
                if (finalState != null) {
                    beforeRows.add(new HashMap<>(finalState));
                }
            }
        }
    }
    
    return beforeRows;
}
```

- [ ] **Step 2: 重构 extractAfterDataRowsFromEvents 方法**

修改为使用新的 MergedRecord：

```java
private List<Map<String, Object>> extractAfterDataRowsFromEvents(
        List<SmartMerger.MergedRecord> mergedRecords) {
    List<Map<String, Object>> afterRows = new ArrayList<>();
    
    for (SmartMerger.MergedRecord mergedRecord : mergedRecords) {
        Map<String, Object> finalState = mergedRecord.getFinalState();
        if (finalState != null) {
            afterRows.add(finalState);
        }
    }
    
    return afterRows;
}
```

- [ ] **Step 3: 移除所有旧的 Map API**

删除以下方法（包括 @Deprecated 的）：
- `calculateAffectedKeys(String, List<Map<String, Object>>)`
- `extractBeforePrimaryKey(Map<String, Object>, String)`
- `extractAfterPrimaryKey(Map<String, Object>, String)`
- `isPrimaryKeyUpdated(Map<String, Object>, String)`
- 所有 Map 相关的私有辅助方法

- [ ] **Step 4: 运行编译检查**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn compile -DskipTests
```
Expected: 编译通过

- [ ] **Step 5: 提交**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java
git commit -m "refactor: AffectedKeyCalculator full TapdataEvent refactor"
```

---

## Task 3: DlqWriter 适配

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DlqWriter.java`

- [ ] **Step 1: 适配新的 MergedRecord 结构**

更新 `write` 方法中对 MergedRecord 的处理，`operations` 现在是 `List<TapdataEvent>`，需要适配序列化。

对于 `operations` 字段，可能需要转换或简化：
```java
// 在 write 方法中
if (mergedRecordState != null) {
    // 对于 operations，可能需要转换或只存储元数据
    // 这里保持向后兼容的处理
    List<Object> operationsForStorage = new ArrayList<>();
    for (TapdataEvent event : mergedRecordState.getOperations()) {
        // 简化存储，或者使用现有的序列化方式
        operationsForStorage.add(event.toString());
    }
    
    mergedRecordDoc = new Document()
        .append("initialPk", mergedRecordState.getInitialPk())
        .append("currentPk", mergedRecordState.getCurrentPk())
        .append("operations", operationsForStorage)  // 适配
        .append("finalState", mergedRecordState.getFinalState())
        .append("finalOp", mergedRecordState.getFinalOp());
}
```

- [ ] **Step 2: 运行编译检查**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn compile -DskipTests
```
Expected: 编译通过

- [ ] **Step 3: 提交**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DlqWriter.java
git commit -m "refactor: DlqWriter adapt to new MergedRecord structure"
```

---

## Task 4: HazelcastDuckDbSqlNode 重构

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

- [ ] **Step 1: 更新 flushContext 方法**

修改对 SmartMerger.mergeLastWins 的调用：

```java
// 在 flushContext 方法中
if (!eventsToFlush.isEmpty()) {
    ensureTableExists(context, eventsToFlush);  // 可能需要重构这个方法
    
    // 使用重构后的 mergeLastWins(List<TapdataEvent>)
    List<Map<String, Object>> merged = SmartMerger.mergeLastWins(eventsToFlush);
    
    if (merged != null && !merged.isEmpty()) {
        DuckDbOperator operator = context.getOperator() != null ? 
            context.getOperator() : duckDbOperator;
        if (operator == null) {
            throw new SQLException("DuckDbOperator not initialized");
        }
        
        operator.upsertBatch(context.getTargetTableName(), merged);
        logger.debug("刷写 {} 条记录到DuckDB表: {} (原始 {} 条)", 
            merged.size(), context.getTargetTableName(), eventsToFlush.size());
    }
}
```

- [ ] **Step 2: 更新 ensureTableExists 方法**

确保这个方法也能接受 `List<TapdataEvent>`。

- [ ] **Step 3: 更新 flushBasicBatch 方法**

同样修改另一个调用 SmartMerger.mergeLastWins 的地方。

- [ ] **Step 4: 重构 ensureTableExists 签名（如果需要）**

如果 `ensureTableExists` 接受 `List<Map<String, Object>>`，需要重构为接受 `List<TapdataEvent>`。

- [ ] **Step 5: 运行编译检查**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn compile -DskipTests
```
Expected: 编译通过

- [ ] **Step 6: 提交**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "refactor: HazelcastDuckDbSqlNode full TapdataEvent refactor"
```

---

## Task 5: WideTableIncrementalUpdater 调整

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

- [ ] **Step 1: 检查并调整接口**

确保这个类能与新的数据流正常工作。主要检查：
- 输入参数是否需要变更
- 输出是否保持 TapdataEvent 格式

- [ ] **Step 2: 运行编译检查**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn compile -DskipTests
```
Expected: 编译通过

- [ ] **Step 3: 提交（如有变更）**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git commit -m "refactor: WideTableIncrementalUpdater adapt to new data flow"
```

---

## Task 6: SmartMergerTest 重构

**Files:**
- Modify: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java`

- [ ] **Step 1: 更新所有测试用例**

将所有使用 Map 格式的测试改为使用 TapdataEvent。可以参考已经存在的 TapdataEvent 测试。

- [ ] **Step 2: 运行测试**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn test -Dtest=SmartMergerTest
```
Expected: 所有测试通过

- [ ] **Step 3: 提交**

```bash
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/SmartMergerTest.java
git commit -m "test: update SmartMergerTest to use TapdataEvent"
```

---

## Task 7: AffectedKeyCalculatorTest 重构

**Files:**
- Modify: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java`

- [ ] **Step 1: 更新所有测试用例**

将所有使用 Map 格式的测试改为使用 TapdataEvent。

- [ ] **Step 2: 运行测试**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn test -Dtest=AffectedKeyCalculatorTest
```
Expected: 所有测试通过

- [ ] **Step 3: 提交**

```bash
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorTest.java
git commit -m "test: update AffectedKeyCalculatorTest to use TapdataEvent"
```

---

## Task 8: 其他测试文件重构

**Files:**
- Check and update: `src/test/java/io/tapdata/flow/engine/V2/node/duckdb/` 目录下的其他测试文件

- [ ] **Step 1: 检查其他测试文件**

检查是否有其他测试文件引用了旧的 API，包括：
- AffectedKeyCalculatorTestBase 的实现类
- 其他相关测试

- [ ] **Step 2: 重构所有相关测试**

将所有使用 Map 格式的测试改为使用 TapdataEvent。

- [ ] **Step 3: 运行完整测试**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn test -Dtest="io.tapdata.flow.engine.V2.node.duckdb.*Test"
```
Expected: 所有测试通过

- [ ] **Step 4: 提交**

```bash
git add src/test/java/io/tapdata/flow/engine/V2/node/duckdb/
git commit -m "test: update all DuckDB node tests to use TapdataEvent"
```

---

## Task 9: 最终验证

**Files:**
- 所有相关文件

- [ ] **Step 1: 完整编译**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
mvn clean compile -DskipTests
```
Expected: 编译通过，无错误

- [ ] **Step 2: 完整测试**

```bash
mvn test -Dtest="io.tapdata.flow.engine.V2.node.duckdb.*Test,io.tapdata.flow.engine.V2.node.hazelcast.processor.*Test"
```
Expected: 所有测试通过

- [ ] **Step 3: 验证重构完整性**

检查：
- ✅ 没有旧的 Map API 保留
- ✅ 全链路使用 TapdataEvent
- ✅ 功能完整

- [ ] **Step 4: 最终提交（如有需要）**

```bash
# 确保所有变更都已提交
git status
```

---

## 验收检查

### 规范覆盖检查
✅ **目标：** 统一使用 TapdataEvent
✅ **范围：** SmartMerger, AffectedKeyCalculator, WideTableIncrementalUpdater, HazelcastDuckDbSqlNode
✅ **只保留被调用方法：** 是

### 占位符检查
无任何 "TBD", "TODO", "implement later" 等占位符

### 类型一致性检查
- MergedRecord.operations: List<TapdataEvent>
- MergedRecord.finalStateEvent: TapRecordEvent
- SmartMerger.mergeLastWins: List<TapdataEvent> → List<Map<String, Object>>
- 所有相关方法签名一致
