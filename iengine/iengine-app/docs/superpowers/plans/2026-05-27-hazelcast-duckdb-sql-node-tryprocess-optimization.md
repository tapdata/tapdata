# HazelcastDuckDbSqlNode.tryProcess 优化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 优化HazelcastDuckDbSqlNode.tryProcess方法及相关类的处理逻辑，消除代码重复、简化调用链、统一CDC事件处理格式、规范import和注释。

**Architecture:** 渐进式重构方案，合并重复方法、简化调用链、将CDC事件缓冲区从Map改为TapdataEvent、修复硬编码表名、统一使用import方式、将所有注释改为中文。

**Tech Stack:** Java, Tapdata事件系统, DuckDB, Hazelcast

---

## 文件映射

### 修改文件
- `iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` (主要修改)

### 不受影响
- 所有测试文件（功能不变，仅内部重构）
- 其他duckdb包下的组件类

---

### Task 1: 添加缺失的import并删除完整包名引用

**Files:**
- Modify: `iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

- [ ] **Step 1: 添加缺失的import语句**

在文件开头的import区域添加以下import：

```java
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
```

- [ ] **Step 2: 替换所有完整包名引用为简短类名**

全局搜索并替换以下完整包名：

```java
// 替换前
io.tapdata.entity.event.dml.TapInsertRecordEvent
// 替换后
TapInsertRecordEvent

// 替换前
io.tapdata.entity.event.dml.TapUpdateRecordEvent
// 替换后
TapUpdateRecordEvent

// 替换前
io.tapdata.entity.event.dml.TapDeleteRecordEvent
// 替换后
TapDeleteRecordEvent

// 替换前
io.tapdata.entity.schema.TapField
// 替换后
TapField

// 替换前
io.tapdata.entity.schema.type.TapString
// 替换后
TapString

// 替换前
io.tapdata.entity.schema.type.TapNumber
// 替换后
TapNumber

// 替换前
io.tapdata.entity.schema.type.TapBoolean
// 替换后
TapBoolean

// 替换前
io.tapdata.entity.schema.type.TapDate
// 替换后
TapDate
```

- [ ] **Step 3: 验证编译通过**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -q`
Expected: BUILD SUCCESS

---

### Task 2: 合并processRecordEventWithStage和processRecordEvent方法

**Files:**
- Modify: `iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

- [ ] **Step 1: 删除processRecordEventWithStage方法**

删除整个 `processRecordEventWithStage` 方法（约80行代码）

- [ ] **Step 2: 删除handleOutputWithStage、handleCdcOutput、handleInitialSyncOutput方法**

删除这三个过度抽象的方法（约30行代码）

- [ ] **Step 3: 重写processRecordEvent方法，合并逻辑**

将 `processRecordEvent` 方法重写为统一方法，包含CDC物化视图处理逻辑：

```java
/**
 * 处理记录事件（统一方法）
 * 根据同步阶段决定是否执行CDC物化视图逻辑
 * 
 * @param recordEvent 记录事件
 * @param tapdataEvent 数据事件
 * @param consumer 事件消费者
 */
private void processRecordEvent(TapRecordEvent recordEvent, TapdataEvent tapdataEvent,
                                BiConsumer<TapdataEvent, ProcessResult> consumer) {
    try {
        // 步骤1: 获取表名和上下文
        String tableId = TapEventUtil.getTableId(recordEvent);
        String sourceId = resolveSourceId(tapdataEvent, recordEvent);
        if (tableId == null || tableId.isEmpty()) {
            tableId = "unknown_table";
        }
        String contextKey = buildContextKey(sourceId, tableId);
        String targetTableName = buildTargetTableName(sourceId, tableId);
        PerSourceContext context = getOrCreateContext(contextKey, targetTableName);
        currentTableName = targetTableName;

        // 步骤2: 提取记录数据并写入DuckDB缓冲区
        Map<String, Object> recordData = extractRecordData(recordEvent);
        if (recordData != null && !recordData.isEmpty()) {
            synchronized (context.getCommitLock()) {
                context.addRecord(recordData);
                // 缓冲区满则刷写
                if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                    flushContext(context);
                }
            }
        }

        // 步骤3: CDC阶段处理物化视图
        // 注意: 仅在全量同步完成后才执行增量更新
        if (syncStageTracker != null && syncStageTracker.isTransitionCompleted()
                && affectedKeyCalculator != null
                && (wideTableUpdater != null || incrementalViewUpdater != null)) {
            processCdcEventForMaterializedView(tapdataEvent);
        }

        // 步骤4: 透传事件到下游
        consumer.accept(tapdataEvent, null);

    } catch (Exception e) {
        // 异常处理: 记录错误并写入DLQ
        logger.error("处理记录事件失败: {}", e.getMessage(), e);

        if (errorHandler != null) {
            try {
                Map<String, Object> sourceData = extractRecordData(recordEvent);
                errorHandler.recordError(sourceData != null ? sourceData : new HashMap<>(), e);

                // 写入死信队列
                if (dlqWriter != null && sourceData != null) {
                    try {
                        String tableId = TapEventUtil.getTableId(recordEvent);
                        String sourceId = resolveSourceId(tapdataEvent, recordEvent);
                        String contextKey = buildContextKey(sourceId, tableId);
                        dlqWriter.write(contextKey, buildTargetTableName(sourceId, tableId),
                                Collections.singletonList(sourceData), e);
                    } catch (Exception dlqError) {
                        logger.warn("写入DLQ失败: {}", dlqError.getMessage());
                    }
                }
            } catch (Exception handlerError) {
                logger.warn("记录错误信息失败: {}", handlerError.getMessage());
            }
        }

        // 处理失败时仍然透传事件
        consumer.accept(tapdataEvent, null);
    }
}
```

- [ ] **Step 4: 更新tryProcess方法调用**

将 `tryProcess` 方法中的调用从 `processRecordEventWithStage` 改为 `processRecordEvent`：

```java
// 替换前
processRecordEventWithStage((TapRecordEvent) tapEvent, tapdataEvent, consumer);

// 替换后
processRecordEvent((TapRecordEvent) tapEvent, tapdataEvent, consumer);
```

- [ ] **Step 5: 验证编译通过**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -q`
Expected: BUILD SUCCESS

---

### Task 3: 重构CDC事件处理为TapdataEvent格式

**Files:**
- Modify: `iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

- [ ] **Step 1: 修改cdcEventBuffer类型**

```java
// 替换前
private List<Map<String, Object>> cdcEventBuffer = new ArrayList<>();

// 替换后
private List<TapdataEvent> cdcEventBuffer = new ArrayList<>();
```

- [ ] **Step 2: 重写processCdcEventForMaterializedView方法**

```java
/**
 * 处理CDC事件用于物化视图
 * 直接将TapdataEvent添加到缓冲区，避免Map中间格式转换
 * 
 * @param tapdataEvent 数据事件
 */
private void processCdcEventForMaterializedView(TapdataEvent tapdataEvent) {
    try {
        // 直接存储TapdataEvent，保留完整类型信息
        cdcEventBuffer.add(tapdataEvent);

        // 缓冲区满则触发批量更新
        if (cdcEventBuffer.size() >= CDC_BUFFER_SIZE) {
            flushCdcBuffer();
        }
    } catch (Exception e) {
        logger.error("处理CDC物化视图事件失败: {}", e.getMessage(), e);
    }
}
```

- [ ] **Step 3: 重写flushCdcBuffer方法，从TapdataEvent提取tableId**

```java
/**
 * 刷新CDC事件缓冲区并更新宽表
 * 从TapdataEvent中提取tableId进行分组处理
 */
private void flushCdcBuffer() {
    if (cdcEventBuffer.isEmpty()) {
        return;
    }

    try {
        logger.info("刷新CDC缓冲区: {} 个事件", cdcEventBuffer.size());

        // 从TapdataEvent提取tableId进行分组
        Map<String, List<TapdataEvent>> eventsByTable = new HashMap<>();
        for (TapdataEvent event : cdcEventBuffer) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                String tableId = TapEventUtil.getTableId((TapRecordEvent) tapEvent);
                if (tableId != null) {
                    eventsByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(event);
                }
            }
        }

        if (DuckDbSqlConfig.isUseNewWideTableUpdater() && wideTableUpdater != null) {
            // 新组件: 使用updateWideTableAsTapdataEvents
            flushCdcBufferWithNewComponent(eventsByTable);
        } else if (incrementalViewUpdater != null) {
            // 旧组件: 使用updateWideTable
            flushCdcBufferWithOldComponent(eventsByTable);
        }

        // 清空缓冲区
        cdcEventBuffer.clear();
    } catch (Exception e) {
        logger.error("刷新CDC缓冲区失败: {}", e.getMessage(), e);
    }
}
```

- [ ] **Step 4: 重写flushCdcBufferWithNewComponent方法，修复硬编码表名**

```java
/**
 * 使用新组件刷新CDC缓冲区
 * 修复: 使用配置的mainTableName替代硬编码"users"
 */
private void flushCdcBufferWithNewComponent(Map<String, List<TapdataEvent>> eventsByTable) {
    try {
        // 将TapdataEvent转换为Map格式供AffectedKeyCalculator使用
        Map<String, List<Map<String, Object>>> eventsByTableMap = convertTapdataEventsToMaps(eventsByTable);

        // 计算before/after主键
        Set<Object> beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsByTableMap);
        Set<Object> afterKeys = affectedKeyCalculator.calculateAffectedAfterKeys(eventsByTableMap);

        // 提取after数据行
        List<Map<String, Object>> afterRows = extractAfterRowsFromTapdataEvents(eventsByTable);

        // 执行宽表更新 - 使用配置的mainTableName替代硬编码"users"
        List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, afterRows, mainTableName);

        logger.info("更新宽表: {} 个事件 (新组件)", events.size());
    } catch (Exception e) {
        logger.error("使用新组件刷新CDC缓冲区失败: {}", e.getMessage(), e);
    }
}
```

- [ ] **Step 5: 重写flushCdcBufferWithOldComponent方法**

```java
/**
 * 使用旧组件刷新CDC缓冲区
 */
private void flushCdcBufferWithOldComponent(Map<String, List<TapdataEvent>> eventsByTable) {
    try {
        // 将TapdataEvent转换为Map格式
        Map<String, List<Map<String, Object>>> eventsByTableMap = convertTapdataEventsToMaps(eventsByTable);

        // 对每个表计算受影响的主键并更新宽表
        for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTableMap.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> events = entry.getValue();

            // 计算受影响的宽表主键
            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys(tableName, events);

            if (!affectedKeys.isEmpty()) {
                // 批量更新宽表
                int updatedRows = incrementalViewUpdater.updateWideTable(affectedKeys);
                logger.info("更新宽表: {} 行, 表: {}, 受影响主键: {} 个 (旧组件)",
                        updatedRows, tableName, affectedKeys.size());
            }
        }
    } catch (Exception e) {
        logger.error("使用旧组件刷新CDC缓冲区失败: {}", e.getMessage(), e);
    }
}
```

- [ ] **Step 6: 添加TapdataEvent转Map的辅助方法**

```java
/**
 * 将TapdataEvent列表转换为Map格式
 * 用于兼容AffectedKeyCalculator的输入要求
 * 
 * @param eventsByTable 按表分组的TapdataEvent
 * @return 按表分组的Map格式事件
 */
private Map<String, List<Map<String, Object>>> convertTapdataEventsToMaps(
        Map<String, List<TapdataEvent>> eventsByTable) {
    Map<String, List<Map<String, Object>>> result = new HashMap<>();
    
    for (Map.Entry<String, List<TapdataEvent>> entry : eventsByTable.entrySet()) {
        String tableId = entry.getKey();
        List<TapdataEvent> events = entry.getValue();
        List<Map<String, Object>> mapEvents = new ArrayList<>();
        
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
                Map<String, Object> mapEvent = new HashMap<>();
                
                // 提取op类型
                if (tapEvent instanceof TapInsertRecordEvent) {
                    mapEvent.put("op", "INSERT");
                    mapEvent.put("record", TapEventUtil.getAfter(recordEvent));
                } else if (tapEvent instanceof TapUpdateRecordEvent) {
                    mapEvent.put("op", "UPDATE");
                    mapEvent.put("record", TapEventUtil.getAfter(recordEvent));
                    mapEvent.put("before", TapEventUtil.getBefore(recordEvent));
                } else if (tapEvent instanceof TapDeleteRecordEvent) {
                    mapEvent.put("op", "DELETE");
                    mapEvent.put("record", TapEventUtil.getBefore(recordEvent));
                }
                
                mapEvents.add(mapEvent);
            }
        }
        
        result.put(tableId, mapEvents);
    }
    
    return result;
}
```

- [ ] **Step 7: 重写extractAfterRowsFromBuffer方法**

```java
/**
 * 从TapdataEvent中提取after数据行
 * 
 * @param eventsByTable 按表分组的TapdataEvent
 * @return after数据行列表
 */
private List<Map<String, Object>> extractAfterRowsFromTapdataEvents(
        Map<String, List<TapdataEvent>> eventsByTable) {
    List<Map<String, Object>> afterRows = new ArrayList<>();
    
    for (List<TapdataEvent> events : eventsByTable.values()) {
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                Map<String, Object> after = TapEventUtil.getAfter((TapRecordEvent) tapEvent);
                if (after != null && !after.isEmpty()) {
                    afterRows.add(after);
                }
            }
        }
    }
    
    return afterRows;
}
```

- [ ] **Step 8: 删除旧的extractAfterRowsFromBuffer方法**

删除原 `extractAfterRowsFromBuffer(Map<String, List<Map<String, Object>>> eventsByTable)` 方法

- [ ] **Step 9: 验证编译通过**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -q`
Expected: BUILD SUCCESS

---

### Task 4: 更新tryProcess方法注释为中文并优化结构

**Files:**
- Modify: `iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

- [ ] **Step 1: 重写tryProcess方法，添加详细中文注释**

```java
/**
 * 处理数据事件的核心方法
 * 负责事件分类、错误监控、同步阶段追踪和事件分发
 * 
 * @param tapdataEvent 数据事件
 * @param consumer 事件消费者
 */
@Override
protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
    // 保存当前消费者用于发送待处理事件
    currentConsumer = consumer;

    // 步骤1: 发送待处理事件
    emitPendingEvents();

    // 步骤2: 空事件检查
    if (tapdataEvent == null) {
        return;
    }

    TapEvent tapEvent = tapdataEvent.getTapEvent();

    // 步骤3: 错误率监控
    if (errorHandler != null) {
        errorHandler.recordEvent();

        // 检查是否超过错误阈值需要停止任务
        if (errorHandler.shouldStopTask()) {
            logger.error("超过错误阈值，任务应停止");
            return;
        }
    }

    // 步骤4: 同步阶段追踪
    trackSyncStage(tapdataEvent);

    // 步骤5: 事件分类处理
    if (tapEvent instanceof TapRecordEvent) {
        // DML事件: 统一处理
        processRecordEvent((TapRecordEvent) tapEvent, tapdataEvent, consumer);
    } else {
        // 非DML事件: 直接透传
        consumer.accept(tapdataEvent, null);
    }
}
```

- [ ] **Step 2: 更新emitPendingEvents方法注释**

```java
/**
 * 发送所有待处理事件
 * 用于将查询结果等延迟发送的事件传递给下游
 */
private void emitPendingEvents() {
    if (currentConsumer != null && !pendingEvents.isEmpty()) {
        TapdataEvent event;
        while ((event = pendingEvents.poll()) != null) {
            try {
                currentConsumer.accept(event, null);
            } catch (Exception e) {
                logger.warn("发送待处理事件失败: {}", e.getMessage());
            }
        }
    }
}
```

- [ ] **Step 3: 更新trackSyncStage方法注释**

```java
/**
 * 追踪数据事件的同步阶段
 * 记录每个表当前所处的同步阶段(全量/CDC)
 * 
 * @param tapdataEvent 数据事件
 */
private void trackSyncStage(TapdataEvent tapdataEvent) {
    if (syncStageTracker != null && tapdataEvent != null) {
        SyncStage stage = tapdataEvent.getSyncStage();
        TapEvent tapEvent = tapdataEvent.getTapEvent();

        if (tapEvent instanceof TapRecordEvent) {
            String tableName = TapEventUtil.getTableId((TapRecordEvent) tapEvent);
            if (tableName != null) {
                syncStageTracker.updateTableStageFromEvent(tableName, stage);
            }
        }
    }
}
```

- [ ] **Step 4: 更新所有其他方法的英文注释为中文**

包括但不限于：
- `handleAllTablesCdcTransition()`
- `executeAndEmitQueryResults()`
- `emitResultAsTapEvent()`
- `extractRecordData()`
- `flushBatch()`
- `flushContext()`
- `ensureTableExists()`
- `inferTapTable()`
- `inferTapType()`
- `inferDataType()`
- 所有TapType创建辅助方法
- 所有上下文管理方法

- [ ] **Step 5: 验证编译通过**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -q`
Expected: BUILD SUCCESS

---

### Task 5: 运行测试并验证

**Files:**
- Test: `iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeTest.java`
- Test: `iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeMultiSourceTest.java`
- Test: `iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNodeIntegrationTest.java`

- [ ] **Step 1: 运行单元测试**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=HazelcastDuckDbSqlNodeTest -q`
Expected: BUILD SUCCESS, 所有测试通过

- [ ] **Step 2: 运行多源测试**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=HazelcastDuckDbSqlNodeMultiSourceTest -q`
Expected: BUILD SUCCESS, 所有测试通过

- [ ] **Step 3: 运行集成测试**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=HazelcastDuckDbSqlNodeIntegrationTest -q`
Expected: BUILD SUCCESS, 所有测试通过

- [ ] **Step 4: 提交代码**

```bash
cd /Users/hj/workspace/tapdata/iengine/iengine-app
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "refactor: 优化HazelcastDuckDbSqlNode.tryProcess处理逻辑

- 合并processRecordEventWithStage和processRecordEvent方法，消除代码重复
- 删除handleOutputWithStage等过度抽象层，简化调用链
- CDC事件缓冲区从Map改为TapdataEvent，保留完整类型信息
- 修复flushCdcBufferWithNewComponent中硬编码'users'表名问题
- 统一使用import方式，删除所有完整包名引用
- 所有英文注释改为中文，关键代码添加详细中文注释"
```

---

## 自审检查

### 1. 规范覆盖
✅ 调用链重构 - Task 2
✅ CDC事件处理重构 - Task 3
✅ Import规范 - Task 1
✅ 注释优化 - Task 4
✅ 测试验证 - Task 5

### 2. 占位符扫描
✅ 无TBD/TODO
✅ 所有步骤包含完整代码
✅ 所有命令包含预期输出

### 3. 类型一致性
✅ TapdataEvent、TapRecordEvent等类型在所有任务中一致
✅ Map<String, List<TapdataEvent>> 和 Map<String, List<Map<String, Object>>> 转换逻辑清晰
✅ mainTableName字段使用一致
