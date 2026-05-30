# 全链路 TapdataEvent 重构设计文档

## 1. 概述

### 1.1 目标
- 统一使用 TapdataEvent/TapRecordEvent 全链路处理 CDC 事件
- 完全移除 Map<String, Object> 格式事件的中间转换
- 只保留实际被 HazelcastDuckDbSqlNode 功能调用的方法
- 提升性能，减少内存占用，简化代码结构

### 1.2 范围
- **核心类：** SmartMerger, AffectedKeyCalculator, WideTableIncrementalUpdater
- **调用类：** HazelcastDuckDbSqlNode
- **测试文件：** 所有相关测试类（同步重构）
- **不包含：** IncrementalViewUpdater（已标记 @Deprecated）

---

## 2. 现状分析

### 2.1 当前调用链
```
HazelcastDuckDbSqlNode.flushContext()
├─> AffectedKeyCalculator.calculateAffectedBeforeKeys(List<TapdataEvent>, String)
│   └─> SmartMerger.mergeEventsSmart(List<TapdataEvent>)
├─> 写入当前表
│   └─> SmartMerger.mergeLastWins(List<Map<String, Object>>)
├─> AffectedKeyCalculator.calculateAffectedAfterKeys(List<TapdataEvent>)
│   └─> SmartMerger.mergeEventsSmart(List<TapdataEvent>)
└─> WideTableIncrementalUpdater.updateWideTableAsTapdataEvents(...)
```

### 2.2 存在的问题
1. **双重数据结构：** 同时存在 Map<String, Object> 和 TapdataEvent
2. **多余转换：** 频繁在两种格式之间转换
3. **冗余代码：** 维护两套 API 和辅助方法
4. **性能损耗：** 不必要的对象创建和内存占用

---

## 3. 详细设计

### 3.1 SmartMerger 重构

#### 3.1.1 MergedRecord 类重构
```java
public static class MergedRecord {
    private final Object initialPk;
    private Object currentPk;
    private final List<TapdataEvent> operations;  // List<Map<String, Object>> → List<TapdataEvent>
    private TapRecordEvent finalStateEvent;        // Map<String, Object> → TapRecordEvent
    private String finalOp;
    
    // 保留便捷方法
    public Map<String, Object> getFinalState() {
        return finalStateEvent != null ? TapEventUtil.getAfter(finalStateEvent) : null;
    }
}
```

#### 3.1.2 保留的方法
- ✅ `mergeEventsSmart(List<TapdataEvent>)` - 核心合并方法
- ✅ `mergeLastWins(List<TapdataEvent>)` - 重构为接受 TapdataEvent

#### 3.1.3 移除的方法
- ❌ `mergeEventsSmart(List<Map<String, Object>>)`
- ❌ `mergeLastWins(List<Map<String, Object>>)`
- ❌ `mergedRecordsToMaps(List<MergedRecord>)`
- ❌ `computeKeyForMap(Map<String, Object>)`
- ❌ `extractPkFromMap(Map<String, Object>)`
- ❌ `extractBeforePkFromMap(Map<String, Object>)`
- ❌ `extractAfterFromMap(Map<String, Object>)`
- ❌ `getRecordKey(Object, Map<Object, Object>)`

#### 3.1.4 mergeLastWins 重构实现
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

---

### 3.2 AffectedKeyCalculator 重构

#### 3.2.1 保留的方法
- ✅ `calculateAffectedBeforeKeys(List<TapdataEvent>, String)`
- ✅ `calculateAffectedAfterKeys(List<TapdataEvent>)`

#### 3.2.2 移除的方法
- ❌ `calculateAffectedKeys(String, List<Map<String, Object>>)`
- ❌ `extractBeforePrimaryKey(Map<String, Object>, String)`
- ❌ `extractAfterPrimaryKey(Map<String, Object>, String)`
- ❌ `isPrimaryKeyUpdated(Map<String, Object>, String)`
- ❌ 所有 Map 相关的私有辅助方法

#### 3.2.3 extractBeforeDataRowsFromEvents 重构
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

#### 3.2.4 extractAfterDataRowsFromEvents 重构
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

---

### 3.3 WideTableIncrementalUpdater 重构

#### 3.3.1 保留的方法
- ✅ `updateWideTableAsTapdataEvents(...)` - 保持签名，内部可能简化

#### 3.3.2 说明
这个类内部使用 TapdataEvent 已经比较统一，主要调整是适配新的数据流，保持接口不变。

---

### 3.4 HazelcastDuckDbSqlNode 重构

#### 3.4.1 flushContext 方法调整
```java
private void flushContext(PerSourceContext context) throws SQLException {
    // ... 前置代码 ...
    
    // ========== 步骤2: 写入当前Context对应表（重构） ==========
    // 直接传递 TapdataEvent，不做中间 Map 转换
    if (!eventsToFlush.isEmpty()) {
        ensureTableExists(context, eventsToFlush);  // 重构方法签名
        
        // 使用重构后的 mergeLastWins(List<TapdataEvent>)
        List<Map<String, Object>> merged = SmartMerger.mergeLastWins(eventsToFlush);
        
        // ... 后续写入代码 ...
    }
    
    // ========== 步骤4: 更新宽表（重构） ==========
    if (wideTableUpdater != null && beforeKeys != null && afterKeys != null) {
        // 直接使用 TapdataEvent，简化调用
        List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
            beforeKeys, afterKeys, eventsToFlush, mainTableName);  // 可能需要重构签名
        logger.info("更新宽表: {} 个事件", events.size());
    }
}
```

#### 3.4.2 extractAfterRowsFromEvents 移除
该方法目前用于 Map 转换，重构后应该移除或调整。

---

### 3.5 DlqWriter 适配

由于 DlqWriter 使用了 SmartMerger.MergedRecord，需要适配新的结构：
```java
// 在 write 方法中，处理 operations 从 List<Map> 变为 List<TapdataEvent>
// 可能需要添加转换逻辑或保持现有方法兼容
```

---

## 4. 实施计划

### 4.1 阶段一：SmartMerger 重构
- 重构 MergedRecord 类
- 实现新的 mergeLastWins(List<TapdataEvent>)
- 移除所有旧的 Map API
- 更新核心合并逻辑

### 4.2 阶段二：AffectedKeyCalculator 重构
- 适配新的 SmartMerger
- 重构数据提取方法
- 移除所有旧的 Map API

### 4.3 阶段三：WideTableIncrementalUpdater 调整
- 确保接口兼容性
- 简化与其他组件的交互

### 4.4 阶段四：HazelcastDuckDbSqlNode 重构
- 更新调用链
- 移除中间转换
- 适配新的 API

### 4.5 阶段五：DlqWriter 适配
- 适配新的 MergedRecord 结构
- 确保 DLQ 功能正常

### 4.6 阶段六：测试文件重构
- 更新所有使用 Map 格式的测试
- 确保测试覆盖全面
- 验证功能完整性

---

## 5. 预期收益

| 指标 | 改进幅度 | 说明 |
|------|----------|------|
| 代码复杂度 | -40% | 移除冗余代码，单一数据结构 |
| 内存占用 | -40~60% | 移除中间 Map，减少对象创建 |
| 性能提升 | +30~50% | 减少转换开销，简化逻辑 |
| 可维护性 | 显著提升 | 代码更清晰，逻辑更统一 |

---

## 6. 风险与注意事项

### 6.1 风险
- **破坏性变更：** 完全移除旧 API，需要确保所有调用方都已迁移
- **测试覆盖：** 需要全面的测试确保功能正确
- **DLQ 兼容性：** DlqWriter 需要适配新的数据结构

### 6.2 注意事项
- 保持对外接口的兼容性（如果可能）
- 分阶段进行，每阶段都要有完整测试
- 确保性能确实有所提升，没有回归

---

## 7. 验收标准

- ✅ 所有核心类重构完成，代码编译通过
- ✅ 所有 Map 相关的旧 API 已移除
- ✅ 全链路只使用 TapdataEvent
- ✅ 所有现有功能正常工作
- ✅ 性能和内存占用有所提升
- ✅ 所有测试通过
