# 统一TapdataEvent处理设计文档

> 重构HazelcastDuckDbSqlNode.tryProcess调用链，统一使用TapRecordEvent，移除convertTapdataEventToMap转换

**版本**：V1.0
**日期**：2026-05-27
**状态**：设计中

---

## 1. 项目背景

### 1.1 当前问题
- `AffectedKeyCalculator.calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys` 内部将 `TapRecordEvent` 转换为 `Map<String, Object>` 格式
- `SmartMerger` 只接受 `Map<String, Object>` 输入，导致不必要的类型转换
- 存在多次对象创建和复制，性能开销较大
- 代码可读性和维护性较差

### 1.2 目标
- **统一事件处理**：完全使用 `TapdataEvent`，移除所有 `convertTapdataEventToMap` 调用
- **性能优化**：减少对象创建、简化数据结构转换、优化内存占用
- **保持功能**：不破坏现有逻辑和功能

### 1.3 改造范围
1. **SmartMerger** - 完全重写，支持 `List<TapdataEvent>` 输入
2. **AffectedKeyCalculator** - 重构，直接处理 `TapdataEvent` 集合
3. **HazelcastDuckDbSqlNode** - 适配新的调用链
4. **WideTableIncrementalUpdater** - 确保与新接口兼容（已使用标准事件）

---

## 2. 核心设计

### 2.1 整体架构图

```
TapdataEvent 输入
    ↓
HazelcastDuckDbSqlNode.tryProcess
    ↓
AffectedKeyCalculator.calculateAffectedBeforeKeys
    ↓
SmartMerger.mergeEventsSmart (直接处理 TapdataEvent)
    ↓
WideTableIncrementalUpdater.updateWideTableAsTapdataEvents
    ↓
TapdataEvent 输出
```

### 2.2 SmartMerger 重写方案

#### 新增核心方法
```java
/**
 * 高级智能合并，处理 TapdataEvent 输入
 */
public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents)

/**
 * 从 TapdataEvent 中提取数据用于处理
 */
private static void processEvent(TapdataEvent tapEvent, Map<Object, MergedRecord> mergedRecords, Map<Object, Object> pkMigration)
```

#### MergedRecord 增强
```java
public static class MergedRecord {
    // 保留现有字段
    private Object initialPk;
    private Object currentPk;
    private List<Map<String, Object>> operations;
    private Map<String, Object> finalState;
    private String finalOp;
    
    // 新增：关联原始 TapdataEvent（可选，用于追踪）
    private List<TapdataEvent> sourceEvents;
    
    // 保留现有方法
    // ...
}
```

#### 辅助方法
```java
/**
 * 从 TapRecordEvent 中提取主键值
 */
private static Object extractPrimaryKey(TapRecordEvent recordEvent, String... pkCandidates)

/**
 * 从 TapRecordEvent 中提取 before 数据
 */
private static Map<String, Object> extractBeforeData(TapRecordEvent recordEvent)

/**
 * 从 TapRecordEvent 中提取 after 数据
 */
private static Map<String, Object> extractAfterData(TapRecordEvent recordEvent)

/**
 * 从 TapRecordEvent 中提取更新的字段
 */
private static Map<String, Object> extractUpdatedFields(TapRecordEvent recordEvent)
```

#### 保留向后兼容
- 保留原有的 `mergeEventsSmart(List<Map<String, Object>> events)` 方法
- 标记为 `@Deprecated`，但继续工作

---

### 2.3 AffectedKeyCalculator 重构方案

#### 核心修改
```java
/**
 * 计算 before 受影响主键集合（直接处理 TapdataEvent）
 */
public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException

/**
 * 计算 after 受影响主键集合（直接处理 TapdataEvent）
 */
public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException

// 移除 convertTapdataEventToMap 方法
```

#### 内部方法调整
```java
/**
 * 从原始 TapdataEvent 中提取 DELETE 事件的 before 数据
 */
private List<Map<String, Object>> extractDeleteBeforeRowsFromEvents(List<TapdataEvent> events, String tableName)

/**
 * 从 SmartMerger 合并结果中提取 before 数据行
 */
private List<Map<String, Object>> extractBeforeDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords, String tableName)

/**
 * 从 SmartMerger 合并结果中提取 after 数据行
 */
private List<Map<String, Object>> extractAfterDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords)
```

---

### 2.4 HazelcastDuckDbSqlNode 适配

#### 无需重大改动
- 已经在使用 `List<TapdataEvent>` 调用 `AffectedKeyCalculator`
- 内部调用链会自动适配新的实现

---

### 2.5 性能优化策略

| 优化项 | 方案 | 预期提升 |
|-------|------|--------|
| 对象创建减少 | 直接使用 TapRecordEvent 的数据引用，避免 Map 复制 | 30-50% 内存占用减少 |
| 数据结构简化 | 移除中间 Map 转换层 | 20-40% 处理速度提升 |
| GC压力缓解 | 减少临时对象创建 | GC 频率降低 |

---

## 3. 详细设计

### 3.1 SmartMerger 完整接口定义

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SmartMerger {
    
    private static final Logger logger = LoggerFactory.getLogger(SmartMerger.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> PK_CANDIDATES = Arrays.asList("_id", "id", "pk", "PK", "ID");
    private static final String INSERT_OP = "INSERT";
    private static final String UPDATE_OP = "UPDATE";
    private static final String DELETE_OP = "DELETE";
    private static final String DELETE_INSERT_OP = "DELETE_INSERT";

    // ==================== MergedRecord 内部类 ====================
    public static class MergedRecord {
        private Object initialPk;
        private Object currentPk;
        private List<Map<String, Object>> operations;
        private Map<String, Object> finalState;
        private String finalOp;
        private List<TapdataEvent> sourceEvents; // 新增：关联原始事件

        public MergedRecord(Object initialPk, Object currentPk) {
            this.initialPk = initialPk;
            this.currentPk = currentPk;
            this.operations = new ArrayList<>();
            this.finalState = new HashMap<>();
            this.finalOp = INSERT_OP;
            this.sourceEvents = new ArrayList<>();
        }

        // Getters and Setters
        public Object getInitialPk() { return initialPk; }
        public Object getCurrentPk() { return currentPk; }
        public void setCurrentPk(Object currentPk) { this.currentPk = currentPk; }
        public List<Map<String, Object>> getOperations() { return operations; }
        public Map<String, Object> getFinalState() { return finalState; }
        public void setFinalState(Map<String, Object> finalState) { this.finalState = finalState; }
        public String getFinalOp() { return finalOp; }
        public void setFinalOp(String finalOp) { this.finalOp = finalOp; }
        public List<TapdataEvent> getSourceEvents() { return sourceEvents; }
        public void addSourceEvent(TapdataEvent event) { if (event != null) this.sourceEvents.add(event); }
    }

    // ==================== 新方法：直接处理 TapdataEvent ====================

    /**
     * 高级智能合并：处理 TapdataEvent 输入
     */
    public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents) {
        if (tapEvents == null || tapEvents.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Object, MergedRecord> mergedRecords = new LinkedHashMap<>();
        Map<Object, Object> pkMigration = new HashMap<>();

        for (TapdataEvent tapEvent : tapEvents) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent) {
                processEvent(tapEvent, (TapRecordEvent) tapEventInner, mergedRecords, pkMigration);
            }
        }

        // 后处理：如果 final_pk == initial_pk，将 DELETE_INSERT 改回 UPDATE
        for (MergedRecord record : mergedRecords.values()) {
            if (DELETE_INSERT_OP.equals(record.getFinalOp()) &&
                Objects.equals(record.getInitialPk(), record.getCurrentPk())) {
                record.setFinalOp(UPDATE_OP);
            }
        }

        return new ArrayList<>(mergedRecords.values());
    }

    /**
     * 处理单个 TapdataEvent
     */
    private static void processEvent(TapdataEvent tapEvent, TapRecordEvent recordEvent,
                                     Map<Object, MergedRecord> mergedRecords,
                                     Map<Object, Object> pkMigration) {
        // 判断操作类型
        String opType;
        if (recordEvent instanceof TapInsertRecordEvent) {
            opType = INSERT_OP;
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            opType = UPDATE_OP;
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            opType = DELETE_OP;
        } else {
            return; // 未知类型，跳过
        }

        switch (opType) {
            case INSERT_OP:
                processInsert(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
            case UPDATE_OP:
                processUpdate(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
            case DELETE_OP:
                processDelete(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
        }
    }

    /**
     * 处理 INSERT 事件
     */
    private static void processInsert(TapdataEvent tapEvent, TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object initialPk = extractPrimaryKey(recordEvent);
        if (initialPk == null) {
            String key = computeKey(recordEvent);
            mergedRecords.put(key, new MergedRecord(key, key));
            return;
        }

        MergedRecord record = new MergedRecord(initialPk, initialPk);
        record.addSourceEvent(tapEvent);
        record.getOperations().add(Map.of("op", INSERT_OP, "value", TapEventUtil.getAfter(recordEvent)));
        
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            record.setFinalState(new HashMap<>(after));
        }
        record.setFinalOp(INSERT_OP);

        mergedRecords.put(initialPk, record);
        pkMigration.put(initialPk, initialPk);
    }

    /**
     * 处理 UPDATE 事件
     */
    private static void processUpdate(TapdataEvent tapEvent, TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractBeforePrimaryKey(recordEvent);
        if (oldPk == null) {
            oldPk = extractPrimaryKey(recordEvent);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        // 提取更新/删除的字段
        Map<String, Object> updatedFields = TapEventUtil.getAfter(recordEvent);
        Map<String, Object> beforeData = TapEventUtil.getBefore(recordEvent);

        // 检查主键是否变更
        Object newPk = extractAfterPrimaryKey(recordEvent);
        if (newPk == null) {
            newPk = oldPk;
        }

        Map<String, Object> op = new HashMap<>();
        op.put("op", UPDATE_OP);
        if (!Objects.equals(oldPk, newPk)) {
            op.put("old_pk", oldPk);
            op.put("new_pk", newPk);
        }
        if (updatedFields != null) {
            op.put("fields", new HashMap<>(updatedFields));
        }
        if (beforeData != null) {
            op.put("before", new HashMap<>(beforeData));
        }

        record.getOperations().add(op);
        record.addSourceEvent(tapEvent);

        // 更新主键迁移
        if (!Objects.equals(oldPk, newPk)) {
            pkMigration.remove(oldPk);
            pkMigration.put(newPk, initialPk);
            record.setCurrentPk(newPk);
            record.setFinalOp(DELETE_INSERT_OP);
        } else {
            record.setFinalOp(UPDATE_OP);
        }

        // 应用字段更新到 final_state
        if (updatedFields != null) {
            record.getFinalState().putAll(updatedFields);
        }
    }

    /**
     * 处理 DELETE 事件
     */
    private static void processDelete(TapdataEvent tapEvent, TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractPrimaryKey(recordEvent);
        if (oldPk == null) {
            oldPk = extractBeforePrimaryKey(recordEvent);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        record.getOperations().add(Map.of("op", DELETE_OP));
        record.addSourceEvent(tapEvent);
        record.setFinalOp(DELETE_OP);

        pkMigration.remove(oldPk);
        mergedRecords.remove(initialPk);
    }

    // ==================== 辅助方法 ====================

    /**
     * 从 TapRecordEvent 中提取主键值
     */
    private static Object extractPrimaryKey(TapRecordEvent recordEvent) {
        // 先尝试从 after 中获取
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = after.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        // 再尝试从 before 中获取
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = before.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 从 before 数据中提取主键
     */
    private static Object extractBeforePrimaryKey(TapRecordEvent recordEvent) {
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = before.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 从 after 数据中提取主键
     */
    private static Object extractAfterPrimaryKey(TapRecordEvent recordEvent) {
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = after.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 计算后备键（当没有主键时使用）
     */
    private static String computeKey(TapRecordEvent recordEvent) {
        Map<String, Object> data = TapEventUtil.getAfter(recordEvent);
        if (data == null) {
            data = TapEventUtil.getBefore(recordEvent);
        }
        if (data == null) {
            return "_hash:" + System.identityHashCode(recordEvent);
        }
        for (String pk : PK_CANDIDATES) {
            if (data.containsKey(pk)) {
                Object v = data.get(pk);
                if (v != null) return pk + ":" + v.toString();
            }
        }
        try {
            return "_full:" + MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            return "_hash:" + System.identityHashCode(recordEvent);
        }
    }

    // ==================== 向后兼容方法 ====================

    /**
     * 简单 last-wins 合并（保留）
     * @deprecated 使用新的 TapdataEvent 版本
     */
    @Deprecated
    public static List<Map<String, Object>> mergeLastWins(List<Map<String, Object>> events) {
        if (events == null || events.isEmpty()) return Collections.emptyList();
        Map<String, Map<String, Object>> lastByKey = new LinkedHashMap<>();
        for (Map<String, Object> ev : events) {
            String key = computeKeyFromMap(ev);
            lastByKey.put(key, ev);
        }
        return new ArrayList<>(lastByKey.values());
    }

    /**
     * 高级智能合并（保留向后兼容）
     * @deprecated 使用新的 TapdataEvent 版本
     */
    @Deprecated
    public static List<MergedRecord> mergeEventsSmart(List<Map<String, Object>> events) {
        if (events == null || events.isEmpty()) return Collections.emptyList();
        Map<Object, MergedRecord> mergedRecords = new LinkedHashMap<>();
        Map<Object, Object> pkMigration = new HashMap<>();
        for (Map<String, Object> ev : events) {
            processEventFromMap(ev, mergedRecords, pkMigration);
        }
        for (MergedRecord record : mergedRecords.values()) {
            if (DELETE_INSERT_OP.equals(record.getFinalOp()) &&
                Objects.equals(record.getInitialPk(), record.getCurrentPk())) {
                record.setFinalOp(UPDATE_OP);
            }
        }
        return new ArrayList<>(mergedRecords.values());
    }

    /**
     * 将 MergedRecord 转换为 Map 列表（保留）
     */
    public static List<Map<String, Object>> mergedRecordsToMaps(List<MergedRecord> mergedRecords) {
        if (mergedRecords == null || mergedRecords.isEmpty()) return Collections.emptyList();
        return mergedRecords.stream()
                .map(MergedRecord::getFinalState)
                .collect(Collectors.toList());
    }

    // ==================== 辅助方法（向后兼容） ====================

    @Deprecated
    private static String computeKeyFromMap(Map<String, Object> ev) {
        // 原有实现
        for (String pk : PK_CANDIDATES) {
            if (ev.containsKey(pk)) {
                Object v = ev.get(pk);
                if (v != null) return pk + ":" + v.toString();
            }
        }
        try {
            return "_full:" + MAPPER.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            return "_hash:" + System.identityHashCode(ev);
        }
    }

    @Deprecated
    private static void processEventFromMap(Map<String, Object> ev,
                                            Map<Object, MergedRecord> mergedRecords,
                                            Map<Object, Object> pkMigration) {
        // 原有实现
        String opType = (String) ev.get("op");
        if (opType == null) {
            if (ev.containsKey("updatedFields") || ev.containsKey("o2")) {
                opType = UPDATE_OP;
            } else {
                opType = INSERT_OP;
            }
        }
        switch (opType.toUpperCase()) {
            case INSERT_OP:
                processInsertFromMap(ev, mergedRecords, pkMigration);
                break;
            case UPDATE_OP:
                processUpdateFromMap(ev, mergedRecords, pkMigration);
                break;
            case DELETE_OP:
                processDeleteFromMap(ev, mergedRecords, pkMigration);
                break;
            default:
                processInsertFromMap(ev, mergedRecords, pkMigration);
        }
    }

    @Deprecated
    private static void processInsertFromMap(Map<String, Object> ev,
                                             Map<Object, MergedRecord> mergedRecords,
                                             Map<Object, Object> pkMigration) {
        // 原有实现
        Object initialPk = extractPkFromMap(ev);
        if (initialPk == null) {
            String key = computeKeyFromMap(ev);
            mergedRecords.put(key, new MergedRecord(key, key));
            return;
        }
        MergedRecord record = new MergedRecord(initialPk, initialPk);
        record.getOperations().add(Map.of("op", INSERT_OP, "value", ev));
        record.setFinalState(new HashMap<>(ev));
        record.setFinalOp(INSERT_OP);
        mergedRecords.put(initialPk, record);
        pkMigration.put(initialPk, initialPk);
    }

    @Deprecated
    private static void processUpdateFromMap(Map<String, Object> ev,
                                             Map<Object, MergedRecord> mergedRecords,
                                             Map<Object, Object> pkMigration) {
        // 原有实现
        Object oldPk = extractOldPkFromMap(ev);
        if (oldPk == null) oldPk = extractPkFromMap(ev);
        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }
        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;
        Map<String, Object> updatedFields = extractUpdatedFieldsFromMap(ev);
        Object newPk = extractNewPkFromMap(ev, updatedFields, oldPk);
        Map<String, Object> op = new HashMap<>();
        op.put("op", UPDATE_OP);
        if (!Objects.equals(oldPk, newPk)) {
            op.put("old_pk", oldPk);
            op.put("new_pk", newPk);
        }
        if (updatedFields != null) {
            op.put("fields", updatedFields);
        }
        record.getOperations().add(op);
        if (!Objects.equals(oldPk, newPk)) {
            pkMigration.remove(oldPk);
            pkMigration.put(newPk, initialPk);
            record.setCurrentPk(newPk);
            record.setFinalOp(DELETE_INSERT_OP);
        } else {
            record.setFinalOp(UPDATE_OP);
        }
        if (updatedFields != null) {
            record.getFinalState().putAll(updatedFields);
        }
    }

    @Deprecated
    private static void processDeleteFromMap(Map<String, Object> ev,
                                             Map<Object, MergedRecord> mergedRecords,
                                             Map<Object, Object> pkMigration) {
        // 原有实现
        Object oldPk = extractPkFromMap(ev);
        if (oldPk == null) return;
        if (!pkMigration.containsKey(oldPk)) {
            return;
        }
        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;
        record.getOperations().add(Map.of("op", DELETE_OP));
        record.setFinalOp(DELETE_OP);
        pkMigration.remove(oldPk);
        mergedRecords.remove(initialPk);
    }

    @Deprecated
    private static Object extractPkFromMap(Map<String, Object> ev) {
        // 原有实现
        for (String pkCandidate : PK_CANDIDATES) {
            if (ev.containsKey(pkCandidate)) {
                return ev.get(pkCandidate);
            }
        }
        Object o2 = ev.get("o2");
        if (o2 instanceof Map) {
            return extractPkFromMap((Map<String, Object>) o2);
        }
        Object o = ev.get("o");
        if (o instanceof Map) {
            return extractPkFromMap((Map<String, Object>) o);
        }
        return null;
    }

    @Deprecated
    private static Object extractOldPkFromMap(Map<String, Object> ev) {
        // 原有实现
        Object o2 = ev.get("o2");
        if (o2 instanceof Map) {
            return extractPkFromMap((Map<String, Object>) o2);
        }
        return null;
    }

    @Deprecated
    private static Object extractNewPkFromMap(Map<String, Object> ev, Map<String, Object> updatedFields, Object defaultPk) {
        // 原有实现
        if (updatedFields == null) return defaultPk;
        for (String pkCandidate : PK_CANDIDATES) {
            if (updatedFields.containsKey(pkCandidate)) {
                return updatedFields.get(pkCandidate);
            }
        }
        return defaultPk;
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractUpdatedFieldsFromMap(Map<String, Object> ev) {
        // 原有实现
        Object updated = ev.get("updatedFields");
        if (updated instanceof Map) {
            return new HashMap<>((Map<String, Object>) updated);
        }
        Map<String, Object> result = new HashMap<>(ev);
        PK_CANDIDATES.forEach(result::remove);
        return result;
    }
}
```

---

### 3.2 AffectedKeyCalculator 重构后接口定义

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class AffectedKeyCalculator {

    private static final Logger logger = LoggerFactory.getLogger(AffectedKeyCalculator.class);

    // ... 保留现有字段 ...

    // ==================== 核心重构方法 ====================

    /**
     * 计算 before 受影响主键集合（直接处理 TapdataEvent）
     */
    public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }

        // 按表名分组
        Map<String, List<TapdataEvent>> eventsByTable = groupEventsByTable(events);

        return calculateAffectedBeforeKeysInternal(eventsByTable);
    }

    /**
     * 计算 after 受影响主键集合（直接处理 TapdataEvent）
     */
    public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }

        // 按表名分组
        Map<String, List<TapdataEvent>> eventsByTable = groupEventsByTable(events);

        return calculateAffectedAfterKeysInternal(eventsByTable);
    }

    // ==================== 内部实现方法 ====================

    /**
     * 将 TapdataEvent 按表名分组
     */
    private Map<String, List<TapdataEvent>> groupEventsByTable(List<TapdataEvent> events) {
        Map<String, List<TapdataEvent>> eventsByTable = new LinkedHashMap<>();
        for (TapdataEvent tapEvent : events) {
            TapEvent innerEvent = tapEvent.getTapEvent();
            if (innerEvent instanceof TapRecordEvent) {
                TapRecordEvent recordEvent = (TapRecordEvent) innerEvent;
                String tableName = TapEventUtil.getTableId(recordEvent);
                if (tableName != null) {
                    eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tapEvent);
                }
            }
        }
        return eventsByTable;
    }

    /**
     * 内部方法：计算 before 主键集合
     */
    private Set<Object> calculateAffectedBeforeKeysInternal(Map<String, List<TapdataEvent>> eventsByTable) throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();

        for (Map.Entry<String, List<TapdataEvent>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<TapdataEvent> events = entry.getValue();

            if (events == null || events.isEmpty()) {
                continue;
            }

            // 1. 先收集 DELETE 事件的 before 数据
            List<Map<String, Object>> deleteBeforeRows = extractDeleteBeforeRowsFromEvents(events, tableName);

            // 2. 使用 SmartMerger 合并事件（新 API）
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);

            // 3. 提取所有 before 数据行
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> beforeRows = extractBeforeDataRowsFromEvents(mergedRecords, tableName);

            // 4. 合并 DELETE 的 before 数据
            beforeRows.addAll(deleteBeforeRows);

            if (beforeRows.isEmpty()) {
                continue;
            }

            // 5. 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
            affectedBeforeKeys.addAll(wideTablePks);
        }

        return affectedBeforeKeys;
    }

    /**
     * 内部方法：计算 after 主键集合
     */
    private Set<Object> calculateAffectedAfterKeysInternal(Map<String, List<TapdataEvent>> eventsByTable) throws SQLException {
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        for (Map.Entry<String, List<TapdataEvent>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<TapdataEvent> events = entry.getValue();

            if (events == null || events.isEmpty()) {
                continue;
            }

            // 1. 使用 SmartMerger 合并事件（新 API）
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
            if (mergedRecords.isEmpty()) {
                continue;
            }

            // 2. 提取所有 after 数据行
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> afterRows = extractAfterDataRowsFromEvents(mergedRecords);

            if (afterRows.isEmpty()) {
                continue;
            }

            // 3. 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, afterRows, fields);
            affectedAfterKeys.addAll(wideTablePks);
        }

        return affectedAfterKeys;
    }

    /**
     * 从 TapdataEvent 列表中提取 DELETE 事件的 before 数据
     */
    private List<Map<String, Object>> extractDeleteBeforeRowsFromEvents(List<TapdataEvent> events, String tableName) {
        List<Map<String, Object>> deleteBeforeRows = new ArrayList<>();

        for (TapdataEvent tapEvent : events) {
            TapEvent innerEvent = tapEvent.getTapEvent();
            if (innerEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) innerEvent;
                Map<String, Object> before = TapEventUtil.getBefore(deleteEvent);
                if (before != null) {
                    deleteBeforeRows.add(new HashMap<>(before));
                }
            }
        }

        return deleteBeforeRows;
    }

    /**
     * 从 MergedRecord 中提取 before 数据行
     */
    private List<Map<String, Object>> extractBeforeDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
        List<Map<String, Object>> beforeRows = new ArrayList<>();
        String pkField = getSourceTablePrimaryKey(tableName);

        for (SmartMerger.MergedRecord record : mergedRecords) {
            List<Map<String, Object>> operations = record.getOperations();
            if (operations.isEmpty()) {
                continue;
            }

            int opCount = operations.size();
            // 收集所有操作的 before 状态
            for (int i = 0; i < opCount; i++) {
                Map<String, Object> op = operations.get(i);
                String opType = (String) op.get("op");
                boolean isLastOp = (i == opCount - 1);

                if ("INSERT".equals(opType)) {
                    // INSERT 事件：只有后面还有操作时才需要收集 before 数据
                    if (!isLastOp) {
                        Object value = op.get("value");
                        if (value instanceof Map) {
                            beforeRows.add(new HashMap<>((Map<String, Object>) value));
                        }
                    }
                } else if ("UPDATE".equals(opType)) {
                    // UPDATE 事件的 before 数据
                    Object oldPk = op.get("old_pk");
                    Map<String, Object> beforeRow = new HashMap<>();
                    if (oldPk != null) {
                        // 有主键变更，使用 old_pk
                        beforeRow.put(pkField, oldPk);
                    } else {
                        // 没有主键变更，使用当前主键
                        beforeRow.put(pkField, record.getCurrentPk());
                    }
                    // 复制其他字段（如果有）
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>) op.get("fields");
                    if (fields != null) {
                        for (Map.Entry<String, Object> entry : fields.entrySet()) {
                            if (!pkField.equals(entry.getKey())) {
                                beforeRow.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    beforeRows.add(beforeRow);
                }
                // DELETE 操作不在这里处理
            }

            // 如果最后一个操作是 DELETE，需要添加 finalState 作为 before 数据
            Map<String, Object> lastOp = operations.get(opCount - 1);
            String lastOpType = (String) lastOp.get("op");
            if ("DELETE".equals(lastOpType)) {
                Map<String, Object> beforeRow = new HashMap<>(record.getFinalState());
                if (!beforeRow.isEmpty()) {
                    beforeRows.add(beforeRow);
                }
            }
        }

        return beforeRows;
    }

    /**
     * 从 MergedRecord 中提取 after 数据行
     */
    private List<Map<String, Object>> extractAfterDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords) {
        List<Map<String, Object>> afterRows = new ArrayList<>();

        for (SmartMerger.MergedRecord record : mergedRecords) {
            Map<String, Object> finalState = record.getFinalState();
            if (finalState != null && !finalState.isEmpty()) {
                afterRows.add(new HashMap<>(finalState));
            }
        }

        return afterRows;
    }

    // ... 保留现有其他方法 ...
}
```

---

## 4. 实施计划

### 4.1 第一阶段：SmartMerger 重写
1. 编写新的 `mergeEventsSmart(List<TapdataEvent>)` 方法
2. 增强 `MergedRecord` 内部类
3. 保留原有方法并标记 `@Deprecated`
4. 编写单元测试
5. 运行测试确保功能正常

### 4.2 第二阶段：AffectedKeyCalculator 重构
1. 重构 `calculateAffectedBeforeKeys` 和 `calculateAffectedAfterKeys`
2. 更新内部方法以处理 TapdataEvent
3. 移除 `convertTapdataEventToMap` 方法
4. 编写单元测试
5. 运行测试确保功能正常

### 4.3 第三阶段：集成测试
1. 运行 `HazelcastDuckDbSqlNodeIntegrationTest`
2. 运行 `BatchWideTableUpdateIntegrationTest`
3. 运行 `WithCteIntegrationTest`
4. 确保所有测试通过

### 4.4 第四阶段：性能测试（可选）
1. 对比重构前后的性能指标
2. 验证性能优化效果

---

## 5. 风险控制

| 风险项 | 影响 | 概率 | 缓解措施 |
|-------|------|------|---------|
| 功能回归 | 高 | 中 | 完整测试覆盖，保留向后兼容方法 |
| 性能不升反降 | 中 | 低 | 仔细实现，避免额外对象创建 |
| API使用不当 | 中 | 低 | 完整文档，渐进式改造 |

---

## 6. 测试策略

### 6.1 单元测试
- SmartMerger 新 API 测试
- AffectedKeyCalculator 新 API 测试
- 边界情况测试

### 6.2 集成测试
- 现有集成测试全部通过
- 新增特定场景测试

### 6.3 回归测试
- 确保现有功能不受影响
