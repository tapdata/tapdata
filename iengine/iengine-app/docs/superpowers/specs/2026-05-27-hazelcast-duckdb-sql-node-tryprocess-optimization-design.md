# HazelcastDuckDbSqlNode.tryProcess 优化设计文档

**日期:** 2026-05-27
**作者:** AI Assistant
**状态:** 待评审

---

## 1. 优化目标

优化 `HazelcastDuckDbSqlNode.tryProcess` 方法及相关类的整体处理逻辑，实现：
- 结构清晰、步骤紧凑、事务严谨
- 关键代码添加详细中文注释
- 去除冗余无意义环节和重复代码
- CDC事件统一使用 `TapdataEvent` 而非 `Map` 结构
- 所有引用使用 `import` 方式，禁止完整包名

---

## 2. 当前问题分析

### 2.1 调用链冗长
```
tryProcess
  → processRecordEventWithStage (与processRecordEvent大量重复)
    → handleOutputWithStage (过度抽象)
      → handleCdcOutput / handleInitialSyncOutput (仅一行代码)
```

### 2.2 代码重复
- `processRecordEventWithStage` 和 `processRecordEvent` 的错误处理、DLQ写入逻辑完全相同
- 两个方法约80%代码重复

### 2.3 CDC事件处理使用Map中间格式
- 丢失原始 `TapdataEvent` 类型信息
- 需要手动提取 `table`、`record`、`op` 字段
- 增加转换开销和维护成本

### 2.4 硬编码问题
- `flushCdcBufferWithNewComponent` 中硬编码 `"users"` 表名
- 应使用配置的 `mainTableName`

### 2.5 完整包名引用
- 多处使用 `io.tapdata.entity.event.dml.TapInsertRecordEvent` 等完整包名
- 应统一使用 `import` 方式

### 2.6 英文注释
- 大量英文注释需要改为中文
- 关键代码缺少详细注释

---

## 3. 设计方案

### 3.1 调用链重构

**重构后调用链：**
```
tryProcess
  ├── 步骤1: 空事件检查
  ├── 步骤2: 错误率监控
  ├── 步骤3: 同步阶段追踪
  ├── 步骤4: 事件分类处理
  │   ├── 非DML事件 → 直接透传
  │   └── DML事件 → processRecordEvent (统一方法)
  │       ├── 提取上下文
  │       ├── 写入DuckDB缓冲区
  │       ├── CDC阶段 → 处理物化视图
  │       └── 透传事件
  └── 步骤5: 异常处理 (统一错误记录+DLQ)
```

**删除的方法：**
- `processRecordEventWithStage`
- `handleOutputWithStage`
- `handleCdcOutput`
- `handleInitialSyncOutput`

**保留并优化的方法：**
- `processRecordEvent` (合并逻辑)
- `tryProcess` (简化调用链)

### 3.2 CDC事件处理重构

**改动1: cdcEventBuffer类型变更**
```java
// 之前
private List<Map<String, Object>> cdcEventBuffer = new ArrayList<>();

// 之后
private List<TapdataEvent> cdcEventBuffer = new ArrayList<>();
```

**改动2: processCdcEventForMaterializedView简化**
```java
// 之前: 构建Map中间格式
private void processCdcEventForMaterializedView(String tableName, TapRecordEvent recordEvent, Map<String, Object> recordData) {
    Map<String, Object> eventMap = new HashMap<>();
    eventMap.put("table", tableName);
    eventMap.put("record", recordData);
    eventMap.put("op", opType);
    cdcEventBuffer.add(eventMap);
}

// 之后: 直接存储TapdataEvent
private void processCdcEventForMaterializedView(TapdataEvent tapdataEvent) {
    cdcEventBuffer.add(tapdataEvent);
    if (cdcEventBuffer.size() >= CDC_BUFFER_SIZE) {
        flushCdcBuffer();
    }
}
```

**改动3: flushCdcBuffer从TapdataEvent提取tableId**
```java
// 之前: 从Map提取table字段
Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();
for (Map<String, Object> event : cdcEventBuffer) {
    String table = (String) event.get("table");
    eventsByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(event);
}

// 之后: 从TapdataEvent提取tableId
Map<String, List<TapdataEvent>> eventsByTable = new HashMap<>();
for (TapdataEvent event : cdcEventBuffer) {
    TapEvent tapEvent = event.getTapEvent();
    if (tapEvent instanceof TapRecordEvent) {
        String tableId = TapEventUtil.getTableId((TapRecordEvent) tapEvent);
        eventsByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(event);
    }
}
```

**改动4: 修复硬编码表名**
```java
// 之前
List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
    beforeKeys, afterKeys, afterRows, "users");

// 之后
List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
    beforeKeys, afterKeys, afterRows, mainTableName);
```

### 3.3 Import规范

**新增import：**
```java
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import com.tapdata.entity.SyncStage;
```

**删除完整包名引用：**
- 所有 `io.tapdata.entity.event.dml.TapInsertRecordEvent` → `TapInsertRecordEvent`
- 所有 `io.tapdata.entity.schema.TapField` → `TapField`
- 所有 `io.tapdata.entity.schema.type.TapString` → `TapString`
- 等等...

### 3.4 注释规范

**要求：**
1. 每个方法必须有JavaDoc中文注释
2. 复杂逻辑必须分步骤注释（步骤1、步骤2...）
3. 关键业务逻辑必须说明"为什么"而不仅是"做什么"
4. 所有英文注释改为中文

**示例：**
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
        
        // 步骤2: 写入DuckDB缓冲区
        synchronized (context.getCommitLock()) {
            context.addRecord(recordData);
            // 缓冲区满则刷写
            if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                flushContext(context);
            }
        }
        
        // 步骤3: CDC阶段处理物化视图
        // 注意: 仅在全量同步完成后才执行增量更新
        if (syncStageTracker.isTransitionCompleted() && affectedKeyCalculator != null) {
            processCdcEventForMaterializedView(tapdataEvent);
        }
        
        // 步骤4: 透传事件到下游
        consumer.accept(tapdataEvent, null);
    } catch (Exception e) {
        // 异常处理: 记录错误并写入DLQ
        // ...
    }
}
```

---

## 4. 影响范围

### 4.1 修改的文件
- `HazelcastDuckDbSqlNode.java` (主要修改)

### 4.2 不受影响的文件
- 所有测试文件（功能不变，仅内部重构）
- 其他duckdb包下的组件类

### 4.3 风险评估
- **低风险**: 功能逻辑不变，仅重构代码结构
- **需要验证**: 现有测试用例应全部通过

---

## 5. 验证方法

1. 编译通过
2. 运行现有测试用例:
   - `HazelcastDuckDbSqlNodeTest`
   - `HazelcastDuckDbSqlNodeMultiSourceTest`
   - `HazelcastDuckDbSqlNodeIntegrationTest`
3. 确认测试全部通过
