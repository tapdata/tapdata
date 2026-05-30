# HazelcastDuckDbSqlNode flushContext合并设计文档

## 设计目标

将 `flushCdcBuffer()` 与 `flushContext(context)` 合并，严格保证执行顺序：
1. `calculateAffectedBeforeKeys` → 所有数据写入DuckDB之前
2. 当前Context当前批次数据 → 写入当前Context对应表
3. `calculateAffectedAfterKeys` → 当前Context对应表所有数据写入DuckDB之后、宽表更新之前
4. `updateWideTable` → 最后执行

## 核心变更

### 1. 数据结构变更

#### PerSourceContext
- **旧**: `List<Map<String, Object>> batchBuffer`
- **新**: `List<TapdataEvent> batchBuffer`
- **原因**: 保留完整事件类型信息，去除Map中间格式转换

#### HazelcastDuckDbSqlNode
- **删除**: `List<TapdataEvent> cdcEventBuffer`
- **原因**: CDC事件直接缓存在Context的buffer中，无需独立缓冲区

### 2. AffectedKeyCalculator接口变更

```java
// 旧接口
Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable);
Set<Object> calculateAffectedAfterKeys(Map<String, List<Map<String, Object>>> eventsByTable);

// 新接口
Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events);
Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events);
```

### 3. 锁机制

- **保留**: `synchronized(context.getCommitLock())` Context内部锁
- **不引入**: HazelcastDuckDbSqlNode对象锁
- **原因**: 每个Context独立执行4步流程，Context级别锁已足够

## 完整调用链

```
tryProcess()
  └─ processRecordEvent()
      ├─ synchronized(context.getCommitLock())
      │   ├─ context.addEvent(tapdataEvent)
      │   └─ if buffer满 → flushContext(context)
      │       └─ synchronized(context.getCommitLock())
      │           ├─ 步骤1: affectedKeyCalculator.calculateAffectedBeforeKeys(eventsToFlush)
      │           ├─ 步骤2: upsertBatch(context.getTargetTableName(), merged)
      │           ├─ 步骤3: affectedKeyCalculator.calculateAffectedAfterKeys(eventsToFlush)
      │           └─ 步骤4: wideTableUpdater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, afterRows, mainTableName)
      └─ consumer.accept(tapdataEvent, null)
```

## 时序保证

| 步骤 | 操作 | 数据状态 | 锁状态 |
|------|------|----------|--------|
| 1 | `calculateAffectedBeforeKeys` | buffer中数据**未写入**DuckDB | `synchronized(context.getCommitLock())` |
| 2 | `upsertBatch` | buffer数据**写入**对应表 | 持有锁 |
| 3 | `calculateAffectedAfterKeys` | buffer数据**已写入**DuckDB | 持有锁 |
| 4 | `updateWideTable` | 宽表**更新**完成 | 持有锁 |

## 废弃方法

- `processCdcEventForMaterializedView()` → 删除
- `flushCdcBuffer()` → 删除
- `flushCdcBufferWithNewComponent()` → 删除
- `flushCdcBufferWithOldComponent()` → 删除
- `convertTapdataEventsToMaps()` → 删除
- `extractAfterRowsFromTapdataEvents()` → 删除

## 保留方法

- `extractAfterRowsFromEvents(List<TapdataEvent> events)` → 从TapdataEvent提取after数据
- `extractDataFromEvents(List<TapdataEvent> events)` → 用于DLQ写入

## 需要修改的文件

1. **AffectedKeyCalculator.java** → 修改接口签名
2. **AffectedKeyCalculatorImpl.java** → 实现新接口
3. **HazelcastDuckDbSqlNode.java** → 重写flushContext，删除Map转换逻辑
4. **PerSourceContext.java** → 修改buffer类型为 `List<TapdataEvent>`

## 测试策略

1. 单元测试：验证AffectedKeyCalculator新接口
2. 集成测试：验证flushContext 4步顺序
3. 多源测试：验证多Context并发安全性
4. 回归测试：确保现有功能不受影响
