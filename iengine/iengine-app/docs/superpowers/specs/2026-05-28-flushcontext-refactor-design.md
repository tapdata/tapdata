# HazelcastDuckDbSqlNode.flushContext 重构设计方案

## 设计日期: 2026-05-28

## 一、背景与目标

### 背景
当前 `HazelcastDuckDbSqlNode.flushContext` 的实现存在以下问题：
1. 全量阶段 (`processInitialSyncStage`) 中包含了不必要的宽表更新逻辑
2. 全量结束后的宽表更新时机不够清晰
3. 两个阶段的职责边界模糊

### 目标
重构 `flushContext` 方法，实现清晰的双阶段处理模式：
- **全量阶段**：专注数据写入 DuckDB，不包含宽表更新
- **增量阶段**：完整的宽表更新 + CDC 事件生成流程
- **全量结束统一处理**：在 `handleAllTablesCdcTransition` 中一次性更新宽表并生成 CDC 事件

---

## 二、架构设计

### 2.1 整体流程图

```
flushContext(context)
    │
    ├─ 判断同步阶段
    │   ├─ isInitialSync = true → processInitialSyncStage()
    │   └─ isInitialSync = false → processCdcStage()
    │
processInitialSyncStage() [全量阶段]
    ├── 步骤1: SmartMerger.mergeEventsSmart(events)
    ├── 步骤2: ensureTableExists()
    └── 步骤3: executeInTransaction()
            ├── DELETE Before 数据
            └── writeBatch After 数据
    
processCdcStage() [增量阶段]
    ├── 步骤1: SmartMerger.mergeEventsSmart(events)
    ├── 步骤2: calculateAffectedBeforeKeys()
    ├── 步骤3: ensureTableExists()
    ├── 步骤4: executeInTransaction()
    │       ├── DELETE Before 数据
    │       └── writeBatch After 数据
    ├── 步骤5: calculateAffectedAfterKeys()
    ├── 步骤6: updateWideTableAsTapdataEvents()
    └── 步骤7: emitWideTableChangelogEvents()

handleAllTablesCdcTransition() [全量结束统一处理]
    ├── 步骤1: flushAllContexts()
    ├── 步骤2: updateWideTableInFullSyncComplete() [INSERT INTO ... SELECT]
    ├── 步骤3: generateWideTableInsertEvents() [SELECT * FROM wide_table]
    ├── 步骤4: triggerChangelogListener()
    └── 步骤5: emitEventsToDownstream()
```

### 2.2 核心方法职责划分

| 方法 | 职责 | 阶段 |
|------|------|------|
| `flushContext()` | 路由分发，根据同步阶段调用不同处理逻辑 | 入口 |
| `processInitialSyncStage()` | 全量数据合并 + 写入 DuckDB | 全量 |
| `processCdcStage()` | 增量数据合并 + 写入 DuckDB + 宽表更新 + CDC 事件生成 | 增量 |
| `updateWideTableInFullSyncComplete()` | 全量结束后一次性更新宽表 | 全量结束 |
| `generateWideTableInsertEvents()` | 查询宽表生成 INSERT CDC 事件 | 全量结束 |
| `handleAllTablesCdcTransition()` | 协调全量结束后的所有操作 | 全量结束 |

---

## 三、详细设计

### 3.1 全量阶段：processInitialSyncStage()

```java
private void processInitialSyncStage(PerSourceContext context, List<TapdataEvent> eventsToFlush) 
    throws SQLException, java.io.IOException {
    
    // 步骤1: 使用 SmartMerger 合并事件（第二步、第三步）
    List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsToFlush);
    if (mergedRecords.isEmpty()) {
        return;
    }
    
    // 确保表存在
    ensureTableExists(context, eventsToFlush);
    
    // 获取 DuckDbOperator
    DuckDbOperator operator = getOperatorForContext(context);
    
    // 步骤2-3: DuckDB 事务开启（可选）
    operator.executeInTransaction(() -> {
        // Before 的所有数据全部执行 delete 操作
        deleteBeforeData(operator, context.getTargetTableName(), mergedRecords);
        
        // After 的数据执行 Arrow 批量写入
        writeAfterData(operator, context.getTargetTableName(), mergedRecords);
    });
    
    // 注意：不在此处更新宽表，由 handleAllTablesCdcTransition 统一处理
}
```

**关键点**：
- ✅ 只负责数据写入 DuckDB
- ❌ 不包含宽表更新逻辑
- ❌ 不生成 CDC 事件
- 使用事务保证原子性

### 3.2 增量阶段：processCdcStage()

```java
private void processCdcStage(PerSourceContext context, List<TapdataEvent> eventsToFlush) 
    throws SQLException, java.io.IOException {
    
    // 步骤1: 使用 SmartMerger 合并事件（第二步、第三步、第四步都用）
    List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsToFlush);
    if (mergedRecords.isEmpty()) {
        return;
    }
    
    // 确保表存在
    ensureTableExists(context, eventsToFlush);
    
    // 获取 DuckDbOperator
    DuckDbOperator operator = getOperatorForContext(context);
    
    // 步骤2: 计算 beforeKeys（数据写入 DuckDB 之前）
    Set<Object> beforeKeys = calculateBeforeKeys(eventsToFlush, context.getKey());
    
    // 步骤3-4: DuckDB 事务开启，写入数据
    operator.executeInTransaction(() -> {
        deleteBeforeData(operator, context.getTargetTableName(), mergedRecords);
        writeAfterData(operator, context.getTargetTableName(), mergedRecords);
    });
    
    // 步骤5: 计算 afterKeys（数据写入 DuckDB 之后、宽表更新之前）
    Set<Object> afterKeys = calculateAfterKeys(eventsToFlush);
    
    // 步骤6: 更新宽表（可选）
    updateWideTable(beforeKeys, afterKeys, eventsToFlush);
    
    // 步骤7: 生成宽表 CDC 事件，触发 ChangelogListener，发射到下游
    emitWideTableChangelogEvents();
}
```

**关键点**：
- ✅ 包含完整的宽表更新流程
- ✅ 包含 CDC 事件生成
- ✅ 使用事务保证数据一致性
- before/after keys 计算时机正确

### 3.3 全量结束统一处理：handleAllTablesCdcTransition()

```java
private void handleAllTablesCdcTransition() {
    logger.info("所有表已切换到CDC阶段，执行查询并发射结果...");
    
    try {
        // 步骤1: 刷写所有剩余数据
        flushAllContexts();
        
        // 步骤2: 更新宽表（可选）一次，直接用 insert + querySQL 语句一次性写入
        if (shouldUpdateWideTable()) {
            try {
                duckDbOperator.executeInTransaction(() -> {
                    updateWideTableInFullSyncComplete();
                });
                logger.info("全量结束后宽表更新完成");
            } catch (Exception e) {
                logger.error("全量结束后更新宽表失败: {}", e.getMessage(), e);
            }
        }
        
        // 步骤3: 生成宽表 CDC 事件，直接用 select 查询宽表语句查询出所有结果集拼接 insertEvent
        if (outputChangelogEnabled && shouldUpdateWideTable()) {
            try {
                List<TapdataEvent> wideTableEvents = generateWideTableInsertEvents();
                
                // 步骤4: 触发 ChangelogListener
                triggerChangelogListener(wideTableEvents);
                
                // 步骤5: 发射宽表 CDC 事件到下游
                emitEventsToDownstream(wideTableEvents);
                
                logger.info("发射 {} 个宽表 CDC 事件到下游", wideTableEvents.size());
            } catch (Exception e) {
                logger.error("生成宽表 CDC 事件失败: {}", e.getMessage(), e);
            }
        }
        
        // 步骤6: 执行原有的查询并发射结果
        if (executeQueryOnFullSyncComplete && !queryExecuted) {
            executeAndEmitQueryResults();
            queryExecuted = true;
            logger.info("查询执行成功，结果已发射");
        }
        
    } catch (Exception e) {
        logger.error("全量同步完成后处理失败: {}", e.getMessage(), e);
    }
}
```

**关键点**：
- ✅ 统一在所有表全量结束后执行
- ✅ 一次性更新宽表（使用 INSERT INTO ... SELECT）
- ✅ 一次性生成所有 CDC 事件
- ✅ 性能最优，避免重复计算

---

## 四、提取公共方法

为了减少重复代码，将公共逻辑提取为独立方法：

### 4.1 数据删除方法
```java
private void deleteBeforeData(DuckDbOperator operator, String tableName, 
                              List<SmartMerger.MergedRecord> mergedRecords) {
    List<Object> beforePks = extractPrimaryKeys(mergedRecords);
    if (!beforePks.isEmpty()) {
        String deleteSql = buildDeleteSql(tableName, beforePks);
        operator.executeUpdate(deleteSql);
    }
}
```

### 4.2 数据写入方法
```java
private void writeAfterData(DuckDbOperator operator, String tableName,
                            List<SmartMerger.MergedRecord> mergedRecords) {
    List<Map<String, Object>> afterData = extractFinalStates(mergedRecords);
    if (!afterData.isEmpty()) {
        operator.writeBatch(afterData, tableName);
    }
}
```

### 4.3 主键提取方法
```java
private List<Object> extractPrimaryKeys(List<SmartMerger.MergedRecord> mergedRecords) {
    return mergedRecords.stream()
        .map(SmartMerger.MergedRecord::getInitialPk)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
}
```

### 4.4 最终状态提取方法
```java
private List<Map<String, Object>> extractFinalStates(List<SmartMerger.MergedRecord> mergedRecords) {
    return mergedRecords.stream()
        .map(SmartMerger.MergedRecord::getFinalState)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
}
```

---

## 五、错误处理策略

### 5.1 全量阶段错误处理
```java
try {
    processInitialSyncStage(context, eventsToFlush);
} catch (Exception e) {
    // 回滚数据到缓冲区
    rollbackDataToBuffer(context, eventsToFlush);
    // 写入 DLQ
    writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
    // 记录日志
    logger.error("全量阶段刷写失败: {}", context.getKey(), e.getMessage(), e);
}
```

### 5.2 增量阶段错误处理
```java
try {
    processCdcStage(context, eventsToFlush);
} catch (Exception e) {
    // 回滚数据到缓冲区
    rollbackDataToBuffer(context, eventsToFlush);
    // 写入 DLQ
    writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
    // 记录日志
    logger.error("增量阶段刷写失败: {}", context.getKey(), e.getMessage(), e);
}
```

### 5.3 全量结束统一处理错误处理
- 宽表更新失败不影响后续流程
- CDC 事件生成失败只记录日志
- 所有异常都被捕获，不会导致系统崩溃

---

## 六、性能优化点

### 6.1 减少对象创建
- 直接复用 `TapdataEvent` 引用，避免中间转换
- 使用 Stream API 提取数据，减少循环次数

### 6.2 批量操作
- 使用 `writeBatch` 替代逐行插入
- 使用 `DELETE ... IN (...)` 替代逐行删除
- 使用事务包装多个操作

### 6.3 SQL 优化
- 全量结束后的宽表更新使用 `INSERT INTO ... SELECT ...`
- 避免 N+1 查询问题

---

## 七、测试策略

### 7.1 单元测试
- 测试 `processInitialSyncStage` 只负责数据写入
- 测试 `processCdcStage` 包含完整流程
- 测试 `handleAllTablesCdcTransition` 统一处理逻辑

### 7.2 集成测试
- 测试全量 → 增量的转换过程
- 测试宽表更新的正确性
- 测试 CDC 事件的生成和发射

### 7.3 性能测试
- 对比重构前后的吞吐量
- 对比内存占用情况
- 测试大数据量场景下的表现

---

## 八、实施计划

### Phase 1: 重构核心方法（预计 2 小时）
1. 重构 `processInitialSyncStage` - 移除宽表更新逻辑
2. 重构 `processCdcStage` - 添加完整的宽表更新流程
3. 提取公共方法减少重复代码

### Phase 2: 优化 handleAllTablesCdcTransition（预计 1 小时）
1. 调整宽表更新时机
2. 优化 CDC 事件生成逻辑
3. 添加错误处理

### Phase 3: 编译验证和测试（预计 1 小时）
1. 运行编译验证
2. 运行现有测试用例
3. 补充新的测试用例

### Phase 4: 文档和审查（预计 30 分钟）
1. 更新代码注释
2. 代码审查
3. 性能验证

---

## 九、风险评估

### 高风险项
- ⚠️ **事务一致性**：确保全量和增量的事务边界正确
- ⚠️ **CDC 事件顺序**：确保事件的发射顺序符合预期

### 低风险项
- ℹ️ **性能回归**：需要对比测试确认性能没有下降
- ℹ️ **兼容性**：确保现有功能不受影响

---

## 十、验收标准

### 功能验收
- ✅ 全量阶段只负责数据写入，不更新宽表
- ✅ 增量阶段包含完整的宽表更新和 CDC 事件生成
- ✅ 全量结束后统一更新宽表并生成 CDC 事件
- ✅ 所有现有功能正常工作

### 性能验收
- ✅ 吞吐量不低于重构前
- ✅ 内存占用不超过重构前
- ✅ 大数据量场景下表现稳定

### 代码质量验收
- ✅ 代码编译通过
- ✅ 所有测试用例通过
- ✅ 代码注释清晰
- ✅ 符合项目编码规范
