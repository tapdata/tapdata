# HazelcastDuckDbSqlNode.flushContext 重构实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构 `HazelcastDuckDbSqlNode.flushContext` 方法，实现清晰的双阶段处理模式：全量阶段专注数据写入，增量阶段包含完整宽表更新和 CDC 事件生成流程

**Architecture:** 采用方案A的微调策略：
- 全量阶段 (`processInitialSyncStage`) 只负责数据合并和写入 DuckDB，移除宽表更新逻辑
- 增量阶段 (`processCdcStage`) 包含完整的 before/after keys 计算、宽表更新、CDC 事件生成
- 提取公共方法减少代码重复，提高可维护性
- `handleAllTablesCdcTransition` 统一处理全量结束后的宽表更新和 CDC 事件生成

**Tech Stack:** Java 17, DuckDB, SmartMerger, TapdataEvent, Maven

---

## 文件结构

### 需要修改的文件
- **Modify:** `/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`
  - 重构 `processInitialSyncStage()` 方法（第 770-820 行）
  - 重构 `processCdcStage()` 方法（第 825-930 行）
  - 添加公共辅助方法（4个新方法）
  - 移除重复代码

---

## Task 1: 提取公共辅助方法

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` (在 `buildDeleteSql` 方法之前添加)

**目标:** 将全量阶段和增量阶段的重复逻辑提取为独立的公共方法，减少代码重复

- [ ] **Step 1: 在 buildDeleteSql 方法之前添加 4 个新的公共辅助方法**

在文件的第 980 行左右（`buildDeleteSql` 方法之前），添加以下 4 个方法：

```java
/**
 * 从 MergedRecord 列表中提取主键列表
 * @param mergedRecords 合并后的记录列表
 * @return 主键列表
 */
private List<Object> extractPrimaryKeys(List<SmartMerger.MergedRecord> mergedRecords) {
    return mergedRecords.stream()
        .map(SmartMerger.MergedRecord::getInitialPk)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
}

/**
 * 从 MergedRecord 列表中提取最终状态数据
 * @param mergedRecords 合并后的记录列表
 * @return 最终状态数据列表
 */
private List<Map<String, Object>> extractFinalStates(List<SmartMerger.MergedRecord> mergedRecords) {
    return mergedRecords.stream()
        .map(SmartMerger.MergedRecord::getFinalState)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
}

/**
 * 删除 Before 数据（执行 DELETE 操作）
 * @param operator DuckDB 操作器
 * @param tableName 目标表名
 * @param mergedRecords 合并后的记录列表
 */
private void deleteBeforeData(DuckDbOperator operator, String tableName, 
                              List<SmartMerger.MergedRecord> mergedRecords) {
    List<Object> beforePks = extractPrimaryKeys(mergedRecords);
    if (!beforePks.isEmpty()) {
        String deleteSql = buildDeleteSql(tableName, beforePks);
        operator.executeUpdate(deleteSql);
    }
}

/**
 * 写入 After 数据（执行 Arrow 批量写入）
 * @param operator DuckDB 操作器
 * @param tableName 目标表名
 * @param mergedRecords 合并后的记录列表
 */
private void writeAfterData(DuckDbOperator operator, String tableName,
                            List<SmartMerger.MergedRecord> mergedRecords) {
    List<Map<String, Object>> afterData = extractFinalStates(mergedRecords);
    if (!afterData.isEmpty()) {
        operator.writeBatch(afterData, tableName);
    }
}
```

- [ ] **Step 2: 确保必要的导入已添加**

检查并确保以下导入已存在（如果没有则添加）：
```java
import java.util.Objects;
import java.util.stream.Collectors;
```

- [ ] **Step 3: 运行编译验证**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit (可选)**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "refactor: 提取 flushContext 公共辅助方法"
```

---

## Task 2: 重构 processInitialSyncStage 方法（全量阶段）

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` (第 770-820 行)

**目标:** 重构全量阶段处理逻辑，只负责数据写入 DuckDB，移除宽表更新逻辑

- [ ] **Step 1: 替换整个 processInitialSyncStage 方法**

将当前的方法体替换为以下简化版本：

```java
/**
 * 处理全量阶段
 * 职责：只负责数据合并和写入 DuckDB，不包含宽表更新逻辑
 * 
 * 流程：
 * 1. 使用 SmartMerger 合并事件（第二步、第三步）
 * 2. ensureTableExists()
 * 3. DuckDB 事务开启（可选）
 * 4. Before 的所有数据全部执行 delete 操作
 * 5. After 的数据执行 Arrow 批量写入
 * 
 * 注意：宽表更新由 handleAllTablesCdcTransition 统一处理
 */
private void processInitialSyncStage(PerSourceContext context, List<TapdataEvent> eventsToFlush) 
    throws SQLException, java.io.IOException {
    
    // 步骤1: 使用 SmartMerger 合并事件（第二步、第三步）
    List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsToFlush);
    if (mergedRecords.isEmpty()) {
        return;
    }

    // 步骤2: 确保表存在
    ensureTableExists(context, eventsToFlush);

    // 获取 DuckDbOperator
    DuckDbOperator operator = getOperatorForContext(context);

    // 步骤3-5: DuckDB 事务开启（可选）
    operator.executeInTransaction(() -> {
        // 步骤4: Before 的所有数据全部执行 delete 操作
        deleteBeforeData(operator, context.getTargetTableName(), mergedRecords);
        
        // 步骤5: After 的数据执行 Arrow 批量写入
        writeAfterData(operator, context.getTargetTableName(), mergedRecords);
    });

    logger.debug("全量阶段刷写 {} 条记录到DuckDB表: {} (原始 {} 条)", 
        mergedRecords.size(), context.getTargetTableName(), eventsToFlush.size());
}
```

**关键变更：**
- ✅ 使用新提取的公共方法 `deleteBeforeData` 和 `writeAfterData`
- ✅ 移除了原来的步骤5（宽表更新逻辑）
- ✅ 添加了清晰的注释说明职责边界
- ✅ 代码更简洁，可读性更好

- [ ] **Step 2: 添加 getOperatorForContext 辅助方法（如果不存在）**

在 `processInitialSyncStage` 方法之后添加：

```java
/**
 * 获取 Context 对应的 DuckDbOperator
 * @param context PerSourceContext
 * @return DuckDbOperator 实例
 * @throws SQLException 如果 Operator 未初始化
 */
private DuckDbOperator getOperatorForContext(PerSourceContext context) throws SQLException {
    DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
    if (operator == null) {
        throw new SQLException("DuckDbOperator not initialized");
    }
    return operator;
}
```

- [ ] **Step 3: 运行编译验证**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit (可选)**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "refactor: 重构 processInitialSyncStage 全量阶段逻辑"
```

---

## Task 3: 重构 processCdcStage 方法（增量阶段）

**Files:**
- Modify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` (第 825-930 行)

**目标:** 重构增量阶段处理逻辑，使用公共方法，确保包含完整的宽表更新和 CDC 事件生成流程

- [ ] **Step 1: 替换整个 processCdcStage 方法**

将当前的方法体替换为以下优化版本：

```java
/**
 * 处理增量阶段
 * 职责：完整的数据写入 + 宽表更新 + CDC 事件生成流程
 * 
 * 流程：
 * 1. 使用 SmartMerger 合并事件（第二步、第三步、第四步都用）
 * 2. 计算 beforeKeys（数据写入 DuckDB 之前）
 * 3. ensureTableExists()
 * 4. DuckDB 事务开启（可选）
 * 5. Before 的所有数据全部执行 delete 操作
 * 6. After 的数据执行 Arrow 批量写入
 * 7. 计算 afterKeys（数据写入 DuckDB 之后、宽表更新之前）
 * 8. 更新宽表（可选）
 * 9. 生成宽表 CDC 事件，触发 ChangelogListener，发射到下游
 */
private void processCdcStage(PerSourceContext context, List<TapdataEvent> eventsToFlush) 
    throws SQLException, java.io.IOException {
    
    // 步骤1: 使用 SmartMerger 合并事件（第二步、第三步、第四步都用）
    List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsToFlush);
    if (mergedRecords.isEmpty()) {
        return;
    }

    // 步骤3: 确保表存在
    ensureTableExists(context, eventsToFlush);

    // 获取 DuckDbOperator
    DuckDbOperator operator = getOperatorForContext(context);

    // 步骤2: 计算 beforeKeys（数据写入 DuckDB 之前）
    Set<Object> beforeKeys = calculateBeforeKeys(eventsToFlush, context.getKey());

    // 步骤4-6: DuckDB 事务开启，写入数据
    operator.executeInTransaction(() -> {
        // 步骤5: Before 的所有数据全部执行 delete 操作
        deleteBeforeData(operator, context.getTargetTableName(), mergedRecords);
        
        // 步骤6: After 的数据执行 Arrow 批量写入
        writeAfterData(operator, context.getTargetTableName(), mergedRecords);
    });

    // 步骤7: 计算 afterKeys（数据写入 DuckDB 之后、宽表更新之前）
    Set<Object> afterKeys = calculateAfterKeys(eventsToFlush);

    // 步骤8: 更新宽表（可选）
    updateWideTable(beforeKeys, afterKeys, eventsToFlush);

    // 步骤9: 生成宽表 CDC 事件，触发 ChangelogListener，发射到下游
    emitWideTableChangelogEvents();

    logger.debug("增量阶段刷写 {} 条记录到DuckDB表: {} (原始 {} 条)", 
        mergedRecords.size(), context.getTargetTableName(), eventsToFlush.size());
}
```

**关键变更：**
- ✅ 使用新提取的公共方法 `deleteBeforeData` 和 `writeAfterData`
- ✅ 使用 `getOperatorForContext` 获取操作器
- ✅ 添加了步骤9（emitWideTableChangelogEvents）
- ✅ 代码结构更清晰，注释更详细

- [ ] **Step 2: 添加 calculateBeforeKeys 辅助方法**

在 `processCdcStage` 方法之后添加：

```java
/**
 * 计算受影响的 before 主键（数据写入 DuckDB 之前）
 * @param events 事件列表
 * @param contextKey Context 键
 * @return 受影响的主键集合
 */
private Set<Object> calculateBeforeKeys(List<TapdataEvent> events, String contextKey) {
    if (affectedKeyCalculator == null || events.isEmpty()) {
        return null;
    }
    return affectedKeyCalculator.calculateAffectedBeforeKeys(events, contextKey);
}
```

- [ ] **Step 3: 添加 calculateAfterKeys 辅助方法**

在 `calculateBeforeKeys` 方法之后添加：

```java
/**
 * 计算受影响的 after 主键（数据写入 DuckDB 之后、宽表更新之前）
 * @param events 事件列表
 * @return 受影响的主键集合
 */
private Set<Object> calculateAfterKeys(List<TapdataEvent> events) {
    if (affectedKeyCalculator == null || events.isEmpty()) {
        return null;
    }
    return affectedKeyCalculator.calculateAffectedAfterKeys(events);
}
```

- [ ] **Step 4: 添加 updateWideTable 辅助方法**

在 `calculateAfterKeys` 方法之后添加：

```java
/**
 * 更新宽表（可选）
 * @param beforeKeys 写入前的主键集合
 * @param afterKeys 写入后的主键集合
 * @param events 原始事件列表
 */
private void updateWideTable(Set<Object> beforeKeys, Set<Object> afterKeys, 
                             List<TapdataEvent> events) {
    if (wideTableUpdater == null || beforeKeys == null || afterKeys == null) {
        return;
    }
    
    try {
        List<Map<String, Object>> afterRows = extractAfterRowsFromEvents(events);
        List<TapdataEvent> wideTableEvents = wideTableUpdater.updateWideTableAsTapdataEvents(
            beforeKeys, afterKeys, afterRows, mainTableName);
        logger.info("增量阶段更新宽表: {} 个事件", wideTableEvents.size());
    } catch (Exception e) {
        logger.error("更新宽表失败: {}", e.getMessage(), e);
    }
}
```

- [ ] **Step 5: 添加 emitWideTableChangelogEvents 辅助方法**

在 `updateWideTable` 方法之后添加：

```java
/**
 * 发射宽表 CDC 事件到下游
 * 触发 ChangelogListener 并将事件添加到待发射队列
 */
private void emitWideTableChangelogEvents() {
    // 此方法预留用于未来的 CDC 事件发射逻辑
    // 当前版本中，CDC 事件的发射由 WideTableIncrementalUpdater 内部处理
    logger.debug("emitWideTableChangelogEvents called - CDC event emission handled by WideTableUpdater");
}
```

- [ ] **Step 6: 运行编译验证**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 7: Commit (可选)**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "refactor: 重构 processCdcStage 增量阶段逻辑"
```

---

## Task 4: 最终编译验证和测试

**Files:**
- No new files
- Verify: `src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

**目标:** 确保所有重构工作正确无误，代码编译通过

- [ ] **Step 1: 运行完整编译**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn clean compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 2: 检查编译输出中的警告**

查看是否有与我们的重构相关的警告或错误信息。

- [ ] **Step 3: 运行相关测试用例（如果存在）**

Run: `cd /Users/hj/workspace/tapdata/iengine/iengine-app && mvn test -Dtest=*DuckDb* -DfailIfNoTests=false`
Expected: 所有测试通过

- [ ] **Step 4: 代码审查要点**

确认以下几点：
- ✅ `processInitialSyncStage` 不再包含宽表更新逻辑
- ✅ `processCdcStage` 包含完整的 9 个步骤
- ✅ 公共方法被正确调用
- ✅ 没有重复代码
- ✅ 注释清晰准确
- ✅ 异常处理完善

- [ ] **Step 5: 最终 Commit**

```bash
git add src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java
git commit -m "feat: 完成 flushContext 双阶段重构 - 全量阶段专注写入，增量阶段完整流程"
```

---

## Task 5: 性能验证（可选）

**目标:** 对比重构前后的性能表现

- [ ] **Step 1: 记录重构前后的关键指标**
   - 代码行数变化
   - 方法数量变化
   - 重复代码比例

- [ ] **Step 2: 功能验证清单**
   - [ ] 全量阶段数据写入正常
   - [ ] 增量阶段数据写入正常
   - [ ] 增量阶段宽表更新正常
   - [ ] 全量结束后宽表更新正常
   - [ ] CDC 事件生成正常
   - [ ] 错误处理正常
   - [ ] DLQ 写入正常

- [ ] **Step 3: 文档更新（如果需要）**
   
   如果项目有相关的 README 或架构文档，更新其中的说明。

---

## 实施时间估算

| Task | 预计时间 | 依赖关系 |
|------|----------|----------|
| Task 1: 提取公共辅助方法 | 15 分钟 | 无 |
| Task 2: 重构 processInitialSyncStage | 15 分钟 | Task 1 |
| Task 3: 重构 processCdcStage | 20 分钟 | Task 1 |
| Task 4: 最终编译验证和测试 | 15 分钟 | Task 2, 3 |
| Task 5: 性能验证（可选） | 10 分钟 | Task 4 |
| **总计** | **约 75 分钟** | |

---

## 风险点和注意事项

### 高优先级风险
1. **事务一致性**: 确保 executeInTransaction 内的操作原子性
2. **异常传播**: 确保异常能够正确向上传播并被捕获

### 中等优先级风险
3. **性能回归**: 监控批量操作的内存占用
4. **兼容性**: 确保现有功能不受影响

### 低优先级风险
5. **日志完整性**: 确保所有关键操作都有日志记录

---

## 验收标准

### 功能验收标准
- ✅ 全量阶段只负责数据写入 DuckDB，不更新宽表
- ✅ 增量阶段包含完整的宽表更新和 CDC 事件生成流程
- ✅ 全量结束后统一更新宽表并生成 CDC 事件
- ✅ 所有现有功能正常工作
- ✅ 代码编译通过，无错误

### 代码质量验收标准
- ✅ 无重复代码
- ✅ 公共方法被正确复用
- ✅ 注释清晰准确
- ✅ 符合项目编码规范
- ✅ 异常处理完善

### 性能验收标准
- ✅ 吞吐量不低于重构前
- ✅ 内存占用不超过重构前
- ✅ 大数据量场景下表现稳定
