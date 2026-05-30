# AffectedKeyCalculator 重构与 WideTableIncrementalUpdater 融合设计文档

> **基于 before/after 主键分离的宽表增量更新方案**

**版本**：V2.0  
**日期**：2026-05-25  
**状态**：已确认

---

## 1. 项目背景与目标

### 1.1 背景

现有 `AffectedKeyCalculator.extractPrimaryKey` 方法存在以下 Bug：

1. **主键更新场景丢失旧主键**：当源表主键被更新时（如 `id: 123 → 456`），只提取 after 中的新主键，旧主键对应的宽表记录不会被删除
2. **从表 JOIN KEY 更新遗漏**：当从表的 JOIN KEY 被更新时，只查询新 JOIN KEY 对应的主表主键，旧 JOIN KEY 对应的宽表记录不会更新
3. **extractPrimaryKey 优先级混乱**：顶层、after、before、o2、o 的提取顺序不清晰，可能导致取错值

### 1.2 目标

重构 `AffectedKeyCalculator`，融合 `WideTableIncrementalUpdater` 的 WITH CTE 优势：

- **主键分离提取**：拆分为 `extractBeforePrimaryKey` 和 `extractAfterPrimaryKey`
- **主键更新支持**：准确处理 beforePk ≠ afterPk 场景
- **JOIN KEY 更新支持**：分别查询 before/after JOIN KEY 对应的主表主键
- **减少查询开销**：before 数据直接从 CDC 事件获取，不需要查询
- **零假设**：不假设宽表主键=源表主键，不假设 JOIN KEY 不变

---

## 2. 核心方案概述

### 2.1 核心原理

**before/after 主键分离 + WITH CTE SQL 等价替换**：

1. 从 CDC 事件分别提取 beforePk 和 afterPk
2. beforePk → 直接 DELETE 宽表记录（不需要查询）
3. afterPk → WITH CTE + querySql → 查询原表 → INSERT/UPDATE 宽表
4. 从表事件：分别查询 before/after JOIN KEY 对应的主表主键

### 2.2 核心优势

| 对比维度 | 原方案 | 新方案 |
|----------|--------|--------|
| 主键更新 | 丢失旧主键，数据不一致 | beforePk DELETE + afterPk INSERT |
| JOIN KEY 更新 | 只查询新 JOIN KEY | 分别查询 before/after JOIN KEY |
| before 数据查询 | 需要执行 before SQL | 直接从 CDC 事件获取 |
| 假设条件 | 假设宽表主键=源表主键 | 零假设，完全基于 CDC 数据 |
| 字段列表 | 需要用户配置 | 自动推断（CDC 事件 → DuckDB 元数据） |

### 2.3 技术栈

- **CDC 数据源**：Tapdata 标准 CDC 事件流
- **SQL 引擎**：DuckDB（内嵌模式）
- **编程语言**：Java 17
- **元数据查询**：DuckDB `information_schema.columns`

---

## 3. 系统架构设计

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│ AffectedKeyCalculator（重构版）                                  │
│                                                                 │
│ 1. extractBeforePrimaryKey(event, pkField) → Optional<Object>  │
│ 2. extractAfterPrimaryKey(event, pkField) → Optional<Object>   │
│ 3. 主键更新检测：beforePk ≠ afterPk                            │
│ 4. 从表 JOIN KEY 更新：分别查询 before/after 对应的主表主键    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ WideTableIncrementalUpdater（重构版）                            │
│                                                                 │
│ 1. beforePk → 直接 DELETE 宽表记录（不需要查询）                │
│ 2. afterPk → WITH CTE + querySql → 查询原表 → INSERT/UPDATE    │
│ 3. 字段列表：CDC 事件推断 → DuckDB 元数据回退                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 核心模块职责

| 模块名称 | 核心职责 |
|---------|---------|
| AffectedKeyCalculator | 提取 before/after 主键，计算受影响的主表主键集合 |
| WideTableIncrementalUpdater | 编排 before DELETE + after WITH CTE 查询 + 宽表更新 |
| WithCteSqlGenerator | 将 CDC 数据嵌入 WITH 子句，生成完整 SQL |
| FourStateJudge | 根据 before/after 数据判断 INSERT/UPDATE/DELETE/SKIP |
| SchemaResolver | 从 CDC 事件或 DuckDB 元数据解析源表字段列表 |

---

## 4. 详细设计

### 4.1 AffectedKeyCalculator 重构

#### 4.1.1 extractBeforePrimaryKey

```java
/**
 * 从 CDC 事件提取 before 主键（宽表改动前）
 * @param event CDC 事件
 * @param pkField 主键字段名
 * @return Optional 包含 before 主键值，如果不存在则返回 empty
 */
public Optional<Object> extractBeforePrimaryKey(Map<String, Object> event, String pkField) {
    // 1. 优先从 before 字段提取（DELETE/UPDATE 事件）
    Object before = event.get("before");
    if (before instanceof Map) {
        Object pk = ((Map<?, ?>) before).get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
    }
    
    // 2. 回退到顶层直接访问（某些 CDC 格式）
    Object pk = event.get(pkField);
    if (pk != null) {
        return Optional.of(pk);
    }
    
    // 3. MongoDB 风格 o2/o 字段
    Object o2 = event.get("o2");
    if (o2 instanceof Map) {
        Object pk2 = ((Map<?, ?>) o2).get(pkField);
        if (pk2 != null) {
            return Optional.of(pk2);
        }
    }
    
    Object o = event.get("o");
    if (o instanceof Map) {
        Object pk3 = ((Map<?, ?>) o).get(pkField);
        if (pk3 != null) {
            return Optional.of(pk3);
        }
    }
    
    return Optional.empty();
}
```

#### 4.1.2 extractAfterPrimaryKey

```java
/**
 * 从 CDC 事件提取 after 主键（宽表改动后）
 * @param event CDC 事件
 * @param pkField 主键字段名
 * @return Optional 包含 after 主键值，如果不存在则返回 empty
 */
public Optional<Object> extractAfterPrimaryKey(Map<String, Object> event, String pkField) {
    // 1. 优先从 after 字段提取（INSERT/UPDATE 事件）
    Object after = event.get("after");
    if (after instanceof Map) {
        Object pk = ((Map<?, ?>) after).get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
    }
    
    // 2. 回退到顶层直接访问
    Object pk = event.get(pkField);
    if (pk != null) {
        return Optional.of(pk);
    }
    
    return Optional.empty();
}
```

#### 4.1.3 主键更新检测

```java
/**
 * 检测主键是否更新
 * @return true 表示主键更新（beforePk ≠ afterPk）
 */
public boolean isPrimaryKeyUpdated(Map<String, Object> event, String pkField) {
    Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
    Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
    
    // 只有 before 和 after 都存在且不相等时，才是主键更新
    return beforePk.isPresent() && afterPk.isPresent() 
            && !Objects.equals(beforePk.get(), afterPk.get());
}
```

### 4.2 WideTableIncrementalUpdater 重构

#### 4.2.1 批量处理策略

**核心思想**：所有表一起处理，统一计算 before/after 受影响主键集合，一次执行宽表更新。

```
CDC 事件缓冲区（多表混合）
  ├─ calculateAffectedBeforeKeys(allEvents) → Set<Object> affectedBeforeKeys
  │   └─ 所有表的 before 主键集合（主表直接提取 + 从表通过 JOIN 查询）
  │
  └─ calculateAffectedAfterKeys(allEvents) → Set<Object> affectedAfterKeys
      └─ 所有表的 after 主键集合（主表直接提取 + 从表通过 JOIN 查询）
          │
          ▼
  updateWideTable(affectedBeforeKeys, affectedAfterKeys)
      ├─ DELETE FROM wideTable WHERE pk IN (affectedBeforeKeys - affectedAfterKeys)
      └─ WITH CTE + querySql → 查询 affectedAfterKeys → INSERT/UPDATE wideTable
```

**优势**：
- **减少 SQL 执行次数**：从 N 次（每条事件一次）降低到 2 次（before DELETE + after INSERT/UPDATE）
- **批量处理性能提升**：DuckDB 批量 IN 查询比单条查询快 5~20 倍
- **简化逻辑**：不区分主表/从表，统一处理

#### 4.2.2 calculateAffectedBeforeKeys

```java
/**
 * 批量计算所有事件的 before 受影响主键集合
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 before 主键集合（用于 DELETE 宽表记录）
 */
public Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable) {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        String pkField = getTablePrimaryKey(tableName);
        
        for (Map<String, Object> event : events) {
            Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
            beforePk.ifPresent(affectedBeforeKeys::add);
        }
    }
    
    return affectedBeforeKeys;
}
```

#### 4.2.3 calculateAffectedAfterKeys

```java
/**
 * 批量计算所有事件的 after 受影响主键集合
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 after 主键集合（用于 INSERT/UPDATE 宽表记录）
 */
public Set<Object> calculateAffectedAfterKeys(Map<String, List<Map<String, Object>>> eventsByTable) {
    Set<Object> affectedAfterKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        String pkField = getTablePrimaryKey(tableName);
        
        for (Map<String, Object> event : events) {
            Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
            afterPk.ifPresent(affectedAfterKeys::add);
        }
    }
    
    return affectedAfterKeys;
}
```

#### 4.2.4 updateWideTable 批量更新

```java
/**
 * 批量更新宽表
 * @param affectedBeforeKeys before 受影响主键集合（DELETE）
 * @param affectedAfterKeys after 受影响主键集合（INSERT/UPDATE）
 */
public List<WideTableCdcEvent> updateWideTable(Set<Object> affectedBeforeKeys, Set<Object> affectedAfterKeys) {
    List<WideTableCdcEvent> events = new ArrayList<>();
    
    // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
    Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
    pureDeleteKeys.removeAll(affectedAfterKeys);
    
    // 2. 生成 DELETE 事件
    for (Object pk : pureDeleteKeys) {
        events.add(new WideTableCdcEvent(DELETE, pk, null));
    }
    
    // 3. 计算需要查询的主键（在 after 中）
    if (!affectedAfterKeys.isEmpty()) {
        // 获取字段列表
        List<String> fields = schemaResolver.resolveFieldsFromEvents(eventsByTable);
        
        // 生成批量 WITH CTE SQL
        String afterSql = withCteSqlGenerator.generateBatch(
                querySql, affectedAfterKeys, fields);
        
        // 执行查询
        List<Map<String, Object>> results = duckDbOperator.executeQuery(afterSql);
        
        // 生成 INSERT/UPDATE 事件
        for (Map<String, Object> row : results) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk != null) {
                events.add(new WideTableCdcEvent(INSERT, pk, row));
            }
        }
    }
    
    return events;
}
```

### 4.3 SchemaResolver 设计

```java
/**
 * 源表字段列表解析器
 * 优先从 CDC 事件推断，回退到 DuckDB 元数据查询
 */
public class SchemaResolver {
    
    private final DuckDbOperator duckDbOperator;
    private final Map<String, List<String>> schemaCache = new ConcurrentHashMap<>();
    
    /**
     * 解析源表字段列表
     */
    public List<String> resolveFields(String tableName, Map<String, Object> event) {
        // 1. 优先从 after 数据推断
        Object after = event.get("after");
        if (after instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) after).keySet());
            if (!fields.isEmpty()) {
                return fields;
            }
        }
        
        // 2. 从 before 数据推断
        Object before = event.get("before");
        if (before instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) before).keySet());
            if (!fields.isEmpty()) {
                return fields;
            }
        }
        
        // 3. 从 DuckDB 元数据查询
        return resolveFieldsFromMetadata(tableName);
    }
    
    /**
     * 从 DuckDB information_schema 查询字段列表
     */
    private List<String> resolveFieldsFromMetadata(String tableName) {
        return schemaCache.computeIfAbsent(tableName, table -> {
            try {
                String sql = String.format(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
                        table);
                List<Map<String, Object>> results = duckDbOperator.executeQuery(sql);
                return results.stream()
                        .map(row -> (String) row.get("column_name"))
                        .collect(Collectors.toList());
            } catch (SQLException e) {
                logger.warn("Failed to resolve fields from metadata for table {}: {}", table, e.getMessage());
                return Collections.emptyList();
            }
        });
    }
}
```

---

## 5. 数据流程

### 5.1 批量处理完整流程

**CDC 事件缓冲区**（多表混合）：
```json
{
  "users": [
    { "before": { "id": 123, "name": "John" }, "after": { "id": 456, "name": "John" } },
    { "before": { "id": 789, "name": "Jane" }, "after": { "id": 789, "name": "Jane Updated" } }
  ],
  "orders": [
    { "before": { "order_id": 1, "user_id": 123 }, "after": { "order_id": 1, "user_id": 456 } }
  ]
}
```

**处理流程**：
```
1. calculateAffectedBeforeKeys(allEvents)
   ├─ users 表: before id=123, id=789 → {123, 789}
   └─ orders 表: before user_id=123 → 查询主表 → {123}
   → affectedBeforeKeys = {123, 789}

2. calculateAffectedAfterKeys(allEvents)
   ├─ users 表: after id=456, id=789 → {456, 789}
   └─ orders 表: after user_id=456 → 查询主表 → {456}
   → affectedAfterKeys = {456, 789}

3. updateWideTable(affectedBeforeKeys, affectedAfterKeys)
   ├─ pureDeleteKeys = {123, 789} - {456, 789} = {123}
   │   └─ DELETE FROM wideTable WHERE pk = 123
   │
   └─ WITH CTE + querySql → 查询 id IN (456, 789) → INSERT/UPDATE
```

**输出事件**：
```
WideTableCdcEvent{op=DELETE, pk=123, data=null}
WideTableCdcEvent{op=INSERT, pk=456, data={id: 456, name: "John", ...}}
WideTableCdcEvent{op=UPDATE, pk=789, data={id: 789, name: "Jane Updated", ...}}
```

### 5.2 主键更新场景（批量处理）

**CDC 事件**：
```json
{
  "users": [
    { "before": { "id": 123, "name": "John" }, "after": { "id": 456, "name": "John" } }
  ]
}
```

**处理流程**：
```
1. calculateAffectedBeforeKeys → {123}
2. calculateAffectedAfterKeys → {456}
3. pureDeleteKeys = {123} - {456} = {123}
4. DELETE 宽表记录 WHERE pk = 123
5. WITH CTE + querySql → 查询 id=456 → INSERT 宽表记录
```

### 5.3 JOIN KEY 更新场景（批量处理）

**CDC 事件**：
```json
{
  "orders": [
    { "before": { "order_id": 1, "user_id": 123 }, "after": { "order_id": 1, "user_id": 456 } }
  ]
}
```

**处理流程**：
```
1. calculateAffectedBeforeKeys
   └─ before user_id=123 → customJoinQuery(123) → {123}
   → affectedBeforeKeys = {123}

2. calculateAffectedAfterKeys
   └─ after user_id=456 → customJoinQuery(456) → {456}
   → affectedAfterKeys = {456}

3. pureDeleteKeys = {123} - {456} = {123}
4. DELETE 宽表记录 WHERE pk = 123
5. WITH CTE + querySql → 查询 user_id=456 → INSERT 宽表记录
```

---

## 6. 性能优化方案

### 6.1 第一梯队（必做，收益 90%+）

1. **before 数据不查询**：直接从 CDC 事件获取，减少 50% SQL 执行
2. **预编译 SQL 模板**：避免运行时字符串拼接与解析开销
3. **DuckDB 只读事务**：查询速度提升 3~5 倍

### 6.2 第二梯队（推荐，收益 5~10 倍）

1. **单线程单连接**：避免 DuckDB 多线程锁竞争
2. **字段列表缓存**：避免重复查询 information_schema
3. **批量处理**：吞吐量提升 5~20 倍

---

## 7. 异常处理与可靠性保证

### 7.1 主键提取失败

- 如果 beforePk 和 afterPk 都为空，记录 WARN 日志并跳过该事件
- 不会抛出异常，保证 CDC 流不中断

### 7.2 字段列表解析失败

- 如果 CDC 事件和 DuckDB 元数据都无法获取字段列表，记录 ERROR 日志
- 回退到使用 `customJoinQueries` 中的字段（如果有）

### 7.3 事务保证

- 宽表更新在单个 DuckDB 事务中完成
- 任何异常发生时自动回滚

---

## 8. 与现有代码的集成

### 8.1 修改 AffectedKeyCalculator

- 拆分 `extractPrimaryKey` 为 `extractBeforePrimaryKey` 和 `extractAfterPrimaryKey`
- 新增 `isPrimaryKeyUpdated` 方法
- 保留 `queryRelatedMainTablePks` 和 `executeCustomJoinQuery` 方法

### 8.2 修改 WideTableIncrementalUpdater

- 重构 `processCdcEvent` 方法，使用新的 before/after 分离逻辑
- 集成 `SchemaResolver` 自动解析字段列表
- 移除 `BeforeSqlTemplateGenerator`（不再需要 before SQL 模板）

### 8.3 保留现有组件

- `WithCteSqlGenerator`：继续用于生成 after SQL
- `FourStateJudge`：继续用于四态判断（但逻辑简化，因为 before 数据已知）

---

## 9. 测试计划

### 9.1 功能测试

- 主键更新场景测试（beforePk ≠ afterPk）
- JOIN KEY 更新场景测试
- INSERT/UPDATE/DELETE 标准场景测试
- 字段列表自动推断测试
- DuckDB 元数据回退测试

### 9.2 性能测试

- before 数据不查询的性能提升验证
- 批量处理吞吐量测试
- 字段列表缓存命中率测试

### 9.3 一致性测试

- 全量数据对比：宽表数据与原始 SQL 执行结果对比
- 主键更新后数据一致性验证

---

## 10. 实施计划

| 阶段 | 任务 | 预计时间 |
|------|------|----------|
| 1 | 重构 AffectedKeyCalculator：拆分 extractBeforePrimaryKey/extractAfterPrimaryKey | 1 小时 |
| 2 | 新增 calculateAffectedBeforeKeys/calculateAffectedAfterKeys 批量方法 | 1 小时 |
| 3 | 创建 SchemaResolver 类 | 0.5 小时 |
| 4 | 重构 WideTableIncrementalUpdater.updateWideTable 批量更新 | 2 小时 |
| 5 | 编写单元测试（批量处理、主键更新、JOIN KEY 更新） | 2 小时 |
| 6 | 集成测试与性能测试 | 2 小时 |
| 7 | 移除旧的单条事件处理逻辑 | 0.5 小时 |

**总计**：约 9 小时

---

## 11. 总结

本方案通过 **before/after 主键分离 + 批量统一处理** 技术，解决了原 `AffectedKeyCalculator.extractPrimaryKey` 在主键更新和 JOIN KEY 更新场景下的 Bug。相比原方案，具有以下优势：

- **零假设**：不假设宽表主键=源表主键，不假设 JOIN KEY 不变
- **减少查询**：before 数据直接从 CDC 事件获取，不需要执行 before SQL
- **批量处理**：所有表一起处理，SQL 执行次数从 N 次降低到 2 次，性能提升 5~20 倍
- **准确处理更新**：beforePk 和 afterPk 分别处理，保证数据一致性
- **自动推断字段**：不需要用户手动配置字段列表
