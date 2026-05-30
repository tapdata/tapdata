# Implementer Task 1: 改造 calculateAffectedBeforeKeys 方法

## Context

你正在修改 `AffectedKeyCalculator.java`，这是 DuckDB 宽表更新系统的一部分。当前 `calculateAffectedBeforeKeys` 方法只是简单提取 CDC 事件中的主键值，需要改造为：
1. 使用 SmartMerger 合并事件为 MergedRecord
2. 提取所有历史状态的 before 数据
3. 通过 WITH CTE SQL 查询宽表主键

## 关键依赖

- `SmartMerger.mergeEventsSmart()` - 合并 CDC 事件为 MergedRecord 列表
- `WithCteSqlGenerator.generateBatch()` - 生成 WITH CTE SQL
- `DuckDbOperator.executeQuery()` - 执行 SQL 查询
- `FromTableConfig` - 表配置（当前只有 tableName 和 primaryKey，需要添加 querySql 和 fields）

## 文件

- Modify: `/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculator.java`

## 任务

### Step 1: 新增 extractBeforeDataRows 辅助方法

```java
/**
 * 从 SmartMerger 合并结果中提取所有 before 数据行
 * 收集 operations 中所有 before/after 数据（排除 finalState），保证历史错误数据全被删除
 */
private List<Map<String, Object>> extractBeforeDataRows(List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
    List<Map<String, Object>> beforeRows = new ArrayList<>();
    String pkField = getSourceTablePrimaryKey(tableName);
    
    for (SmartMerger.MergedRecord record : mergedRecords) {
        for (Map<String, Object> op : record.getOperations()) {
            String opType = (String) op.get("op");
            if ("INSERT".equals(opType)) {
                // INSERT 事件的 value 作为 before 数据
                Object value = op.get("value");
                if (value instanceof Map) {
                    beforeRows.add(new HashMap<>((Map<String, Object>) value));
                }
            } else if ("UPDATE".equals(opType)) {
                // UPDATE 事件的 before 数据（从 old_pk 或 operations 中提取）
                Object oldPk = op.get("old_pk");
                if (oldPk != null) {
                    // 有主键变更，提取 before 数据
                    Map<String, Object> beforeRow = new HashMap<>();
                    beforeRow.put(pkField, oldPk);
                    // 复制其他字段（如果有）
                    Map<String, Object> fields = (Map<String, Object>) op.get("fields");
                    if (fields != null) {
                        beforeRow.putAll(fields);
                    }
                    beforeRows.add(beforeRow);
                }
            } else if ("DELETE".equals(opType)) {
                // DELETE 事件的 before 数据从 finalState 提取
                Map<String, Object> beforeRow = new HashMap<>(record.getFinalState());
                if (!beforeRow.isEmpty()) {
                    beforeRows.add(beforeRow);
                }
            }
        }
    }
    
    return beforeRows;
}
```

### Step 2: 新增 queryWideTablePksWithCte 方法

```java
/**
 * 使用 WITH CTE SQL 查询宽表主键
 * @param tableName 子表名
 * @param dataRows 数据行
 * @param fields 字段列表
 * @return 宽表主键集合
 */
private Set<Object> queryWideTablePksWithCte(String tableName, List<Map<String, Object>> dataRows, List<String> fields) throws SQLException {
    if (dataRows == null || dataRows.isEmpty()) {
        return Collections.emptySet();
    }
    
    // 获取 querySql（需要从外部传入或存储）
    String querySql = getQuerySqlForTable(tableName);
    if (querySql == null) {
        logger.warn("No querySql found for table {}", tableName);
        return Collections.emptySet();
    }
    
    // 生成 WITH CTE SQL
    String withSql = withCteSqlGenerator.generateBatch(querySql, tableName, dataRows, fields);
    
    // 执行查询
    List<Map<String, Object>> results = operator.executeQuery(withSql);
    
    // 提取宽表主键
    Set<Object> wideTablePks = new LinkedHashSet<>();
    for (Map<String, Object> row : results) {
        Object pk = row.get(wideTablePrimaryKey);
        if (pk != null) {
            wideTablePks.add(pk);
        }
    }
    
    return wideTablePks;
}
```

### Step 3: 新增 getQuerySqlForTable 方法

```java
/**
 * 获取表对应的 querySql
 * 从 fromTables 中查找匹配的表，返回其 querySql
 */
private String getQuerySqlForTable(String tableName) {
    // 遍历 fromTables 查找匹配的表
    for (FromTableConfig config : fromTables) {
        if (config.getTableName().equalsIgnoreCase(tableName)) {
            return config.getQuerySql();
        }
    }
    return null;
}
```

### Step 4: 新增 getTableFields 辅助方法

```java
/**
 * 获取表的字段列表
 * 从 FromTableConfig 中提取字段，如果没有配置则从事件中推断
 */
private List<String> getTableFields(String tableName) {
    for (FromTableConfig config : fromTables) {
        if (config.getTableName().equalsIgnoreCase(tableName)) {
            return config.getFields();
        }
    }
    // 回退：返回主键字段
    return Collections.singletonList(getSourceTablePrimaryKey(tableName));
}
```

### Step 5: 修改 calculateAffectedBeforeKeys 方法

将现有方法改造为：

```java
/**
 * 批量计算所有事件的 before 受影响主键集合
 * 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键
 * @param eventsByTable 按表名分组的 CDC 事件
 * @return 所有 before 主键集合（用于 DELETE 宽表记录）
 */
public Set<Object> calculateAffectedBeforeKeys(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
    Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
    
    for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
        String tableName = entry.getKey();
        List<Map<String, Object>> events = entry.getValue();
        
        if (events == null || events.isEmpty()) {
            continue;
        }
        
        // 使用 SmartMerger 合并事件
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
        if (mergedRecords.isEmpty()) {
            continue;
        }
        
        // 提取所有 before 数据行
        String pkField = getSourceTablePrimaryKey(tableName);
        List<String> fields = getTableFields(tableName);
        List<Map<String, Object>> beforeRows = extractBeforeDataRows(mergedRecords, tableName);
        
        if (beforeRows.isEmpty()) {
            continue;
        }
        
        // 使用 WITH CTE SQL 查询宽表主键
        Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
        affectedBeforeKeys.addAll(wideTablePks);
    }
    
    return affectedBeforeKeys;
}
```

## 注意事项

1. `FromTableConfig` 需要添加 `querySql` 和 `fields` 字段（这是 Task 3 的内容，但你需要在这里调用 `config.getQuerySql()` 和 `config.getFields()`，所以假设这些方法已存在）
2. 保持现有代码风格（使用 `LinkedHashSet` 保持插入顺序）
3. 使用 `logger` 记录调试信息
4. 所有方法都是 `private`，除了 `calculateAffectedBeforeKeys` 是 `public`

## 完成后

请报告：
- DONE: 所有方法已添加，代码编译通过
- DONE_WITH_CONCERNS: 完成但有疑虑
- NEEDS_CONTEXT: 需要更多信息
- BLOCKED: 无法完成
