# DuckDB 预览模式临时表设计

## 1. 背景

在任务预览模式下，DuckDB SQL 节点会创建临时表用于数据处理，但目前这些临时表是用 `CREATE TABLE` 创建的永久表，需要在任务结束时手动清理。为了更好的资源管理，我们希望在预览模式下使用真正的 DuckDB 临时表（`CREATE TEMP TABLE`）。

## 2. 需求

1. **预览模式**：使用 `CREATE TEMP TABLE`（临时表），连接关闭时自动删除
2. **正常模式**：使用 `CREATE TABLE`（正式表），持久化存储
3. **DuckLake 支持**：两种模式都要支持 DuckLake
4. **代码一致性**：参考 JS 节点的实现，保持架构一致

## 3. 设计方案

### 3.1 修改 `DuckDbOperator` 接口

**文件**：`/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperator.java`

**修改**：
- 给 `createTempTable()` 方法增加 `useTempTable` 参数
- 给 `createTable()` 方法增加 `useTempTable` 参数（可选）

**修改前**：
```java
void createTempTable(TapTable tapTable, String tempTableName) throws SQLException;
```

**修改后**：
```java
void createTempTable(TapTable tapTable, String tempTableName, boolean useTempTable) throws SQLException;
```

### 3.2 修改 `DuckDbOperatorImpl` 实现

**文件**：`/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperatorImpl.java`

**修改**：
- 实现新的 `createTempTable()` 方法
- 修改 `createTable()` 方法，根据 `useTempTable` 参数选择 SQL 语法
- 修改 `buildDuckLakeCreateTableSql()` 方法，支持 `TEMP` 选项

**关键修改**：
```java
private void createTable(TapTable tapTable, String tableName, boolean useTempTable) throws SQLException {
    StringBuilder sql = new StringBuilder();
    if (useTempTable) {
        sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (");
    } else {
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
    }
    // ... 其余逻辑不变
}
```

### 3.3 修改 `DuckDbSqlNode`

**文件**：`/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/DuckDbSqlNode.java`

**修改**：
- 在调用 `createTempTable()` 时，传入预览模式信息
- 通过 `processorBaseContext.getTaskDto().isPreviewTask()` 判断是否是预览模式

**关键修改**：
```java
private void ensureTableExists(List<Map<String, Object>> data) throws SQLException {
    // ... 现有逻辑
    
    if (tapTable != null && tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
        String tempTableName = "temp_" + currentTableName;
        boolean isPreview = processorBaseContext.getTaskDto() != null 
            && processorBaseContext.getTaskDto().isPreviewTask();
        duckDbOperator.createTempTable(tapTable, tempTableName, isPreview);
        // ... 其余逻辑
    }
}

private void ensureTableExists(PerSourceContext context, List<Map<String, Object>> data) throws SQLException {
    // ... 现有逻辑
    
    if (tapTable != null && tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
        DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
        boolean isPreview = processorBaseContext.getTaskDto() != null 
            && processorBaseContext.getTaskDto().isPreviewTask();
        operator.createTempTable(tapTable, context.getTargetTableName(), isPreview);
        // ... 其余逻辑
    }
}
```

### 3.4 修改 `ArrowWriter` 中的 DuckLake 创建逻辑

**文件**：`/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/ArrowWriter.java`

**修改**：
- 修改 `buildDuckLakeCreateTableSql()` 方法，增加 `useTempTable` 参数
- 修改 `createDuckLakeTableIfNotExists()` 方法，增加 `useTempTable` 参数

## 4. 验证

1. 编译验证通过
2. 预览模式下使用临时表
3. 正常模式下使用永久表
4. DuckLake 在两种模式下都工作正常

## 5. 为什么这个方案是对的？

1. **自动清理**：DuckDB 临时表在连接关闭时自动删除，无需手动管理
2. **最小改动**：只需要修改接口和实现，不影响核心逻辑
3. **DuckLake 支持**：方案覆盖了 DuckLake 场景
4. **与现有代码一致**：参考 JS 节点的实现，不修改预览结果的处理流程
