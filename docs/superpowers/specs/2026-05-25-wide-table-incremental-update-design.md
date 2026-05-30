# DuckDB SQL 节点实时宽表增量更新系统设计文档

> **基于 WITH CTE 的 SQL 等价替换方案**

**版本**：V1.0  
**日期**：2026-05-25  
**状态**：设计中

---

## 1. 项目背景与目标

### 1.1 背景

当前 DuckDbSqlNode 实现宽表增量更新的方式存在以下问题：

- **SQL 解析复杂**：使用 JSqlParser 手动解析 JOIN 关系，CTE/子查询/UNION 难以处理
- **AffectedKeyCalculator 逻辑脆弱**：依赖用户配置 customJoinQueries，配置错误导致数据遗漏
- **多次查询开销大**：需要先查受影响主键，再重新查询宽表数据，两次查询
- **数据一致性风险**：两次查询之间源表可能发生变化，导致 before/after 不对齐

### 1.2 目标

设计并实现一套**零 SQL 解析、高性能、强一致性**的实时宽表增量更新系统：

- **零 SQL 解析**：完全复用原始业务 SQL，无手工解析误差
- **零临时表**：纯内存计算（WITH CTE），无 IO 开销
- **单次查询**：before/after 数据通过一次 SQL 执行获取
- **强一致性**：DuckDB 事务快照保证数据一致性
- **轻量实现**：仅依赖 DuckDB 内嵌引擎，无额外组件

---

## 2. 核心方案概述

### 2.1 核心原理

**基于 WITH CTE 的 SQL 等价替换**：

1. 接收 Tapdata 标准 CDC 事件（包含 before/after 数据）
2. 将 CDC 数据直接嵌入原始宽表 SQL 的 WITH 子句中
3. 一次性执行 SQL 生成宽表的 before/after 数据
4. 基于四态判断（INSERT/UPDATE/DELETE/SKIP）生成宽表 CDC 事件
5. 转换为 TapRecordEvent 输出到下游节点

### 2.2 核心优势

| 对比维度 | 原方案（SQL 解析） | 新方案（WITH CTE） |
|----------|-------------------|-------------------|
| SQL 解析 | 需要 JSqlParser 解析 JOIN | 零解析，直接复用原始 SQL |
| 复杂 SQL 支持 | CTE/子查询/UNION 难以处理 | 完全支持，不关心 SQL 复杂度 |
| 查询次数 | 2 次（查主键 + 查数据） | 2 次（before 查主键 + after 查数据） |
| 数据一致性 | 两次查询之间可能不一致 | 事务快照保证一致性 |
| 配置负担 | 需配置 customJoinQueries | 仅需配置 querySql 和 wideTablePrimaryKey |
| 实现复杂度 | 高（SQL 解析 + JOIN 关系推导） | 低（字符串拼接 + SQL 执行） |

### 2.3 技术栈

- **CDC 数据源**：Tapdata 标准 CDC 事件流（`TapInsertRecordEvent` / `TapUpdateRecordEvent` / `TapDeleteRecordEvent`）
- **SQL 引擎**：DuckDB（内嵌模式）
- **编程语言**：Java 17
- **并发框架**：Java 并发包

---

## 3. 系统架构设计

### 3.1 整体架构图

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Tapdata CDC 流  │───>│ CDC 事件过滤    │───>│ WITH CTE 生成   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ 输出到下游节点  │<───│ 四态判断        │<───│ SQL 执行模块    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 3.2 核心模块职责

| 模块名称 | 核心职责 |
|---------|---------|
| CDC 事件过滤 | 三层过滤：表过滤、字段过滤、无变化过滤 |
| WITH CTE 生成 | 将 CDC 数据嵌入 WITH 子句，生成 before/after SQL |
| SQL 执行模块 | 执行 before/after SQL，获取宽表数据 |
| 四态判断 | 根据 before/after 数据判断 INSERT/UPDATE/DELETE/SKIP |
| 输出模块 | 将宽表 CDC 事件转换为 TapRecordEvent 输出到下游 |

---

## 4. 详细设计

### 4.1 标准化 CDC 事件定义

复用 Tapdata 现有事件类型：

```java
// 插入事件
TapInsertRecordEvent {
    String tableName;
    Map<String, Object> after;  // 新数据
}

// 更新事件
TapUpdateRecordEvent {
    String tableName;
    Map<String, Object> before; // 旧数据
    Map<String, Object> after;  // 新数据
}

// 删除事件
TapDeleteRecordEvent {
    String tableName;
    Map<String, Object> before; // 旧数据
}
```

### 4.2 SQL 模板预编译

服务启动时预编译两个 SQL 模板，运行时仅替换 WITH 子句：

**用户原始 SQL**：
```sql
SELECT u.id, u.name, o.order_id, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.status = 1
```

**Before 模板**（只查宽表主键）：
```sql
WITH {tableName} AS ({valuesClause}) AS t({fields})
SELECT {wideTablePrimaryKey} FROM ({originalSqlWithoutSelect})
```

生成结果：
```sql
WITH users AS (VALUES (123, 'John', 1)) AS t(id, name, status)
SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.status = 1
```

**After 模板**（查完整宽表字段）：
```sql
WITH {tableName} AS ({valuesClause}) AS t({fields})
{originalSql}
```

生成结果：
```sql
WITH users AS (VALUES (123, 'John', 1)) AS t(id, name, status)
SELECT u.id, u.name, o.order_id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.status = 1
```

### 4.3 Before SQL 自动生成

从 `querySql` 自动生成 before SQL：

1. 提取 `wideTablePrimaryKey`（如 `"id"`）
2. 解析 `querySql` 的 SELECT 字段，找到主键字段（如 `u.id`）
3. 替换 SELECT 子句为 `SELECT {主键字段}`

**实现方式**：字符串替换

```java
String generateBeforeSql(String querySql, String wideTablePrimaryKey) {
    // 简单实现：替换 SELECT 后面的内容为 wideTablePrimaryKey
    // 假设 querySql = "SELECT u.id, u.name, o.order_id FROM ..."
    // wideTablePrimaryKey = "id"
    // 需要找到 u.id 并替换为 SELECT u.id
    // 注意：这里需要基础的 SQL 解析，但只处理 SELECT 子句
}
```

### 4.4 WITH CTE 生成

**单条 CDC 事件**：

```java
String generateWithCteSql(String originalSql, String tableName, 
                          Map<String, Object> rowData, List<String> fields) {
    // 生成 VALUES 子句
    String valuesClause = buildValuesClause(rowData, fields);
    
    // 拼接 WITH 子句
    return String.format("WITH %s AS (%s) AS t(%s) %s",
            tableName, valuesClause, String.join(", ", fields), originalSql);
}

String buildValuesClause(Map<String, Object> rowData, List<String> fields) {
    List<String> values = new ArrayList<>();
    for (String field : fields) {
        Object value = rowData.get(field);
        values.add(formatValue(value));
    }
    return "VALUES (" + String.join(", ", values) + ")";
}

String formatValue(Object value) {
    if (value == null) return "NULL";
    if (value instanceof String) return "'" + ((String) value).replace("'", "''") + "'";
    if (value instanceof Number) return value.toString();
    return "'" + value.toString() + "'";
}
```

**批量 CDC 事件**：

```java
String generateBatchWithCteSql(String originalSql, String tableName,
                               List<Map<String, Object>> rows, List<String> fields) {
    List<String> valueRows = new ArrayList<>();
    for (Map<String, Object> row : rows) {
        valueRows.add(buildValuesClause(row, fields));
    }
    
    String valuesClause = String.join(", ", valueRows);
    return String.format("WITH %s AS (%s) AS t(%s) %s",
            tableName, valuesClause, String.join(", ", fields), originalSql);
}
```

### 4.5 单条处理流程

```
1. 接收单条 CDC 事件
2. 前置三层过滤：
   - 表过滤：事件表是否在 fromTables 中
   - 字段过滤：变更字段是否影响宽表结果
   - 无变化过滤：UPDATE 事件中 before == after
3. 生成 WITH CTE SQL：
   - before SQL：WITH 源表 AS (VALUES beforeData) + before 模板
   - after SQL：WITH 源表 AS (VALUES afterData) + after 模板
4. 执行 before SQL，获取旧宽表主键集合
5. 执行 after SQL，获取新宽表数据集合
6. 四态判断：
   - 有旧无新 → DELETE
   - 无旧有新 → INSERT
   - 新旧都有 → UPDATE
   - 都无 → SKIP
7. 将宽表 CDC 事件转换为 TapRecordEvent 输出到下游
```

### 4.6 批量处理流程

```
1. CDC 事件按表名分组存入对应队列
2. 复用现有批处理参数触发：
   - 数量阈值：batchSize = 1000
   - 时间阈值：commitIntervalMs = 5000
3. 批量去重，保留同一主键最后一条事件
4. 生成批量 WITH CTE SQL：
   - before SQL：WITH 源表 AS (VALUES 多条 beforeData) + before 模板
   - after SQL：WITH 源表 AS (VALUES 多条 afterData) + after 模板
5. 一次执行 before SQL 获取所有旧主键
6. 一次执行 after SQL 获取所有新宽表数据
7. 四态判断，生成宽表 CDC 事件
8. 批量输出到下游节点
```

### 4.7 四态判断逻辑

| before 存在 | after 存在 | 操作类型 | 说明 |
|-----------|-----------|---------|------|
| 是 | 是 | UPDATE | 宽表记录需要更新 |
| 是 | 否 | DELETE | 宽表记录需要删除 |
| 否 | 是 | INSERT | 宽表记录需要插入 |
| 否 | 否 | SKIP | 宽表不受影响，跳过 |

### 4.8 宽表 CDC 事件定义

```java
@Data
public class WideTableCdcEvent {
    public enum OpType { INSERT, UPDATE, DELETE }
    
    private OpType opType;
    private Object primaryKey;           // 宽表主键值
    private Map<String, Object> data;    // 宽表完整数据（INSERT/UPDATE 时有值）
}
```

---

## 5. 核心类设计

### 5.1 WideTableIncrementalUpdater

```java
/**
 * 宽表增量更新器
 * 基于 WITH CTE 实现零 SQL 解析的宽表增量更新
 */
public class WideTableIncrementalUpdater {
    
    private final String querySql;              // 用户原始 SQL
    private final String wideTablePrimaryKey;   // 宽表主键
    private final String beforeSqlTemplate;     // Before SQL 模板
    private final String afterSqlTemplate;      // After SQL 模板
    private final List<FromTableConfig> fromTables; // 源表配置
    private final DuckDbOperator duckDbOperator;    // DuckDB 操作器
    
    /**
     * 初始化：预编译 SQL 模板
     */
    public WideTableIncrementalUpdater(String querySql, String wideTablePrimaryKey,
                                       List<FromTableConfig> fromTables,
                                       DuckDbOperator duckDbOperator) {
        this.querySql = querySql;
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.fromTables = fromTables;
        this.duckDbOperator = duckDbOperator;
        
        // 预编译 SQL 模板
        this.beforeSqlTemplate = generateBeforeSqlTemplate(querySql, wideTablePrimaryKey);
        this.afterSqlTemplate = querySql; // After 模板就是原始 SQL
    }
    
    /**
     * 处理单条 CDC 事件
     */
    public List<WideTableCdcEvent> processCdcEvent(TapRecordEvent event) {
        // 1. 前置过滤
        if (!shouldProcess(event)) {
            return Collections.emptyList();
        }
        
        // 2. 生成 before/after SQL
        String beforeSql = generateBeforeSql(event);
        String afterSql = generateAfterSql(event);
        
        // 3. 执行 SQL
        Set<Object> beforePks = executeBeforeSql(beforeSql);
        List<Map<String, Object>> afterData = executeAfterSql(afterSql);
        
        // 4. 四态判断
        return fourStateJudge(beforePks, afterData);
    }
    
    /**
     * 处理批量 CDC 事件
     */
    public List<WideTableCdcEvent> processBatchCdcEvents(List<TapRecordEvent> events) {
        // 1. 按表分组
        Map<String, List<TapRecordEvent>> eventsByTable = groupByTable(events);
        
        // 2. 对每个表生成批量 WITH CTE SQL
        // 3. 执行 before/after SQL
        // 4. 四态判断
        // 5. 返回宽表 CDC 事件列表
    }
    
    // ... 其他方法
}
```

### 5.2 WithCteSqlGenerator

```java
/**
 * WITH CTE SQL 生成器
 * 负责将 CDC 数据嵌入 WITH 子句
 */
public class WithCteSqlGenerator {
    
    /**
     * 生成单条 WITH CTE SQL
     */
    public String generateSingle(String sqlTemplate, String tableName,
                                 Map<String, Object> rowData, List<String> fields) {
        String valuesClause = buildValuesClause(rowData, fields);
        return String.format("WITH %s AS (%s) AS t(%s) %s",
                tableName, valuesClause, String.join(", ", fields), sqlTemplate);
    }
    
    /**
     * 生成批量 WITH CTE SQL
     */
    public String generateBatch(String sqlTemplate, String tableName,
                                List<Map<String, Object>> rows, List<String> fields) {
        List<String> valueRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            valueRows.add(buildValuesClause(row, fields));
        }
        String valuesClause = String.join(", ", valueRows);
        return String.format("WITH %s AS (%s) AS t(%s) %s",
                tableName, valuesClause, String.join(", ", fields), sqlTemplate);
    }
    
    /**
     * 构建 VALUES 子句
     */
    private String buildValuesClause(Map<String, Object> rowData, List<String> fields) {
        List<String> values = new ArrayList<>();
        for (String field : fields) {
            values.add(formatValue(rowData.get(field)));
        }
        return "VALUES (" + String.join(", ", values) + ")";
    }
    
    /**
     * 格式化值（处理字符串转义、NULL 等）
     */
    private String formatValue(Object value) {
        if (value == null) return "NULL";
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        return "'" + value.toString() + "'";
    }
}
```

### 5.3 FourStateJudge

```java
/**
 * 四态判断器
 * 根据 before/after 数据判断 INSERT/UPDATE/DELETE/SKIP
 */
public class FourStateJudge {
    
    private final String wideTablePrimaryKey;
    
    /**
     * 四态判断
     */
    public List<WideTableCdcEvent> judge(Set<Object> beforePks, 
                                         List<Map<String, Object>> afterData) {
        List<WideTableCdcEvent> events = new ArrayList<>();
        
        Set<Object> afterPks = extractPrimaryKeys(afterData);
        
        // 有旧无新 → DELETE
        for (Object pk : beforePks) {
            if (!afterPks.contains(pk)) {
                events.add(new WideTableCdcEvent(DELETE, pk, null));
            }
        }
        
        // 无旧有新 → INSERT
        // 新旧都有 → UPDATE
        for (Map<String, Object> row : afterData) {
            Object pk = row.get(wideTablePrimaryKey);
            if (beforePks.contains(pk)) {
                events.add(new WideTableCdcEvent(UPDATE, pk, row));
            } else {
                events.add(new WideTableCdcEvent(INSERT, pk, row));
            }
        }
        
        return events;
    }
    
    private Set<Object> extractPrimaryKeys(List<Map<String, Object>> data) {
        Set<Object> pks = new HashSet<>();
        for (Map<String, Object> row : data) {
            pks.add(row.get(wideTablePrimaryKey));
        }
        return pks;
    }
}
```

---

## 6. 数据流程

### 6.1 完整数据流

```
┌─────────────────────────────────────────────────────────────────┐
│ DuckDbSqlNode.doInit()                                          │
│ 1. 解析 querySql，提取源表字段列表                              │
│ 2. 预编译 beforeSqlTemplate（只查宽表主键）                     │
│ 3. 初始化 WideTableIncrementalUpdater                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ DuckDbSqlNode.onTapEvent() - 接收 CDC 事件                      │
│ 1. 将事件加入缓冲区                                             │
│ 2. 检查是否触发批处理（batchSize 或 commitIntervalMs）          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ WideTableIncrementalUpdater.processBatchCdcEvents()              │
│ 1. 按表分组 CDC 事件                                            │
│ 2. 对每个表生成批量 WITH CTE SQL                                │
│ 3. 执行 before SQL → 获取旧主键集合                             │
│ 4. 执行 after SQL → 获取新宽表数据                              │
│ 5. 四态判断 → 生成宽表 CDC 事件列表                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ DuckDbSqlNode.outputWideTableEvents()                           │
│ 1. 将宽表 CDC 事件转换为 TapRecordEvent                         │
│ 2. 批量输出到下游节点                                           │
│ 3. 提交事务                                                     │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 单表 CDC 事件处理示例

**用户 SQL**：
```sql
SELECT u.id, u.name, o.order_id, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.status = 1
```

**宽表主键**：`id`

**收到 CDC 事件**：
```java
TapUpdateRecordEvent {
    tableName: "users",
    before: { id: 123, name: "John", status: 1 },
    after: { id: 123, name: "John Updated", status: 1 }
}
```

**生成 before SQL**：
```sql
WITH users AS (VALUES (123, 'John', 1)) AS t(id, name, status)
SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.status = 1
```

**执行 before SQL**：
```
结果：[123]  // 旧宽表主键集合
```

**生成 after SQL**：
```sql
WITH users AS (VALUES (123, 'John Updated', 1)) AS t(id, name, status)
SELECT u.id, u.name, o.order_id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.status = 1
```

**执行 after SQL**：
```
结果：[
  { id: 123, name: "John Updated", order_id: 1001, amount: 99.9 },
  { id: 123, name: "John Updated", order_id: 1002, amount: 199.9 }
]
```

**四态判断**：
- beforePks = [123]
- afterPks = [123, 123]（同一用户有多个订单）
- 结果：UPDATE 事件（主键 123 的宽表数据需要更新）

**输出到下游**：
```java
TapUpdateRecordEvent {
    tableName: "duckdb_output",
    before: { id: 123, name: "John", order_id: 1001, amount: 99.9 },
    after: { id: 123, name: "John Updated", order_id: 1001, amount: 99.9 }
}
TapUpdateRecordEvent {
    tableName: "duckdb_output",
    before: { id: 123, name: "John", order_id: 1002, amount: 199.9 },
    after: { id: 123, name: "John Updated", order_id: 1002, amount: 199.9 }
}
```

---

## 7. 性能优化方案

### 7.1 第一梯队（必做，收益 90%+）

1. **三层前置过滤**：过滤非依赖表、非影响字段、无变化事件，减少 80%~95% 无效计算
2. **BEFORE 只查主键**：JOIN 计算量减少 70%+
3. **预编译 SQL 模板**：避免运行时字符串拼接与解析开销
4. **DuckDB 只读事务**：查询速度提升 3~5 倍

### 7.2 第二梯队（推荐，收益 5~10 倍）

1. **单线程单连接**：避免 DuckDB 多线程锁竞争，性能提升 40%~100%
2. **关闭自动检查点**：关闭 DuckDB 自动写盘检查点，性能提升 20%~50%
3. **强制内存模式**：所有计算在内存完成，不写临时文件
4. **宽表主键索引**：增删改操作从 O(n) 提升到 O(1)

### 7.3 第三梯队（高并发专用，收益 2~5 倍）

1. **快速路径优化**：JOIN KEY 和 WHERE 条件不变时，不执行 before SQL
2. **关联表本地缓存**：缓存几乎不变的从表数据，避免频繁查库
3. **批量处理**：吞吐量提升 5~20 倍

---

## 8. 异常处理与可靠性保证

### 8.1 事务保证

- 单条/批量处理均在一个 DuckDB 事务中完成
- 任何异常发生时自动回滚，保证数据一致性

### 8.2 死信队列（DLQ）

- 处理失败的 CDC 事件写入 DuckDB 死信队列表
- 支持手动/自动重试
- 死信队列包含事件原始数据、异常信息、时间戳

### 8.3 幂等性保证

- 基于宽表主键进行更新，重复事件不重复处理
- 批量处理时按主键去重，保留最后一条事件

### 8.4 监控告警

- 监控指标：处理成功率、平均耗时、吞吐量、死信队列长度
- 告警规则：成功率低于 99.9%、死信队列长度超过阈值、处理耗时超过 100ms

---

## 9. 与现有代码的集成

### 9.1 修改 DuckDbSqlNode

**新增组件**：
- `WideTableIncrementalUpdater`：宽表增量更新器
- `WithCteSqlGenerator`：WITH CTE SQL 生成器
- `FourStateJudge`：四态判断器

**移除组件**：
- `AffectedKeyCalculator`：不再需要计算受影响主键
- `IncrementalViewUpdater`：不再需要增量视图更新

**修改流程**：
- `doInit()`：初始化新组件，预编译 SQL 模板
- `onTapEvent()`：将 CDC 事件传递给 `WideTableIncrementalUpdater`
- `processBatch()`：调用 `processBatchCdcEvents()` 获取宽表 CDC 事件
- `output()`：将宽表 CDC 事件转换为 TapRecordEvent 输出

### 9.2 保留现有批处理机制

- 复用 `batchSize = 1000` 和 `commitIntervalMs = 5000`
- 复用 `eventBuffer` 和 `flushBuffer()` 逻辑
- 仅修改缓冲区刷新后的处理逻辑

---

## 10. 测试计划

### 10.1 功能测试

- 单表 INSERT/UPDATE/DELETE 测试
- 多表 JOIN 测试
- JOIN KEY 变更测试
- WHERE 条件进出测试
- 批量处理测试
- 异常处理测试

### 10.2 性能测试

- 单条处理耗时测试
- 批量处理吞吐量测试
- 不同批量大小性能对比
- 内存占用测试

### 10.3 一致性测试

- 全量数据对比：宽表数据与原始 SQL 执行结果对比
- 长时间运行一致性测试：连续运行 72 小时，数据无差异

---

## 11. 实施计划

| 阶段 | 任务 | 预计时间 |
|------|------|----------|
| 1 | 创建 WithCteSqlGenerator 类 | 1 小时 |
| 2 | 创建 FourStateJudge 类 | 0.5 小时 |
| 3 | 创建 WideTableIncrementalUpdater 类 | 2 小时 |
| 4 | 修改 DuckDbSqlNode 集成新组件 | 2 小时 |
| 5 | 编写单元测试 | 2 小时 |
| 6 | 集成测试与性能测试 | 2 小时 |
| 7 | 移除 AffectedKeyCalculator 和 IncrementalViewUpdater | 1 小时 |

**总计**：约 10.5 小时

---

## 12. 风险评估与应对措施

| 风险点 | 影响程度 | 应对措施 |
|-------|---------|---------|
| SQL 过长导致执行失败 | 低 | 限制批量大小，拆分过大的批量 |
| VALUES 语法不支持复杂类型 | 中 | 测试 DuckDB 对 JSON/ARRAY 等类型的支持 |
| 源表字段与 SQL 字段不匹配 | 高 | 启动时验证字段一致性 |
| DuckDB 内存溢出 | 中 | 限制批量队列最大容量，设置内存上限 |

---

## 13. 总结

本方案通过 **WITH CTE SQL 等价替换** 技术，实现了零 SQL 解析、高性能、强一致性的实时宽表增量更新。相比原方案（SQL 解析 + AffectedKeyCalculator），具有以下优势：

- **零 SQL 解析**：完全复用原始业务 SQL，支持任意复杂度的 SQL
- **实现简单**：字符串拼接 + SQL 执行，无复杂逻辑
- **数据一致性强**：事务快照保证 before/after 对齐
- **性能优异**：预编译模板 + 批量处理 + 前置过滤
