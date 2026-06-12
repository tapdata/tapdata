# WITH CTE SQL 生成器设计文档

> **增强 WideTableIncrementalUpdater 的 SQL 生成能力**

**版本**：V1.0  
**日期**：2026-05-26  
**状态**：设计中

---

## 1. 项目背景

### 1.1 当前状态

已完成 AffectedKeyCalculator 批量重构：
- `calculateAffectedBeforeKeys/AfterKeys` - 批量计算 before/after 主键集合
- `WideTableIncrementalUpdater.updateWideTable` - 基于主键集合批量更新宽表
- `SchemaResolver` - 自动解析字段列表

### 1.2 当前缺口

`WideTableIncrementalUpdater.generateAfterSql` 使用简单 IN 子句：
```sql
SELECT * FROM (原始SQL) WHERE id IN (1, 2, 3)
```

**问题**：
- 复杂 SQL（CTE/子查询/UNION）可能不支持外层 WHERE
- 需要解析原始 SQL 结构才能正确添加 WHERE
- 无法利用 DuckDB 的临时表覆盖能力

### 1.3 目标

引入 WITH CTE SQL 生成器，将 CDC 数据嵌入 WITH 子句生成临时表，覆盖源表数据：
```sql
WITH users AS (VALUES (1,'John',1), (2,'Jane',2)) AS t(id,name,status)
SELECT u.id, u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id
```

---

## 2. 核心方案

### 2.1 原理

1. 从 CDC 事件提取 before/after 数据行
2. 按字段顺序生成 VALUES 子句
3. 将 VALUES 嵌入 WITH 子句创建临时表
4. 临时表名与源表名一致，覆盖原始数据
5. 执行 SQL 获取完整宽表结果

### 2.2 优势

| 对比维度 | IN 子句方案 | WITH CTE 方案 |
|----------|------------|--------------|
| SQL 解析 | 需要解析结构添加 WHERE | 零解析，直接拼接 |
| 复杂 SQL 支持 | 可能失败 | 完全支持 |
| 数据完整性 | 只返回主键匹配行 | 返回完整宽表数据 |
| 性能 | 需要额外 WHERE 过滤 | DuckDB 优化临时表 |

---

## 3. 类设计

### 3.1 WithCteSqlGenerator

```java
public class WithCteSqlGenerator {
    /**
     * 生成批量 WITH CTE SQL
     */
    public String generateBatch(String sqlTemplate, String tableName,
                                List<Map<String, Object>> rows, List<String> fields);
    
    /**
     * 生成单条 WITH CTE SQL
     */
    public String generateSingle(String sqlTemplate, String tableName,
                                 Map<String, Object> rowData, List<String> fields);
    
    /**
     * 构建 VALUES 子句
     */
    public String buildValuesClause(Map<String, Object> rowData, List<String> fields);
    
    /**
     * 格式化 SQL 值
     */
    protected String formatValue(Object value);
}
```

### 3.2 集成方式

修改 `WideTableIncrementalUpdater`：
```java
// 旧实现
private String generateAfterSql(Set<Object> affectedAfterKeys) {
    String inClause = buildInClause(affectedAfterKeys);
    return String.format("WITH affected AS (%s) SELECT * FROM (%s) WHERE %s IN %s",
            inClause, querySql, wideTablePrimaryKey, inClause);
}

// 新实现
private String generateAfterSql(List<Map<String, Object>> afterRows, String tableName) {
    return withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
}
```

---

## 4. 数据流程

```
CDC 事件缓冲区
    ↓
AffectedKeyCalculator.calculateAffectedBeforeKeys/AfterKeys()
    → beforeKeys = [123, 789]
    → afterKeys = [456, 789]
    ↓
从 CDC 事件提取 after 数据行
    → afterRows = [{id:456, name:'John'}, {id:789, name:'Jane'}]
    ↓
WithCteSqlGenerator.generateBatch(querySql, 'users', afterRows, fields)
    → WITH users AS (VALUES (456,'John'), (789,'Jane')) AS t(id,name)
      SELECT u.id, u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id
    ↓
DuckDB 执行 SQL
    → 返回完整宽表数据
    ↓
WideTableIncrementalUpdater.updateWideTable()
    → 四态判断：DELETE(123), INSERT(456), UPDATE(789)
    → 输出 CDC 事件到下游
```

---

## 5. 测试覆盖

### 5.1 单元测试

- 单行 VALUES 生成（正常/NULL/字符串转义/数字/布尔）
- 批量 VALUES 生成（多行混合）
- 完整 WITH CTE SQL 生成（单条/批量）
- 边界场景（空数据/特殊字符）

### 5.2 集成测试

- WithCteSqlGenerator + WideTableIncrementalUpdater 联合工作
- 验证复杂 SQL（JOIN/CTE/子查询）支持

---

## 6. 实施计划

### Task 1: 创建 WithCteSqlGenerator 类
- 编写测试用例
- 实现 generateBatch/generateSingle/buildValuesClause/formatValue
- 运行测试验证

### Task 2: 集成到 WideTableIncrementalUpdater
- 修改 generateAfterSql 使用 WITH CTE
- 更新构造函数注入 WithCteSqlGenerator
- 运行集成测试验证

### Task 3: 清理旧代码
- 移除旧的 IN 子句生成逻辑
- 运行全部测试验证无回归
