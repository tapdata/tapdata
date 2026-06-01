# DuckDbSqlNode 表映射与 SQL 替换机制设计文档

> **日期**: 2026-05-30
> **状态**: 已批准 ✅
> **作者**: AI Assistant
> **审批人**: User

---

## 1. 背景与动机

### 1.1 当前问题

HazelcastDuckDbSqlNode 在处理多数据源场景时存在以下问题：

1. **表名歧义性**：使用 `%s` 占位符或原始表名，在多数据源同名表场景下容易混淆
2. **配置不直观**：FromTableConfig 的 `tableName` 和 `primaryKey` 字段语义不够清晰
3. **SQL 灵活性不足**：无法支持复杂的 JOIN 查询，只能简单替换单个表名
4. **表生命周期管理缺失**：缺少统一的建表/重建机制

### 1.2 设计目标

1. **唯一性保证**：确保 DuckDB 中写入的表名任务级唯一
2. **配置清晰化**：通过 `preNodeId` 明确关联前置节点，`tableNameInSql` 作为 SQL 别名
3. **SQL 灵活性**：支持用户直接编写包含表别名的复杂 SQL，系统自动解析和替换
4. **生命周期管理**：提供完整的建表/重建/验证机制

---

## 2. 核心变更

### 2.1 FromTableConfig 结构重构

**文件位置**: `/Users/hj/workspace/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/commons/dag/process/DuckDbSqlNode.java`

#### 当前结构

```java
public static class FromTableConfig {
    private String tableName;      // 从表名
    private String primaryKey;    // 从表主键字段
    
    public FromTableConfig() {}
    
    public FromTableConfig(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }
}
```

#### 目标结构

```java
public static class FromTableConfig {
    /** 前置节点 ID（用于查找对应的 NodeSchemaInfo） */
    private String preNodeId;
    
    /** SQL 中使用的表别名（如 t1, t2, users_alias） */
    private String tableNameInSql;
    
    public FromTableConfig() {}
    
    public FromTableConfig(String preNodeId, String tableNameInSql) {
        this.preNodeId = preNodeId;
        this.tableNameInSql = tableNameInSql;
    }
}
```

#### 字段映射关系

| 旧字段 | 新字段 | 旧含义 | 新含义 | 示例 |
|--------|-------|--------|--------|------|
| `primaryKey` | `preNodeId` | 主键字段名 | 前置节点 ID | `"node_mysql_1"` |
| `tableName` | `tableNameInSql` | 原始表名 | SQL 表别名 | `"t1"` |

---

### 2.2 配置示例对比

#### 旧配置方式

```json
{
  "querySql": "SELECT * FROM %s",
  "fromTables": [
    { "tableName": "users", "primaryKey": "id" },
    { "tableName": "orders", "primaryKey": "order_id" }
  ]
}
```

#### 新配置方式

```json
{
  "querySql": "SELECT t1.id, t1.name, t2.order_id, t2.amount 
               FROM t1 
               JOIN t2 ON t1.id = t2.user_id",
  "fromTables": [
    { "preNodeId": "node_mysql_1", "tableNameInSql": "t1" },
    { "preNodeId": "node_pg_1", "tableNameInSql": "t2" }
  ],
  "duckLakeEnabled": false
}
```

**优势**:
- ✅ SQL 更直观，直接使用表别名
- ✅ 通过 `preNodeId` 明确关联前置节点
- ✅ 支持 JOIN、子查询等复杂 SQL
- ✅ 避免硬编码表名，提高灵活性

---

## 3. 技术方案：正则边界检测替换

### 3.1 方案选择理由

经过对比分析，选择**正则表达式边界检测**方案：

| 方案 | 实现复杂度 | 性能 | 安全性 | 适用场景 |
|------|----------|------|--------|---------|
| A: 简单字符串替换 | ⭐ 低 | ⭐⭐⭐ 高 | ⭐⭐ 中 | 规范命名场景 |
| **B: 正则边界检测** | ⭐⭐ 中 | ⭐⭐ 高 | ⭐⭐⭐ 高 | **通用场景（推荐）** |
| C: SQL 上下文感知 | ⭐⭐⭐ 高 | ⭐ 中 | ⭐⭐⭐⭐ 最高 | 复杂 SQL 场景 |

**推荐理由**:
1. 在安全性和实现复杂度之间取得良好平衡
2. 使用 `\b` 单词边界避免部分匹配误替换
3. 性能满足初始化时一次性执行的需求

### 3.2 核心算法

#### 输入输出

```
输入：
- querySql (原始 SQL，包含表别名)
- fromTables (配置列表)
- nodeSchemaCache (Schema 缓存)

输出：
- resolvedQuerySql (所有别名已替换为 targetTableName)
```

#### 算法流程

```
Step 1: 构建映射表 aliasMap: tableNameInSql → targetTableName
   for each fromTable in fromTables:
     a. 根据 preNodeId 查找 NodeSchemaInfo
     b. 构建 targetTableName = buildTargetTableName(schemaInfo)
     c. aliasMap[tableNameInSql] = targetTableName

Step 2: 替换 querySql (使用 \b 边界检测)
   resolvedQuerySql = querySql
   for each (alias, targetTableName) in aliasMap:
     a. 编译正则: Pattern.compile("\\b" + Pattern.quote(alias) + "\\b")
     b. 执行全局替换: matcher.replaceAll(targetTableName)
     c. 记录替换次数和日志

Step 3: 验证结果
   a. 确保 resolvedQuerySql != querySql (至少有一次有效替换)
   b. 调用 ensureSelectQuery() 验证 SQL 合法性

Step 4: 返回 resolvedQuerySql
```

### 3.3 示例演示

#### 输入配置

```java
querySql = """
    SELECT t1.id, t1.name, t2.order_id, t2.amount 
    FROM t1 
    JOIN t2 ON t1.id = t2.user_id 
    WHERE t1.status = 'active' AND t2.amount > 100
""";

fromTables = [
    FromTableConfig(preNodeId="node_mysql_1", tableNameInSql="t1"),
    FromTableConfig(preNodeId="node_pg_1", tableNameInSql="t2")
];

nodeSchemaCache = {
    "node_mysql_1" → NodeSchemaInfo(
        sourceId="node_mysql_1", 
        tableName="users",
        qualifiedName="T_Mysql_db1_public_users_...",
        primaryKeys=["id"],
        fieldMap={id, name, email, created_at}
    ),
    "node_pg_1" → NodeSchemaInfo(
        sourceId="node_pg_1",
        tableName="orders",
        qualifiedName="T_Pg_db2_public_orders_...",
        primaryKeys=["order_id"],
        fieldMap={order_id, user_id, amount, status}
    )
};
```

#### 执行过程

```
Step 1: Build alias map
┌─────────────────┬──────────────────────┬──────────────────────────┐
│ tableNameInSql  │ targetTableName      │ Source Info              │
├─────────────────┼──────────────────────┼──────────────────────────┤
│ t1              │ node_mysql_1__users  │ MySQL: users table       │
│ t2              │ node_pg_1__orders    │ PostgreSQL: orders table │
└─────────────────┴──────────────────────┴──────────────────────────┘

Step 2: Replace 't1' with boundary detection
Pattern: \bt1\b
Match positions: t1.id, t1.name, FROM t1, t1.id (JOIN), t1.status
Replace count: 5
Intermediate result:
SELECT node_mysql_1__users.id, node_mysql_1__users.name, t2.order_id...
FROM node_mysql_1__users
JOIN t2 ON node_mysql_1__users.id = t2.user_id...

Step 3: Replace 't2' with boundary detection
Pattern: \bt2\b
Match positions: t2.order_id, t2.amount, t2 (JOIN), t2.user_id, t2.amount
Replace count: 5
Final result:
SELECT node_mysql_1__users.id, node_mysql_1__users.name,
       node_pg_1__orders.order_id, node_pg_1__orders.amount
FROM node_mysql_1__users
JOIN node_pg_1__orders ON node_mysql_1__users.id = node_pg_1__orders.user_id
WHERE node_mysql_1__users.status = 'active' AND node_pg_1__orders.amount > 100
```

---

## 4. 初始化流程调整

### 4.1 doInit() 流程图

```
doInit()
│
├→ Step 1: Read Configuration
│   - querySql (原始 SQL)
│   - fromTables (新结构)
│   - duckLakeEnabled (保持原有语义)
│   - 其他配置项
│
├→ Step 2: Initialize Schema Cache [MUST BE FIRST]
│   └→ initNodeSchemaCache()
│       └→ 填充 nodeSchemaCache: preNodeId → NodeSchemaInfo
│
├→ Step 3: Manage DuckDB Tables [NEW]
│   └→ manageDuckDbTables()
│       ├→ determineIfShouldRecreateTables()
│       │   └→ 返回 boolean (重建标识)
│       ├→ 遍历 nodeSchemaCache
│       │   ├→ buildTargetTableName(schemaInfo)
│       │   └→ duckDbOperator.ensureTableExists(
│       │        targetTableName,
│       │        fields,
│       │        primaryKeys,
│       │        shouldRecreate  ← 外部控制!
│       │      )
│       └→ 日志记录
│
├→ Step 4: Resolve Table Aliases in QuerySql [NEW]
│   └→ resolveQuerySqlTableAliases()
│       ├→ buildAliasToTargetMapping()
│       │   └→ Map<alias, targetTableName>
│       ├→ 正则替换 (\b boundary detection)
│       └→ 输出 resolvedQuerySql
│
├→ Step 5: Validate Resolved SQL
│   └→ ensureSelectQuery(resolvedQuerySql)
│
├→ Step 6: Initialize Other Components
│   - DuckDbOperator
│   - ErrorHandler
│   - DlqWriter
│   - ContextFlusher
│
└→ Success! Ready to process events.
```

### 4.2 执行顺序依赖关系

```
关键依赖链:

doInit()
  │
  ├─→ initNodeSchemaCache()           ← 第一步（必须最先执行）
  │     └─→ 填充 nodeSchemaCache
  │          { "node_mysql_1" → NodeSchemaInfo(...),
  │            "node_pg_1"   → NodeSchemaInfo(...) }
  │
  ├─→ manageDuckDbTables()           ← 第二步（依赖 Step 1）
  │     ├─→ 读取 nodeSchemaCache
  │     ├─→ 决定是否重建 (shouldRecreate)
  │     └─→ 创建/重建 DuckDB 表
  │         ├─→ if shouldRecreate=true
  │         │   └→ DROP TABLE IF EXISTS + CREATE TABLE
  │         └─→ if shouldRecreate=false
  │             └→ CREATE TABLE IF NOT EXISTS
  │
  ├─→ resolveQuerySqlTableAliases()  ← 第三步（依赖 Step 1）
  │     ├─→ 构建 alias → targetTableName 映射
  │     └─→ 正则替换 querySql → resolvedQuerySql
  │
  ├─→ ensureSelectQuery(...)         ← 第四步（依赖 Step 3）
  │
  └─→ 初始化其他组件...              ← 第五步

错误处理:
- Step 1 失败 → TapCodeException, 终止初始化
- Step 2 失败 (找不到 schema) → IllegalStateException
- Step 3 失败 (SQL 执行错误) → SQLException → TapCodeException
- Step 4 失败 (替换失败) → IllegalStateException
- Step 5 失败 (SQL 不合法) → IllegalArgumentException → TapCodeException
```

---

## 5. DuckDB 表管理机制

### 5.1 架构设计原则

采用**职责分离**模式：

```
┌─────────────────────────────────────────────────────┐
│           HazelcastDuckDbSqlNode                    │
│  (业务逻辑层 - 控制策略)                              │
│                                                     │
│  决策权:                                             │
│  ├─→ 是否重建表? (determineIfShouldRecreateTables)  │
│  ├─→ 何时创建? (manageDuckDbTables)                │
│  └─→ 如何映射? (buildTargetTableName)               │
│                                                     │
│  调用:                                              │
│  └─→ duckDbOperator.ensureTableExists(              │
│        name, fields, pks, shouldRecreate            │
│      )                                              │
└─────────────────────────────────────────────────────┘
                    ↓ 调用
┌─────────────────────────────────────────────────────┐
│              DuckDbOperator                         │
│  (基础设施层 - 执行操作)                              │
│                                                     │
│  执行权:                                             │
│  ├─→ ensureTableExists(name, fields, pks, recreate) │
│  ├─→ buildCreateTableSql(name, fields, pks)         │
│  └─→ mapToDuckDbType(tapType)                       │
└─────────────────────────────────────────────────────┘
```

**核心优势**:
- ✅ HazelcastDuckDbSqlNode 控制"何时重建"，DuckDbOperator 负责"如何创建"
- ✅ DuckDbOperator 方法可独立单元测试
- ✅ 其他节点可复用 `ensureTableExists()` 方法
- ✅ 易于扩展（未来可添加索引、分区等功能）

### 5.2 DuckDbOperator 公共建表方法

#### 方法签名

```java
/**
 * Ensure table exists with correct schema, optionally recreate
 *
 * @param tableName Target table name (will be sanitized)
 * @param fields Field definitions from NodeSchemaInfo/TapTable
 * @param primaryKeys Primary key field names
 * @param recreate If true, drop and recreate; if false, create only if not exists
 * @throws SQLException if database operation fails
 */
public void ensureTableExists(String tableName, List<TapField> fields, 
                             List<String> primaryKeys, boolean recreate) throws SQLException;

/**
 * Build CREATE TABLE SQL statement from TapField definitions
 *
 * @param tableName Target table name (will be sanitized)
 * @param fields Field definitions
 * @param primaryKeys Primary key field names
 * @return Complete CREATE TABLE SQL statement
 */
public static String buildCreateTableSql(String tableName, List<TapField> fields, 
                                        List<String> primaryKeys);
```

#### 实现逻辑

```java
public void ensureTableExists(String tableName, List<TapField> fields, 
                             List<String> primaryKeys, boolean recreate) throws SQLException {
    String safeTableName = sanitizeIdentifier(tableName);
    
    if (recreate) {
        // Task reset scenario: Drop existing table first
        String dropSql = "DROP TABLE IF EXISTS " + safeTableName;
        execute(dropSql);
        logger.debug("Dropped table: {}", safeTableName);
        
        // Create new table
        String createSql = buildCreateTableSql(tableName, fields, primaryKeys);
        execute(createSql);
        logger.info("Recreated table: {}", safeTableName);
        
    } else {
        // Normal start: Check existence first
        if (tableExists(safeTableName)) {
            logger.debug("Table already exists: {}, skipping creation", safeTableName);
            return;
        }
        
        // Create table if not exists
        String createSql = buildCreateTableSql(tableName, fields, primaryKeys);
        execute(createSql);
        logger.info("Created new table: {}", safeTableName);
    }
}

public static String buildCreateTableSql(String tableName, List<TapField> fields, 
                                        List<String> primaryKeys) {
    StringBuilder sql = new StringBuilder();
    String safeTableName = sanitizeIdentifier(tableName);
    
    sql.append("CREATE TABLE ").append(safeTableName).append(" (\n");
    
    List<String> columnDefs = new ArrayList<>();
    for (TapField field : fields) {
        String colName = sanitizeIdentifier(field.getName());
        String colType = mapToDuckDbType(field.getTapType());
        
        StringBuilder def = new StringBuilder()
            .append("  ").append(colName).append(" ").append(colType);
        
        if (primaryKeys.contains(field.getName())) {
            def.append(" PRIMARY KEY NOT NULL");
        }
        
        columnDefs.add(def.toString());
    }
    
    sql.append(String.join(",\n", columnDefs));
    sql.append("\n)");
    
    return sql.toString();
}
```

### 5.3 重建标识判断逻辑

**重要说明**: 重建标识**不**使用 `duckLakeEnabled` 字段（该字段用于区分 DuckLake vs DuckDB 表类型）。

#### 判断优先级

```java
private boolean determineIfShouldRecreateTables() {
    // Priority 1: Check processorBaseContext attributes (recommended)
    if (processorBaseContext != null && processorBaseContext.getAttributes() != null) {
        // Option A: Explicit reset flag from framework
        Object taskReset = processorBaseContext.getAttributes().get("taskReset");
        if (taskReset instanceof Boolean) {
            return (Boolean) taskReset;
        }
        
        // Option B: Check sync stage (restart/resync scenario)
        Object syncStage = processorBaseContext.getAttributes().get("syncStage");
        if (syncStage != null) {
            String stageStr = syncStage.toString();
            if (stageStr.contains("RESTART") || stageStr.contains("FULL_RESYNC")) {
                logger.info("Detected restart/resync stage: {}, will recreate tables", stageStr);
                return true;
            }
        }
        
        // Option C: Check task context for restart marker
        if (processorBaseContext.getTaskContext() != null) {
            Object isRestart = processorBaseContext.getTaskContext().get("isRestart");
            if (isRestart instanceof Boolean && (Boolean) isRestart) {
                return true;
            }
        }
    }
    
    // Default: Safe option - do not recreate unless explicitly requested
    logger.debug("No reset indicator found, will preserve existing tables");
    return false;
}
```

---

## 6. 错误处理机制

### 6.1 异常类型与场景

| 异常类型 | 触发场景 | 处理方式 |
|---------|---------|---------|
| `IllegalArgumentException` | FromTableConfig 配置无效（空值） | 终止初始化，抛出 TapCodeException |
| `IllegalStateException` | Schema 缓存未找到 / 替换失败 | 记录详细日志，终止初始化 |
| `SQLException` | DuckDB 表操作失败 | 回滚已创建的表，抛出 TapCodeException |
| `PatternSyntaxException` | 正则表达式编译错误（理论上不会发生） | 使用默认字符串替换降级 |

### 6.2 错误示例

#### 场景 1: preNodeId 无效

```java
// 配置错误
{ "preNodeId": "", "tableNameInSql": "t1" }

// 抛出异常
throw new IllegalArgumentException(
    "FromTableConfig has blank preNodeId: ''. " +
    "Each fromTable must specify a valid preNodeId."
);

// 日志输出
ERROR Failed to initialize DuckDbSqlNode: Invalid configuration
IllegalArgumentException: FromTableConfig has blank preNodeId: ''
    at HazelcastDuckDbSqlNode.buildAliasToTargetMapping(...)
    at HazelcastDuckDbSqlNode.resolveQuerySqlTableAliases(...)
    at HazelcastDuckDbSqlNode.doInit(...)
```

#### 场景 2: Schema 缓存未命中

```java
// 配置正确但前置节点不存在
{ "preNodeId": "nonexistent_node", "tableNameInSql": "t1" }

// 抛出异常
throw new IllegalStateException(
    "Cannot find NodeSchemaInfo for preNodeId: 'nonexistent_node'. " +
    "Ensure the pre-node is properly configured and its schema is loaded."
);

// 日志输出
ERROR Schema cache miss for preNodeId: nonexistent_node
Available schemas: [node_mysql_1, node_pg_1]
IllegalStateException: Cannot find NodeSchemaInfo for preNodeId: 'nonexistent_node'
```

#### 场景 3: 表别名重复

```java
// 配置错误：重复的别名
[
  { "preNodeId": "node_mysql_1", "tableNameInSql": "t1" },
  { "preNodeId": "node_pg_1", "tableNameInSql": "t1" }  // 重复！
]

// 抛出异常
throw new IllegalArgumentException(
    "Duplicate tableNameInSql detected: 't1'. " +
    "Each table alias in fromTables must be unique."
);
```

---

## 7. 测试策略

### 7.1 单元测试覆盖

#### FromTableConfig 结构测试

```java
@Test
void testFromTableConfigNewStructure() {
    FromTableConfig config = new FromTableConfig("node_mysql_1", "t1");
    
    assertEquals("node_mysql_1", config.getPreNodeId());
    assertEquals("t1", config.getTableNameInSql());
}

@Test
void testFromTableConfigValidation() {
    assertThrows(IllegalArgumentException.class, () -> {
        new FromTableConfig("", "t1");  // Blank preNodeId
    });
    
    assertThrows(IllegalArgumentException.class, () -> {
        new FromTableConfig("node_1", "");  // Blank tableNameInSql
    });
}
```

#### SQL 替换测试

```java
@Test
void testSimpleAliasReplacement() {
    String querySql = "SELECT t1.id FROM t1";
    Map<String, String> aliasMap = Map.of("t1", "node_mysql_1__users");
    
    String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
    
    assertEquals("SELECT node_mysql_1__users.id FROM node_mysql_1__users", resolved);
}

@Test
void testJoinQueryReplacement() {
    String querySql = "SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.ref_id";
    Map<String, String> aliasMap = Map.of(
        "t1", "node_mysql_1__users",
        "t2", "node_pg_1__orders"
    );
    
    String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
    
    assertTrue(resolved.contains("node_mysql_1__users"));
    assertTrue(resolved.contains("node_pg_1__orders"));
    assertFalse(resolved.contains("t1"));
    assertFalse(resolved.contains("t2"));
}

@Test
void testNoPartialReplacement() {
    // Should NOT replace 't1' within string literals or identifiers
    String querySql = "SELECT t1.id, 't1_is_alias' AS comment FROM t1 WHERE name LIKE '%t1%'";
    Map<String, String> aliasMap = Map.of("t1", "target_table");
    
    String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
    
    // Only standalone t1 should be replaced
    assertEquals("SELECT target_table.id, 't1_is_alias' AS comment FROM target_table WHERE name LIKE '%t1%'", 
                 resolved);
}
```

#### 表管理测试

```java
@Test
void testCreateTableWhenNotExists() throws SQLException {
    // Given: Table does not exist
    when(duckDbOperator.tableExists("test_table")).thenReturn(false);
    
    // When: Ensure table exists with recreate=false
    duckDbOperator.ensureTableExists("test_table", fields, primaryKeys, false);
    
    // Then: Should call create, not drop
    verify(duckDbOperator, never()).execute(anyString()); // DROP
    verify(duckDbOperator, times(1)).execute(contains("CREATE TABLE")); // CREATE
}

@Test
void testRecreateTableWhenFlagTrue() throws SQLException {
    // When: Ensure table exists with recreate=true
    duckDbOperator.ensureTableExists("test_table", fields, primaryKeys, true);
    
    // Then: Should drop then create
    InOrder inOrder = inOrder(duckDbOperator);
    inOrder.verify(duckDbOperator).execute(contains("DROP TABLE"));
    inOrder.verify(duckDbOperator).execute(contains("CREATE TABLE"));
}
```

### 7.2 集成测试场景

#### 完整初始化流程测试

```java
@Test
void testFullInitFlowWithValidConfig() throws Exception {
    // Setup
    setupMockSchemaCache();
    setupMockFromTables();
    
    // Execute
    hazelcastNode.doInit(mockContext);
    
    // Verify initialization order
    InOrder inOrder = inOrder(hazelcastNode);
    inOrder.verify(hazelcastNode).initNodeSchemaCache();           // Step 1
    inOrder.verify(hazelcastNode).manageDuckDbTables();             // Step 2
    inOrder.verify(hazelcastNode).resolveQuerySqlTableAliases();    // Step 3
    inOrder.verify(duckDbOperator).ensureSelectQuery(anyString());  // Step 4
    
    // Verify resolved SQL contains actual table names
    assertNotNull(hazelcastNode.getResolvedQuerySql());
    assertTrue(hazelcastNode.getResolvedQuerySql().contains("node_mysql_1__users"));
    assertTrue(hazelcastNode.getResolvedQuerySql().contains("node_pg_1__orders"));
}
```

---

## 8. 向后兼容性

### 8.1 Breaking Changes

本次变更为 **破坏性变更 (Breaking Change)**：

| 变更点 | 影响 | 迁移指南 |
|-------|------|---------|
| `FromTableConfig.primaryKey` → `preNodeId` | 必须更新配置 | 将主键字段名改为前置节点ID |
| `FromTableConfig.tableName` → `tableNameInSql` | 必须更新配置 | 将表名改为SQL中使用的别名 |
| querySql 格式 | 必须更新 | 从 `%s` 占位符改为显式表别名 |

### 8.2 迁移示例

**旧配置 (不兼容)**:
```json
{
  "querySql": "SELECT * FROM %s",
  "fromTables": [
    { "tableName": "users", "primaryKey": "id" }
  ]
}
```

**新配置 (兼容)**:
```json
{
  "querySql": "SELECT * FROM t1",
  "fromTables": [
    { "preNodeId": "node_mysql_1", "tableNameInSql": "t1" }
  ]
}
```

---

## 9. 性能影响分析

### 9.1 初始化开销

| 操作 | 时间复杂度 | 频率 | 影响评估 |
|------|----------|------|---------|
| Schema 缓存加载 | O(N) | 启动时 1 次 | 可忽略 |
| Alias 映射构建 | O(M) | 启动时 1 次 | 可忽略 |
| 正则替换 | O(L × M) | 启动时 1 次 | 可忽略 |
| DuckDB 建表 | O(F) | 启动时 1 次 | 取决于字段数 |

**总开销**: < 100ms（典型场景），仅在启动时执行一次

### 9.2 运行时性能

- ✅ **零运行时开销**: SQL 已在启动时解析完成
- ✅ **无额外内存占用**: resolvedQuerySql 为单一字符串
- ✅ **查询性能不受影响**: DuckDB 执行的是最终 SQL

---

## 10. 未来扩展方向

### 10.1 可能的增强功能

1. **SQL 解析器升级**
   - 从正则替换升级为 AST 解析（如 JSqlParser）
   - 支持更精确的上下文感知替换
   - 提供语法高亮和自动补全

2. **表结构演化**
   - 支持在线 DDL（ALTER TABLE）
   - 自动检测 Schema 变更并迁移
   - 版本化的表结构管理

3. **高级表管理**
   - 自动索引创建（基于查询模式）
   - 分区策略支持
   - 表压缩和归档

4. **可视化调试**
   - SQL 替换前后对比视图
   - 表映射关系图
   - 性能监控仪表盘

---

## 11. 总结

### 11.1 核心价值

1. **唯一性保证**: 通过 `sourceId__tableName` 格式确保任务级唯一
2. **配置清晰化**: `preNodeId` + `tableNameInSql` 语义明确
3. **SQL 灵活性**: 支持复杂 JOIN 和自定义查询
4. **生命周期完整**: 统一的建表/重建/验证机制
5. **安全性**: 正则边界检测避免误替换

### 11.2 关键决策点

| 决策 | 选择 | 理由 |
|------|------|------|
| 替换算法 | 正则边界检测 | 安全性与复杂度的平衡 |
| 替换时机 | 初始化时一次性 | 性能最优，零运行时开销 |
| 重建控制 | 外部参数传入 | 职责分离，灵活可控 |
| 建表方法 | DuckDbOperator 公共方法 | 可复用，易测试 |

### 11.3 下一步行动

1. ✅ 设计文档已批准
2. ⏭️ 进入实施阶段（调用 writing-plans skill）
3. ⏭️ 按 TDD 流程开发
4. ⏭️ 单元测试 + 集成测试
5. ⏭️ 代码评审与优化

---

## 附录 A: 术语表

| 术语 | 定义 |
|------|------|
| **preNodeId** | 前置节点的唯一标识符，用于关联数据源 |
| **tableNameInSql** | 用户在 SQL 查询中使用的表别名（如 t1, t2） |
| **targetTableName** | 实际写入 DuckDB 的表名（格式：sourceId__tableName） |
| **resolvedQuerySql** | 经过别名替换后的最终可执行 SQL |
| **NodeSchemaInfo** | 封装前置节点完整 Schema 信息的实体类 |
| **shouldRecreate** | 控制是否删除并重建表的布尔标识 |

## 附录 B: 相关文件清单

| 文件路径 | 变更类型 | 说明 |
|---------|---------|------|
| `manager/tm-common/src/main/java/com/tapdata/tm/commons/dag/process/DuckDbSqlNode.java` | 重构 | FromTableConfig 结构改造 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/DuckDbOperator.java` | 增强 | 新增公共建表方法 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java` | 重构 | 初始化流程 + SQL 解析逻辑 |
| `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/ArrowWriter.java` | 清理 | 移除已抽离到 DuckDbOperator 的方法 |

---

**文档版本**: v1.0  
**最后更新**: 2026-05-30  
**审批状态**: ✅ 已批准
