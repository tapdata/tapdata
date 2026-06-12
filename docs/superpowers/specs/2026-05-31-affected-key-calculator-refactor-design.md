# AffectedKeyCalculator 重构设计 - NodeSchemaInfo 适配

> **日期**: 2026-05-31  
> **状态**: 待用户审批  
> **关联计划**: [2026-05-30-duckdb-sql-table-mapping-plan.md](../plans/2026-05-30-duckdb-sql-table-mapping-plan.md)

---

## 📋 **目标**

重构 `AffectedKeyCalculator` 类，将其从废弃的 `FromTableConfig` API 迁移到新的 `NodeSchemaInfo` API，解决 4 个 TODO 项中的 3 个（自动 SQL 解析作为未来 Task）。

## 🎯 **范围**

### **本次完成（✅）**

1. 构造器注入 `NodeSchemaMap` + `resolvedQuerySql`
2. 重写 `getQuerySqlForTable()` 方法
3. 重写 `getTableFields()` 方法
4. 重写 `getSourceTablePrimaryKey()` 方法
5. 适配 `HazelcastDuckDbSqlNode` 调用方

### **未来工作（⏭️）**

6. 自动 SQL 解析功能（Line 577）- 需要独立 Task 和 SQL Parser 组件

---

## 🏗️ **架构设计**

### **依赖关系图**

```
HazelcastDuckDbSqlNode (doInit)
    │
    ├── Step 1: initNodeSchemaCache()
    │       └── nodeSchemaCache: Map<String, NodeSchemaInfo>
    │
    ├── Step 2: resolveSqlTableAliases()
    │       └── this.querySql: String (已解析)
    │
    └── Step 3: new AffectedKeyCalculator(...)
            │
            ├── wideTablePrimaryKey: String
            ├── mainTableName: String
            ├── mainTablePrimaryKey: String
            ├── fromTables: List<FromTableConfig>
            ├── customJoinQueries: Map<String, String>
            ├── duckDbOperator: DuckDbOperator
            ├── nodeSchemaMap: Map<String, NodeSchemaInfo> ← 新增
            └── resolvedQuerySql: String                  ← 新增
```

### **数据流向**

```
FromTableConfig (preNodeId, tableNameInSql)
        ↓
findSchemaInfoByTableNameInSql()
        ↓
nodeSchemaMap.get(preNodeId)
        ↓
NodeSchemaInfo
├── getFieldNames()      → getTableFields()
├── getPrimaryKeys()     → getSourceTablePrimaryKey()
└── getTargetTableName() → (预留)
```

---

## 🔧 **详细实现设计**

### **Part 1: 构造器重构**

#### **新构造器签名**

```java
/**
 * 完整构造器 - 包含所有必需依赖
 * 
 * @param wideTablePrimaryKey 宽表主键字段名
 * @param mainTableName 主表名（在 SQL 中使用的别名）
 * @param mainTablePrimaryKey 主表主键字段名
 * @param fromTables 前置节点配置列表
 * @param customJoinQueries 自定义 JOIN 查询映射
 * @param operator DuckDB 操作器
 * @param nodeSchemaMap 前置节点 Schema 信息映射 (preNodeId → NodeSchemaInfo)
 * @param resolvedQuerySql 已解析的 SQL 语句（表别名已替换为实际表名）
 * @throws IllegalArgumentException 如果必需参数为 null 或空
 */
public AffectedKeyCalculator(
    String wideTablePrimaryKey,
    String mainTableName,
    String mainTablePrimaryKey,
    List<FromTableConfig> fromTables,
    Map<String, String> customJoinQueries,
    DuckDbOperator operator,
    Map<String, NodeSchemaInfo> nodeSchemaMap,
    String resolvedQuerySql
)
```

**校验规则：**
```java
// 强制非空校验
Objects.requireNonNull(wideTablePrimaryKey, "wideTablePrimaryKey must not be null");
Objects.requireNonNull(operator, "operator must not be null");
Objects.requireNonNull(nodeSchemaMap, "nodeSchemaMap must not be null");
Objects.requireNonNull(resolvedQuerySql, "resolvedQuerySql must not be null");

if (wideTablePrimaryKey.isBlank()) {
    throw new IllegalArgumentException("wideTablePrimaryKey must not be blank");
}
if (resolvedQuerySql.isBlank()) {
    throw new IllegalArgumentException("resolvedQuerySql must not be blank");
}
```

**字段声明：**
```java
private final Map<String, NodeSchemaInfo> nodeSchemaMap;   // 新增
private final String resolvedQuerySql;                        // 新增
```

**移除内容：**
- ❌ 删除旧的无参 `nodeSchemaMap`/`resolvedQuerySql` 的构造器（共 2 个）

---

### **Part 2: 方法重写**

#### **2.1 `getQuerySqlForTable()` - 完全重写**

**方法签名：**
```java
private String getQuerySqlForTable(String tableName)
```

**实现逻辑：**
```java
private String getQuerySqlForTable(String tableName) {
    // 直接返回已解析的 querySql（由 HazelcastDuckDbSqlNode 在初始化时完成）
    if (resolvedQuerySql == null || resolvedQuerySql.isBlank()) {
        throw new IllegalStateException(
            "resolvedQuerySql must not be null or blank. " +
            "Ensure HazelcastDuckDbSqlNode.resolveSqlTableAliases() was called before using AffectedKeyCalculator.");
    }
    
    logger.debug("Returning resolved querySql for table {}: {}...", 
                tableName, 
                resolvedQuerySql.substring(0, Math.min(50, resolvedQuerySql.length())));
    
    return resolvedQuerySql;
}
```

**关键改进：**
- ✅ 不再遍历 `fromTables` 查找废弃的 `querySql` 字段
- ✅ 使用已解析的 SQL（表别名已替换为实际目标表名）
- ✅ Fail-fast：缺少必要依赖时立即抛出明确异常
- ✅ 日志截断避免输出过长 SQL

---

#### **2.2 `getTableFields()` - 完全重写**

**方法签名：**
```java
private List<String> getTableFields(String tableName)
```

**实现逻辑：**
```java
private List<String> getTableFields(String tableName) {
    // 通过 tableNameInSql 查找对应的 NodeSchemaInfo
    NodeSchemaInfo schemaInfo = findSchemaInfoByTableNameInSql(tableName);
    
    if (schemaInfo == null) {
        logger.warn("Cannot find NodeSchemaInfo for tableNameInSql={}, available nodeIds: {}", 
                   tableName, 
                   nodeSchemaMap.keySet().stream().limit(10).collect(Collectors.joining(", ")));
        
        // 降级策略：返回主键字段列表
        String fallbackPk = getSourceTablePrimaryKey(tableName);
        logger.info("Falling back to primary key field '{}' for table {}", fallbackPk, tableName);
        return Collections.singletonList(fallbackPk);
    }
    
    // 从 NodeSchemaInfo 获取完整的字段名列表
    List<String> fieldNames = schemaInfo.getFieldNames();
    
    logger.debug("Retrieved {} fields for table {}: {}", 
                fieldNames.size(), 
                tableName, 
                fieldNames.stream().limit(5).collect(Collectors.joining(", ")));
    
    return fieldNames;
}
```

**新增辅助方法：**
```java
/**
 * 通过 tableNameInSql 查找对应的 NodeSchemaInfo
 * 
 * <p>查找流程：</p>
 * <ol>
 *   <li>遍历 fromTables 找到匹配的 tableNameInSql</li>
 *   <li>获取对应的 preNodeId</li>
 *   <li>从 nodeSchemaMap 中查找 NodeSchemaInfo</li>
 * </ol>
 */
private NodeSchemaInfo findSchemaInfoByTableNameInSql(String tableNameInSql) {
    if (nodeSchemaMap == null || nodeSchemaMap.isEmpty()) {
        return null;
    }
    
    for (FromTableConfig config : fromTables) {
        if (config != null && config.getTableNameInSql() != null &&
            config.getTableNameInSql().equalsIgnoreCase(tableNameInSql)) {
            
            String preNodeId = config.getPreNodeId();
            
            if (preNodeId != null && !preNodeId.isBlank()) {
                return nodeSchemaMap.get(preNodeId);
            }
        }
    }
    
    return null;
}
```

**关键改进：**
- ✅ 使用 `NodeSchemaInfo.getFieldNames()` 获取真实字段信息
- ✅ 多层降级策略：
  - 第一层：从 Schema 获取完整字段列表
  - 第二层：找不到 Schema 时返回主键字段
- ✅ 日志优化：限制输出数量避免日志爆炸

---

#### **2.3 `getSourceTablePrimaryKey()` - 完全重写**

**方法签名：**
```java
private String getSourceTablePrimaryKey(String tableName)
```

**实现逻辑：**
```java
private String getSourceTablePrimaryKey(String tableName) {
    // 通过 tableNameInSql 查找对应的 NodeSchemaInfo
    NodeSchemaInfo schemaInfo = findSchemaInfoByTableNameInSql(tableName);
    
    if (schemaInfo == null) {
        logger.error("Cannot find NodeSchemaInfo for table={}, cannot determine primary key. " +
                    "Available schemas: {}", 
                    tableName, 
                    nodeSchemaMap.keySet());
        throw new IllegalStateException(
            "Failed to find schema info for table: " + tableName + ". " +
            "Ensure nodeSchemaMap is properly initialized in HazelcastDuckDbSqlNode.initNodeSchemaCache()");
    }
    
    // 从 NodeSchemaInfo 获取主键列表
    List<String> primaryKeys = schemaInfo.getPrimaryKeys();
    
    if (primaryKeys == null || primaryKeys.isEmpty()) {
        logger.warn("No primary keys defined for table={}, attempting common PK name detection", tableName);
        
        // 降级策略：尝试常见主键名
        String[] commonPkNames = {"id", "ID", "_id", "pk", "Id"};
        
        for (String commonPk : commonPkNames) {
            if (schemaInfo.getFieldMap() != null && schemaInfo.getFieldMap().containsKey(commonPk)) {
                logger.info("Using fallback primary key '{}' for table {} (no explicit PK defined)", 
                           commonPk, tableName);
                return commonPk;
            }
        }
        
        // 最终失败：抛出明确异常
        throw new IllegalStateException(
            "No primary key found for table: " + tableName + ". " +
            "Available fields: " + schemaInfo.getFieldNames() + ". " +
            "Please define primary keys in the source table schema.");
    }
    
    // 支持复合主键：返回第一个主键（可扩展为返回列表）
    String primaryKey = primaryKeys.get(0);
    
    if (primaryKeys.size() > 1) {
        logger.debug("Table {} has composite primary keys ({}), using first key '{}'", 
                    tableName, primaryKeys, primaryKey);
    } else {
        logger.debug("Found single primary key '{}' for table {}", primaryKey, tableName);
    }
    
    return primaryKey;
}
```

**关键改进：**
- ✅ 使用 `NodeSchemaInfo.getPrimaryKeys()` 获取真实主键定义
- ✅ 三层降级策略：
  1. **第一层**：从 Schema 获取显式定义的主键
  2. **第二层**：常见主键名猜测（id, ID, _id, pk, Id）
  3. **第三层**：抛出明确异常（包含可用字段信息）
- ✅ 复合主键支持提示（当前返回第一个，可扩展）
- ✅ 详细的错误信息便于调试

---

### **Part 3: 调用方适配**

#### **修改位置**

**文件:** `HazelcastDuckDbSqlNode.java`  
**方法:** `doInit()` (约 Line 258)

#### **修改前**

```java
// Step 3: 初始化 AffectedKeyCalculator（在 initNodeSchemaCache 之后）
if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
    affectedKeyCalculator = new AffectedKeyCalculator(
        wideTablePrimaryKey,
        mainTableName,
        mainTablePrimaryKey,
        fromTables,
        customJoinQueries,
        duckDbOperator
    );
    // ... 后续代码
}
```

#### **修改后**

```java
// Step 3: 初始化 AffectedKeyCalculator（必须在 resolveSqlTableAliases 之后）
if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
    affectedKeyCalculator = new AffectedKeyCalculator(
        wideTablePrimaryKey,
        mainTableName,
        mainTablePrimaryKey,
        fromTables,
        customJoinQueries,
        duckDbOperator,
        nodeSchemaCache,           // ✨ 新增: Schema 信息映射
        this.querySql              // ✨ 新增: 已解析的 SQL
    );
    
    logger.info("AffectedKeyCalculator initialized with {} schema(s), querySql length={}", 
               nodeSchemaCache.size(), 
               this.querySql.length());
    
    // ... 后续代码不变
}
```

**时序保证说明：**
```java
// doInit() 中的执行顺序（严格保证）

initNodeSchemaCache();          // → nodeSchemaCache 已填充 ✅
manageDuckDbTables();            // （可选）表管理
resolveSqlTableAliases();        // → this.querySql 已更新 ✅
// ↑ 必须在这两步之后创建 AffectedKeyCalculator ↓
affectedKeyCalculator = new AffectedKeyCalculator(..., nodeSchemaCache, this.querySql);
```

#### **新增 Getter 方法（可选但推荐）**

为了增强可测试性，建议添加：

```java
/**
 * 获取已解析的 querySql
 * 
 * <p>供 AffectedKeyCalculator 和单元测试使用。</p>
 * 
 * @return 已解析的 SQL 语句（表别名已替换为实际表名）
 */
public String getResolvedQuerySql() {
    return this.querySql;
}

/**
 * 获取不可变的 Schema 缓存副本
 * 
 * <p>供外部访问和单元测试使用。</p>
 * 
 * @return 不可修改的 Schema 信息映射
 */
public Map<String, NodeSchemaInfo> getNodeSchemaCache() {
    return Collections.unmodifiableMap(nodeSchemaCache);
}
```

**好处：**
- ✅ AffectedKeyCalculator 可以通过 getter 显式获取依赖
- ✅ 单元测试可以轻松 mock 这些方法
- ✅ 符合依赖注入和封装原则

---

## 🧪 **测试策略**

### **单元测试**

**文件:** `AffectedKeyCalculatorRefactoredTest.java`  
**位置:** `iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/`

#### **测试用例清单**

| # | 测试名称 | 验证点 |
|---|---------|--------|
| 1 | `testConstructorWithValidParams` | 正常参数构造成功 |
| 2 | `testConstructorRejectsNullNodeSchemaMap` | nodeSchemaMap 为 null 时抛异常 |
| 3 | `testConstructorRejectsBlankQuerySql` | querySql 为空时抛异常 |
| 4 | `testGetQuerySqlForTableReturnsResolvedSql` | 返回传入的已解析 SQL |
| 5 | `testGetTableFieldsFromSchemaInfo` | 从 NodeSchemaInfo 获取字段列表 |
| 6 | `testGetTableFieldsFallbackToPrimaryKey` | Schema 不存在时降级到主键 |
| 7 | `testGetSourceTablePrimaryKeyFromSchema` | 从 NodeSchemaInfo 获取主键 |
| 8 | `testGetSourceTablePrimaryKeyFallbackDetection` | 无显式主键时猜测常见名称 |
| 9 | `testGetSourceTablePrimaryKeyThrowsOnMissingSchema` | Schema 缺失时抛出明确异常 |
| 10 | `testFindSchemaInfoByTableNameInSqlSuccess` | 辅助方法正常查找 |
| 11 | `testFindSchemaInfoByTableNameInSqlNotFound` | 辅助方法未找到时返回 null |

#### **Mock 对象示例**

```java
@Test
void testGetSourceTablePrimaryKeyFromSchema() throws SQLException {
    // 准备 Mock 数据
    Map<String, NodeSchemaInfo> mockSchemaMap = new HashMap<>();
    
    NodeSchemaInfo mockSchema = Mockito.mock(NodeSchemaInfo.class);
    when(mockSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("user_id"));
    when(mockSchema.getFieldNames()).thenReturn(Arrays.asList("user_id", "name", "email"));
    when(mockSchema.getFieldMap()).thenReturn(new HashMap<>());
    
    mockSchemaMap.put("node_mysql_1", mockSchema);
    
    FromTableConfig fromTable = new FromTableConfig("node_mysql_1", "users");
    List<FromTableConfig> fromTables = Collections.singletonList(fromTable);
    
    // 创建 Calculator
    AffectedKeyCalculator calculator = new AffectedKeyCalculator(
        "wide_table_pk",
        "users",
        "user_id",
        fromTables,
        Collections.emptyMap(),
        mock(DuckDbOperator.class),
        mockSchemaMap,
        "SELECT * FROM mysql_source_1__users"
    );
    
    // 执行测试
    String pk = Whitebox.invokeMethod(calculator, "getSourceTablePrimaryKey", "users");
    
    // 验证结果
    assertEquals("user_id", pk);
}
```

---

## ⚠️ **风险与缓解**

### **风险 1: 向后兼容性破坏**

**描述：** 移除旧构造器可能导致其他使用方编译失败

**缓解措施：**
- ✅ 编译检查：修改后运行 `mvn compile` 确认无遗漏
- ✅ IDE 搜索：全局搜索 `new AffectedKeyCalculator(` 确认所有调用点
- ✅ 文档更新：在 Javadoc 中标注旧构造器已移除

### **风险 2: 时序错误**

**描述：** 如果在 `resolveSqlTableAliases()` 之前创建 AffectedKeyCalculator，会导致 querySql 未解析

**缓解措施：**
- ✅ 构造器校验：检查 querySql 是否包含未解析的别名（可选）
- ✅ 日志警告：在 doInit() 中添加顺序校验日志
- ✅ 单元测试：验证构造器接收到的 querySql 是已解析版本

### **风险 3: 性能影响**

**描述：** 新增 `findSchemaInfoByTableNameInSql()` 可能增加查找开销

**缓解措施：**
- ✅ 时间复杂度：O(N)，N = fromTables 数量（通常 < 10）
- ✅ 缓存优化：可在内部缓存 tableNameInSql → NodeSchemaInfo 映射（可选）
- ✅ 日志监控：添加耗时日志以便后续优化

---

## 📊 **成功标准**

### **功能验证**

- [ ] `mvn compile` 编译通过（0 错误）
- [ ] `mvn test -pl iengine-app -Dtest=AffectedKeyCalculatorRefactoredTest` 测试全部通过
- [ ] 所有 11 个单元测试用例执行成功
- [ ] 日志中无 "removed from FromTableConfig" 警告

### **质量指标**

- [ ] 代码覆盖率 ≥ 80%（针对重写的 3 个方法）
- [ ] 无 `@Deprecated` 注释残留
- [ ] 无 TODO/FIXME 标记（除了 Line 577 的 SQL 解析功能）

### **集成验证**

- [ ] HazelcastDuckDbSqlNode 初始化流程正常
- [ ] AffectedKeyCalculator.calculateAffectedKeysFromEvents() 可正常调用
- [ ] 宽表增量更新功能不受影响

---

## 📝 **实施检查清单**

### **编码阶段**

- [ ] 修改 `AffectedKeyCalculator.java`:
  - [ ] 更新字段声明（添加 nodeSchemaMap, resolvedQuerySql）
  - [ ] 移除旧构造器（2 个）
  - [ ] 添加新构造器（带完整参数和校验）
  - [ ] 重写 `getQuerySqlForTable()`
  - [ ] 重写 `getTableFields()`
  - [ ] 重写 `getSourceTablePrimaryKey()`
  - [ ] 添加 `findSchemaInfoByTableNameInSql()` 辅助方法
  - [ ] 移除所有 "removed from FromTableConfig" 警告日志

- [ ] 修改 `HazelcastDuckDbSqlNode.java`:
  - [ ] 更新 AffectedKeyCalculator 构造器调用（传入 nodeSchemaCache + querySql）
  - [ ] （可选）添加 `getResolvedQuerySql()` getter
  - [ ] （可选）添加 `getNodeSchemaCache()` getter

- [ ] 创建测试文件:
  - [ ] `AffectedKeyCalculatorRefactoredTest.java`
  - [ ] 实现 11 个测试用例

### **验证阶段**

- [ ] 运行编译：`mvn compile -pl iengine-app -am`
- [ ] 运行单元测试：`mvn test -pl iengine-app -Dtest=AffectedKeyCalculatorRefactoredTest`
- [ ] 运行完整测试套件：`mvn test -pl iengine-app`
- [ ] 检查日志：确认无废弃 API 警告

### **文档阶段**

- [ ] 更新 Javadoc（如有变动）
- [ ] （可选）更新 README 或架构文档

---

## 🔄 **回滚方案**

如果出现问题，可通过以下步骤快速回滚：

```bash
git checkout HEAD~1 -- \
  iengine/iengine-app/src/main/java/.../AffectedKeyCalculator.java \
  iengine/iengine-app/src/main/java/.../HazelcastDuckDbSqlNode.java

mvn compile -pl iengine-app -am
```

---

## 📚 **参考文档**

- [FromTableConfig 设计](../specs/2026-05-30-duckdb-sql-table-mapping-design.md)
- [NodeSchemaInfo API](../../../iengine/iengine-app/src/main/java/.../duckdb/NodeSchemaInfo.java)
- [HazelcastDuckDbSqlNode 初始化流程](../plans/2026-05-30-duckdb-sql-table-mapping-plan.md)

---

**设计者:** AI Assistant (Trae IDE)  
**最后更新:** 2026-05-31  
**状态:** ✅ 设计完成，待用户审批
