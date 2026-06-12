# ⚠️ TapTableMap 中 TableName 唯性深度分析

> **核心问题：** 如果 DuckDbSqlNode 节点的多个前置节点来自不同数据源，且配置了相同的 tableName，使用 `getTableName(preNode)` 作为 key 查询 `processorBaseContext.getTapTableMap()` 是否会有冲突？

---

## 🎯 结论先行：**是的，会有问题！但已有解决方案**

---

## 📊 一、TapTableMap 的 Key 构建机制分析

### 1.1 TapTableMap 数据结构

**源码位置：** `/Users/hj/workspace/tapdata/iengine/iengine-common/src/main/java/io/tapdata/schema/TapTableMap.java`

```java
public class TapTableMap<K extends String, V extends TapTable> extends HashMap<K, V> {
    protected final String nodeId;                    // ⭐ 节点 ID（TapTableMap 所属的处理器节点）
    protected final Long time;
    
    // ⭐⭐⭐ 核心映射：tableName -> qualifiedName
    protected final Map<K, String> tableNameAndQualifiedNameMap;
    
    protected TapTableMap(String nodeId, Long time, Map<K, String> tableNameAndQualifiedNameMap) {
        this.nodeId = nodeId;
        this.time = time;
        this.tableNameAndQualifiedNameMap = new ConcurrentHashMap<>(tableNameAndQualifiedNameMap);
    }
}
```

### 1.2 Key 的构建逻辑

**源码位置：** TapTableMap.java 第 95-103 行

```java
/**
 * 创建 TapTableMap 的工厂方法
 * 
 * @param nodeId 处理器节点 ID
 * @param tapTableList 表列表（包含所有输入表的 Schema）
 */
public static TapTableMap<String, TapTable> create(String prefix, String nodeId, 
        List<TapTable> tapTableList, Long time) {
    
    // ⭐⭐⭐ 关键：构建 tableName -> qualifiedName 映射
    HashMap<String, String> tableNameAndQualifiedNameMap = new HashMap<>();
    for (TapTable tapTable : tapTableList) {
        // Key = tapTable.getName()  ← 就是 tableName（表名）
        // Value = tapTable.getId()  ← 就是 qualifiedName（完全限定名）
        tableNameAndQualifiedNameMap.put(tapTable.getName(), tapTable.getId());
    }
    
    TapTableMap<String, TapTable> tapTableMap = create(prefix, nodeId, tableNameAndQualifiedNameMap, time);
    
    for (TapTable tapTable : tapTableList) {
        // ⚠️ 注意：这里 put 的 key 也是 tapTable.getId()（qualifiedName），不是 tableName
        tapTableMap.put(tapTable.getId(), tapTable);
    }
    
    return tapTableMap;
}
```

### 1.3 get() 方法的查询逻辑

**源码位置：** TapTableMap.java 第 226-234 行

```java
@Override
public final V get(Object key) {
    // ⭐ 外部调用时传入的是 tableName
    return getTapTable((K) key);
}

protected V getTapTable(K key) {
    V tapTable = super.get(key);  // 先从内存缓存中查找
    
    if (null == tapTable) {
        // 缓存未命中，从 MongoDB 加载
        tapTable = handleWithLock(() -> {
            V tmp = super.get(key);
            if (null == tmp) {
                tmp = findSchema(key);  // ⭐⭐⭐ 根据 key 查找 Schema
                super.put(key, tmp);   // 放入缓存
            }
            return tmp;
        });
    }
    return tapTable;
}

protected V findSchema(K k) {
    // 1. 先尝试从 tableNameAndQualifiedNameMap 获取 qualifiedName
    String qualifiedName = tableNameAndQualifiedNameMap.get(k);
    
    // 2. 如果没有找到，尝试其他策略（如心跳表、带 schema 前缀等）
    if (StringUtils.isBlank(qualifiedName)) {
        if (null != k && k.contains(".")) {
            String[] split = k.split("\\.");
            k = (K) split[split.length - 1];  // 取最后一部分作为表名
        }
        // ... 其他 fallback 逻辑
    }
    
    // 3. 使用 qualifiedName 从 MongoDB 查询完整 Schema
    Query query = Query.query(where("qualified_name").is(qualifiedName));
    TapTable tapTable = clientMongoOperator.findOne(query, url, TapTable.class);
    
    return tapTable;
}
```

---

## 🔍 二、TapTableMap 的数据来源

### 2.1 初始化入口：TapTableUtil.getTapTableMap()

**源码位置：** `/Users/hj/workspace/tapdata/iengine/iengine-common/src/main/java/io/tapdata/schema/TapTableUtil.java`

```java
/**
 * 获取节点的 TapTableMap
 *
 * @param node 节点对象（如 HazelcastMergeNode、HazelcastDuckDbSqlNode）
 * @param tmCurrentTime 当前时间戳（用于版本控制）
 * @return 该节点的 TapTableMap 实例
 */
@NotNull
public static TapTableMap<String, TapTable> getTapTableMap(String prefix, Node<?> node, Long tmCurrentTime) {
    // 1. 获取节点的所有输入表 Schema
    List<TapTable> tapTableList = getTapTables(node);
    
    TapTableMap<String, TapTable> tapTableMap;
    if (CollectionUtils.isNotEmpty(tapTableList)) {
        // 2. 创建 TapTableMap（nodeId = 当前处理器节点 ID）
        tapTableMap = TapTableMap.create(prefix, node.getId(), tapTableList, tmCurrentTime);
    } else {
        // 3. 空表时创建空的 TapTableMap
        tapTableMap = TapTableMap.create(node.getId());
    }
    return tapTableMap;
}
```

### 2.2 getTapTables() - 获取输入表列表

**源码位置：** TapTableUtil.java 第 115-140 行

```java
public static List<TapTable> getTapTables(Node<?> node) {
    // 1. 尝试获取节点的 Schema
    Object schema = node.getSchema();
    if (schema == null) {
        schema = node.getOutputSchema();
        if (schema == null) {
            List inputSchema = node.getInputSchema();
            schema = node.mergeSchema(inputSchema, null, null);  // ⭐ 合并输入 Schema
        }
    }
    
    // 2. 将 Schema 转换为 TapTable 列表
    List<Schema> schemaList = null;
    if (schema != null) {
        if (schema instanceof Schema) {
            schemaList = Collections.singletonList((Schema) schema);
        } else if (schema instanceof List) {
            schemaList = (List) schema;
        }
    }
    
    // 3. 转换为 PDK 格式的 TapTable
    if (CollectionUtils.isNotEmpty(schemaList)) {
        return schemaList.stream()
            .map(PdkSchemaConvert::toPdk)
            .collect(Collectors.toList());
    } else {
        return Collections.emptyList();
    }
}
```

### 2.3 MongoDB 存储：tableNameAndQualifiedNameMap

**源码位置：** TapTableUtil.java 第 37-44 行

```java
/**
 * 从 MongoDB 获取节点的 表名->完全限定名 映射
 * 
 * @param nodeId 处理器节点 ID
 * @return Map<tableName, qualifiedName>
 */
public static Map<String, String> getTableNameQualifiedNameMap(String nodeId) {
    return BeanUtil.getBean(ClientMongoOperator.class)
        .findOne(
            Query.query(where("nodeId").is(nodeId)),  // ⭐ 按 nodeId 查询
            ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/node/tableMap",  // MongoDB 集合
            Map.class
        );
}
```

---

## ⚠️ 三、TableName 冲突问题分析

### 3.1 问题场景示例

假设有以下数据管道配置：

```
┌─────────────────┐     ┌─────────────────┐
│ MySQL Source A  │     │ PostgreSQL Src │
│   表: users     │     │   表: users     │
│   字段: id,name │     │   字段: id,email│
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
    ┌──────────────────────────────┐
    │    DuckDbSqlNode             │
    │  (处理器节点)                 │
    │  querySql: SELECT * FROM ... │
    └──────────────────────────────┘
```

### 3.2 ⚠️ 冲突发生的位置

#### ❌ **问题代码模式（HazelcastMergeNode 风格）**

```java
// 在 initSourcePkOrUniqueFieldMap() 中
for (MergeTableProperties mergeProperty : mergeTableProperties) {
    String sourceNodeId = mergeProperty.getId();
    Node<?> preNode = getPreNode(sourceNodeId);
    
    // ⚠️ 问题：两个前置节点都返回 tableName = "users"
    String tableName = getTableName(preNode);  // → "users"
    
    // ⚠️ 问题：tapTableMap.get("users") 只能获取到一个 Schema
    TapTable tapTable = tapTableMap.get(tableName);  // ??? 获取到哪个？
    
    Collection<String> primaryKeys = tapTable.primaryKeys(true);
    this.sourcePkOrUniqueFieldMap.put(sourceNodeId, new ArrayList<>(primaryKeys));
}
```

**结果：**
- `sourcePkOrUniqueFieldMap` 中两个 sourceNodeId 都会映射到**同一个主键列表**
- 如果 MySQL 的 `users` 表主键是 `id`，PostgreSQL 的 `users` 表主键也是 `id`，可能碰巧正确
- 但如果主键不同（如一个是 `user_id`），就会导致**错误的主键信息**

### 3.3 ✅ Tapdata 平台的实际处理机制

通过深入分析源码，发现 Tapdata 平台在以下层面处理了这个问题：

#### **层面 1：qualifiedName 的全局唯一性**

虽然 TapTableMap 的 **key 是 tableName**，但其内部存储的 **value (qualifiedName)** 是全局唯一的。

**qualifiedName 的格式通常为：**
```
{connectionId}/{databaseName}/{schemaName}/{tableName}
```
或
```
{dataSourceType}:{connectionId}:{database}.{table}
```

**示例：**
```java
// MySQL Source A 的 users 表
qualifiedName = "mysql_conn_123/mydb/public/users"

// PostgreSQL Source B 的 users 表  
qualifiedName = "pg_conn_456/postgres/public/users"
```

这意味着：
- **tableNameAndQualifiedNameMap** 中：
  ```java
  {
      "users" -> "mysql_conn_123/mydb/public/users"  // ⚠️ 后者会覆盖前者！
      "users" -> "pg_conn_456/postgres/public/users"
  }
  ```
- **实际存储（super.map）中：**
  ```java
  {
      "mysql_conn_123/mydb/public/users" -> TapTable{...MySQL users 表结构...},
      "pg_conn_456/postgres/public/users" -> TapTable{...PG users 表结构...}
  }
  ```

#### **层面 2：HazelcastDuckDbSqlNode 的解决方案** ⭐⭐⭐

**源码位置：** `/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastDuckDbSqlNode.java`

HazelcastDuckDbSqlNode 已经意识到这个问题，并采用了 **组合键策略**：

##### **方案 1：使用 sourceId + tableName 组合键**

```java
// 第 664-671 行
private void processRecordEvent(TapRecordEvent recordEvent, TapdataEvent tapdataEvent,
                                BiConsumer<TapdataEvent, ProcessResult> consumer) {
    try {
        // ========== 步骤1: 获取上下文 ==========
        String tableId = TapEventUtil.getTableId(recordEvent);       // 表名（可能重复）
        String sourceId = resolveSourceId(tapdataEvent, recordEvent); // ⭐ 来源节点 ID（唯一）
        
        if (tableId == null || tableId.isEmpty()) {
            tableId = "unknown_table";
        }
        
        // ⭐⭐⭐ 组合键：避免同名表冲突
        String contextKey = buildContextKey(sourceId, tableId);       // "node_id_1:users"
        String targetTableName = buildTargetTableName(sourceId, tableId); // "node_id_1__users"
        
        PerSourceContext context = getOrCreateContext(contextKey, targetTableName);
        
        // ... 后续处理
    }
}
```

##### **核心方法实现：**

```java
// 第 1212-1221 行：解析来源 ID
private String resolveSourceId(TapdataEvent tapdataEvent, TapEvent tapEvent) {
    if (tapEvent instanceof TapBaseEvent baseEvent) {
        String associateId = baseEvent.getAssociateId();  // 优先使用关联 ID
        if (associateId != null && !associateId.isBlank()) {
            return associateId;
        }
    }
    // 其次使用 fromNodeId
    String fromNodeId = tapdataEvent != null ? tapdataEvent.getFromNodeId() : null;
    return (fromNodeId != null && !fromNodeId.isBlank()) ? fromNodeId : DEFAULT_SOURCE_ID;
}

// 第 1223-1225 行：构建上下文键（组合键）
private String buildContextKey(String sourceId, String tableId) {
    return sourceId + ":" + tableId;  // ⭐ "node_id_1:users"
}

// 第 1227-1229 行：构建目标表名（带来源前缀）
private String buildTargetTableName(String sourceId, String tableId) {
    return sanitizeIdentifier(sourceId) + "__" + sanitizeIdentifier(tableId);  // ⭐ "node_id_1__users"
}

// 第 1231-1240 行：清理标识符（确保 SQL 合法）
private String sanitizeIdentifier(String value) {
    if (value == null || value.isBlank()) {
        return "unknown";
    }
    String sanitized = value.replaceAll("[^A-Za-z0-9_]", "_");
    if (Character.isDigit(sanitized.charAt(0))) {
        return "_" + sanitized;
    }
    return sanitized;
}
```

##### **PerSourceContext 数据结构：**

```java
// 第 1242-1260 行
private PerSourceContext getOrCreateContext(String contextKey, String targetTableName) {
    synchronized (sourceContextLock) {
        PerSourceContext existing = sourceContexts.get(contextKey);
        if (existing != null) {
            sourceAccessOrder.put(contextKey, Boolean.TRUE);
            return existing;
        }
        evictIfNecessary();
        
        // 为每个 sourceId + tableName 组合创建独立的 Context
        DuckDbOperator contextOperator = createContextOperator();
        PerSourceContext context = new PerSourceContext(contextKey, contextOperator);
        context.setBatchSize(batchSize);
        // ...
        
        sourceContexts.put(contextKey, context);  // ⭐ 使用组合键存储
        return context;
    }
}
```

---

## 📋 四、完整对比分析

### 4.1 三种方案的对比

| 方案 | Key 结构 | 同名表支持 | 复杂度 | 使用场景 |
|------|----------|------------|--------|----------|
| **❌ 直接使用 tableName** | `"users"` | ❌ 不支持 | 低 | 单数据源场景 |
| **⚠️ HazelcastMergeNode 模式** | `sourceNodeId -> [pkFields]` | ⚠️ 依赖外部 Map | 中 | 主从合并（已知来源） |
| **✅ HazelcastDuckDbSqlNode 模式** | `"sourceId:tableName"` | ✅ 完全支持 | 高 | 多数据源 SQL 处理 |

### 4.2 数据流对比图

#### ❌ **错误做法：直接用 tableName 查询 TapTableMap**

```
前置节点 A (MySQL.users)          前置节点 B (PostgreSQL.users)
        │                                    │
        ▼                                    ▼
  getTableName() → "users"           getTableName() → "users"
        │                                    │
        ▼                                    ▼
  tapTableMap.get("users") ◄───────── tapTableMap.get("users")
        │                                    │
        └──────────┬─────────────────────────┘
                   ▼
        ⚠️ 只能获取到一个 TapTable（后插入的覆盖先插入的）
                   │
                   ▼
        ❌ 两个节点都使用错误的 Schema/主键信息
```

#### ✅ **正确做法：使用 sourceId + tableName 组合键**

```
前置节点 A (MySQL.users)          前置节点 B (PostgreSQL.users)
        │                                    │
        ▼                                    ▼
  resolveSourceId() → "node_A"      resolveSourceId() → "node_B"
        │                                    │
        ▼                                    ▼
  buildContextKey()                  buildContextKey()
  ("node_A", "users")                ("node_B", "users")
        │                                    │
        ▼                                    ▼
  "node_A:users" ✅ 唯一              "node_B:users" ✅ 唯一
        │                                    │
        ▼                                    ▼
  PerSourceContext A                  PerSourceContext B
  (独立的 DuckDB 临时表)               (独立的 DuckDB 临时表)
  (正确的 Schema 信息)                (正确的 Schema 信息)
```

---

## 🛠️ 五、推荐的解决方案

### 5.1 **最佳实践：使用 sourceId + tableName 组合键**

如果您正在开发自定义处理器节点（如 DuckDbSqlNode），推荐采用以下模式：

```java
public class YourCustomProcessor extends HazelcastProcessorBaseNode {
    
    // 存储每个来源节点的 Schema 和主键信息
    private final Map<String, TapTable> sourceNodeTapTableMap = new ConcurrentHashMap<>();
    private final Map<String, List<String>> sourceNodePrimaryKeyMap = new ConcurrentHashMap<>();
    
    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        
        // 初始化：为每个前置节点构建唯一的 Schema 映射
        initSourceNodeSchemaInfo();
    }
    
    /**
     * 初始化前置节点的 Schema 信息（支持同名表）
     */
    private void initSourceNodeSchemaInfo() {
        TapTableMap<String, TapTable> globalTapTableMap = processorBaseContext.getTapTableMap();
        Node<?> currentNode = processorBaseContext.getNode();
        List<? extends Node<?>> predecessors = currentNode.predecessors();
        
        for (Node<?> preNode : predecessors) {
            String sourceId = preNode.getId();  // ⭐ 来源节点 ID（唯一）
            String tableName = getTableName(preNode);
            
            // ⭐⭐⭐ 方法 1：使用 qualifiedName 查询（如果可用）
            String qualifiedName = globalTapTableMap.getQualifiedName(tableName);
            if (StringUtils.isNotBlank(qualifiedName)) {
                TapTable tapTableByQN = globalTapTableMap.get(qualifiedName);
                if (tapTableByQN != null) {
                    sourceNodeTapTableMap.put(sourceId, tapTableByQN);
                    extractAndStorePrimaryKeys(sourceId, tapTableByQN);
                    continue;
                }
            }
            
            // ⭐⭐⭐ 方法 2：直接使用 tableName（单数据源场景）
            TapTable tapTable = globalTapTableMap.get(tableName);
            if (tapTable != null) {
                // ⚠️ 注意：如果有同名表，这里可能获取到错误的 Schema
                // 建议添加日志警告
                logger.warn("Using tableName '{}' to query TapTableMap, potential conflict if multiple sources have same table name", 
                    tableName);
                
                sourceNodeTapTableMap.put(sourceId, tapTable);
                extractAndStorePrimaryKeys(sourceId, tapTable);
            } else {
                logger.error("Cannot find TapTable for source node '{}' with tableName '{}'", sourceId, tableName);
            }
        }
    }
    
    /**
     * 提取并存储主键字段名
     */
    private void extractAndStorePrimaryKeys(String sourceId, TapTable tapTable) {
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            List<String> pkFields = new ArrayList<>(primaryKeys);
            sourceNodePrimaryKeyMap.put(sourceId, pkFields);
            logger.info("Source node '{}' has primary keys: {}", sourceId, pkFields);
        } else {
            logger.warn("Source node '{}' has no primary keys defined", sourceId);
        }
    }
    
    @Override
    protected void tryProcess(List<BatchEventWrapper> events, ...) {
        for (BatchEventWrapper event : events) {
            // ⭐ 使用 sourceId 而非 tableName 来获取 Schema 信息
            String sourceId = resolveSourceId(event.getTapdataEvent());
            
            TapTable tapTable = sourceNodeTapTableMap.get(sourceId);
            List<String> pkFields = sourceNodePrimaryKeyMap.get(sourceId);
            
            if (tapTable != null && pkFields != null) {
                // 安全地使用正确的 Schema 和主键信息
                processWithCorrectSchema(event, tapTable, pkFields);
            } else {
                logger.error("No Schema info found for source '{}'", sourceId);
            }
        }
    }
    
    /**
     * 解析事件的来源节点 ID
     */
    private String resolveSourceId(TapdataEvent tapdataEvent) {
        if (tapdataEvent == null) return null;
        
        // 优先级 1：从事件本身获取
        List<String> nodeIds = tapdataEvent.getNodeIds();
        if (CollectionUtils.isNotEmpty(nodeIds)) {
            return nodeIds.get(0);
        }
        
        // 优先级 2：fromNodeId
        String fromNodeId = tapdataEvent.getFromNodeId();
        if (StringUtils.isNotBlank(fromNodeId)) {
            return fromNodeId;
        }
        
        return null;
    }
}
```

### 5.2 **替代方案：使用 Node ID 作为外层 Map 的 Key**

参考 HazelcastMergeNode 的模式（但它依赖额外的配置信息）：

```java
// HazelcastMergeNode 的做法
Map<String, List<String>> sourcePkOrUniqueFieldMap;  // sourceNodeId -> [pkFields]

// 初始化时
for (MergeTableProperties mergeProperty : mergeTableProperties) {
    String sourceNodeId = mergeProperty.getId();  // ⭐ 使用配置中的节点 ID
    
    // 这里仍然使用 tableName 查询 TapTableMap，但因为每个 sourceNodeId 只查一次，
    // 且后续使用 sourceNodeId 作为 key，所以不会混淆
    String tableName = getTableName(getPreNode(sourceNodeId));
    TapTable tapTable = tapTableMap.get(tableName);
    
    sourcePkOrUniqueFieldMap.put(sourceNodeId, extractPrimaryKeys(tapTable));
}

// 使用时
String sourceNodeId = getSourceNodeIdFromEvent(event);
List<String> pkFields = sourcePkOrUniqueFieldMap.get(sourceNodeId);  // ⭐ 用节点 ID 查询
```

---

## 📊 六、实际验证方法

### 6.1 如何检测是否存在同名表冲突

```java
@PostConstruct
public void checkForDuplicateTableNames() {
    TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
    Node<?> currentNode = processorBaseContext.getNode();
    List<? extends Node<?>> predecessors = currentNode.predecessors();
    
    Map<String, List<String>> tableNameToNodeIds = new HashMap<>();
    
    for (Node<?> preNode : predecessors) {
        String tableName = getTableName(preNode);
        tableNameToNodeIds.computeIfAbsent(tableName, k -> new ArrayList<>())
            .add(preNode.getId());
    }
    
    // 检测同名表
    for (Map.Entry<String, List<String>> entry : tableNameToNodeIds.entrySet()) {
        if (entry.getValue().size() > 1) {
            logger.warn("⚠️ DETECTED DUPLICATE TABLE NAME: '{}' is used by {} nodes: {}",
                entry.getKey(),
                entry.getValue().size(),
                entry.getValue());
                
            // 输出详细信息
            for (String nodeId : entry.getValue()) {
                Node<?> node = getPreNode(nodeId);
                String qualifiedName = tapTableMap.getQualifiedName(entry.getKey());
                logger.info("  - Node '{}' (name: '{}') uses table '{}', qualifiedName: '{}'",
                    nodeId, node.getName(), entry.getKey(), qualifiedName);
            }
        }
    }
}
```

### 6.2 日志输出示例

**无冲突时：**
```
INFO  - Source node 'mysql_src_001' has primary keys: [user_id]
INFO  - Source node 'pg_src_002' has primary keys: [order_id]
```

**有冲突时：**
```
WARN  - ⚠️ DETECTED DUPLICATE TABLE NAME: 'users' is used by 2 nodes: [mysql_src_001, pg_src_002]
INFO  -   - Node 'mysql_src_001' (name: 'MySQL Users') uses table 'users', qualifiedName: 'mysql_conn_123/mydb/public/users'
INFO  -   - Node 'pg_src_002' (name: 'PostgreSQL Users') uses table 'users', qualifiedName: 'pg_conn_456/postgres/public/users'
WARN  - Using tableName 'users' to query TapTableMap, potential conflict if multiple sources have same table name
ERROR - Cannot find correct TapTable for source node 'pg_src_002' using tableName 'users', got MySQL schema instead!
```

---

## 🎯 七、总结与建议

### ✅ **核心结论**

1. **TapTableMap 的 key 确实是 tableName**，不是 qualifiedName
2. **如果多个前置节点有相同的 tableName，直接使用 `getTableName(preNode)` 查询 TapTableMap 会产生冲突**
3. **HazelcastDuckDbSqlNode 已通过 `sourceId + tableName` 组合键解决了此问题**

### ✅ **最佳实践建议**

| 场景 | 推荐方案 | 代码复杂度 |
|------|----------|------------|
| **确定只有单数据源输入** | 直接使用 `tapTableMap.get(tableName)` | ⭐ 低 |
| **可能有多个数据源，但表名不同** | 使用 `sourceNodeId -> TapTable` 的二级 Map | ⭐⭐ 中 |
| **可能有多个数据源，且可能有同名表** | 使用 `buildContextKey(sourceId, tableName)` 组合键 | ⭐⭐⭐ 高 |

### ✅ **关键代码模板**

```java
// ✅ 推荐的安全写法
String sourceId = resolveSourceId(tapdataEvent);  // 获取来源节点 ID
String tableName = getTableName(preNode);

// 使用组合键避免冲突
String uniqueKey = sourceId + ":" + tableName;

// 存储和查询都使用组合键
TapTable tapTable = sourceNodeSchemaMap.get(uniqueKey);
List<String> pkFields = sourceNodePrimaryKeyMap.get(uniqueKey);
```

---

**📖 文档版本：** v1.0  
**📅 生成日期：** 2026-05-29  
**🎯 参考源码：** 
- TapTableMap.java (`iengine-common/src/main/java/io/tapdata/schema/TapTableMap.java`)
- TapTableUtil.java (`iengine-common/src/main/java/io/tapdata/schema/TapTableUtil.java`)
- HazelcastDuckDbSqlNode.java (`iengine-app/src/main/java/.../processor/HazelcastDuckDbSqlNode.java`)
- HazelcastMergeNode.java (`iengine-app/src/main/java/.../processor/HazelcastMergeNode.java`)
