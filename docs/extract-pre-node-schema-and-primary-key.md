# 🔍 提取前置节点 Schema 及其主键字段名的方法

> **参考源码：** HazelcastMergeNode.java（主从合并节点）
>
> **源码路径：** `/Users/hj/workspace/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastMergeNode.java`
>
> **适用场景：** 在自定义节点中获取输入节点的表结构（Schema）和主键字段信息

---

## 📌 核心方法总览

在 HazelcastMergeNode 中，提取前置节点 Schema 和主键信息的流程分为 **3 个关键步骤**：

```
步骤 1: initSourceNodeMap()        → 获取前置节点对象
步骤 2: initSourceConnectionMap()  → 获取前置节点连接信息
步骤 3: initSourcePkOrUniqueFieldMap() → 提取主键/唯一键字段名 ⭐ 核心方法
```

---

## 🎯 方法一：initSourceNodeMap() - 获取前置节点对象

### 📍 源码位置

**文件：** HazelcastMergeNode.java  
**行号：** 第 952-968 行

### ✅ 完整源码

```java
/**
 * 初始化源节点映射表
 * 遍历合并配置中的每个属性，通过 getSourceTableNode() 获取对应的前置 TableNode
 *
 * @param mergeTableProperties 合并配置列表（首次调用时传 null，内部自动获取）
 */
private void initSourceNodeMap(List<MergeTableProperties> mergeTableProperties) {
    // 1️⃣ 首次调用时初始化 Map 并获取合并配置
    if (null == mergeTableProperties) {
        this.sourceNodeMap = new HashMap<>();
        Node<?> node = this.processorBaseContext.getNode();
        mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
    }
    
    // 2️⃣ 空配置直接返回
    if (CollectionUtils.isEmpty(mergeTableProperties)) return;
    
    // 3️⃣ 遍历每个合并配置项
    for (MergeTableProperties mergeProperty : mergeTableProperties) {
        // 4️⃣ 通过节点 ID 获取前置的 TableNode 对象
        Node<?> sourceTableNode = getSourceTableNode(mergeProperty.getId());
        
        // 5️⃣ 类型校验：必须是 TableNode
        if (!(sourceTableNode instanceof TableNode)) {
            throw new TapCodeException(
                TaskMergeProcessorExCode_16.INIT_SOURCE_NODE_MAP_WRONG_NODE_TYPE,
                "Expect TableNode, but got: " + sourceTableNode.getClass().getSimpleName()
            );
        }
        
        // 6️⃣ 存入映射表：{"节点ID": TableNode对象}
        this.sourceNodeMap.put(mergeProperty.getId(), sourceTableNode);
        
        // 7️⃣ 递归处理子配置（支持多层嵌套）
        initSourceNodeMap(mergeProperty.getChildren());
    }
}
```

### 🔑 关键依赖方法：getSourceTableNode()

**源码位置：** HazelcastMergeNode.java 第 1762-1771 行

```java
/**
 * 根据 sourceId 获取前置的源表节点
 * 通过 DAG 图的前驱节点遍历找到目标节点
 *
 * @param sourceId 前置节点 ID（来自合并配置）
 * @return 前置的 TableNode 对象
 */
protected Node<?> getSourceTableNode(String sourceId) {
    Node<?> node = this.processorBaseContext.getNode();
    
    // 1. 获取当前节点的所有前驱节点
    List<? extends Node<?>> predecessors = node.predecessors();
    
    // 2. 过滤出 ID 匹配的目标节点
    predecessors = predecessors.stream()
        .filter(n -> n.getId().equals(sourceId))
        .collect(Collectors.toList());
    
    // 3. 通过 DAG 图遍历工具找到最终的数据源节点
    predecessors = GraphUtil.predecessors(node, Node::isDataNode, (List<Node<?>>) predecessors);
    
    // 4. 异常处理：未找到节点
    if (CollectionUtils.isEmpty(predecessors)) {
        throw new TapCodeException(
            TaskMergeProcessorExCode_16.CANNOT_FOUND_PRE_NODE,
            String.format("Source id: %s", sourceId)
        );
    }
    
    return predecessors.get(0);
}
```

### 📊 输出结果

```java
// sourceNodeMap 数据结构
Map<String, Node<?>> sourceNodeMap = {
    "node_id_1" -> TableNode{tableName="users", connectionId="conn_123"},
    "node_id_2" -> TableNode{tableName="orders", connectionId="conn_456"},
    // ... 更多前置节点
};
```

---

## 🔗 方法二：initSourceConnectionMap() - 获取连接信息

### 📍 源码位置

**文件：** HazelcastMergeNode.java  
**行号：** 第 969-985 行

### ✅ 完整源码

```java
/**
 * 初始化源连接映射表
 * 基于已获取的 sourceNodeMap，查询 MongoDB 获取连接详情
 *
 * @param mergeTableProperties 合并配置列表
 */
protected void initSourceConnectionMap(List<MergeTableProperties> mergeTableProperties) {
    // 1️⃣ 首次调用初始化
    if (null == mergeTableProperties) {
        this.sourceConnectionMap = new HashMap<>();
        Node<?> node = this.processorBaseContext.getNode();
        mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
    }
    
    if (CollectionUtils.isEmpty(mergeTableProperties)) return;
    
    for (MergeTableProperties mergeProperty : mergeTableProperties) {
        // 2️⃣ 从 sourceNodeMap 获取节点对象
        Node<?> sourceNode = this.sourceNodeMap.get(mergeProperty.getId());
        
        // 3️⃣ 提取连接 ID
        String connectionId = getConnectionId(sourceNode);
        
        // 4️⃣ 从 MongoDB 查询连接详情（排除 schema 字段以节省内存）
        Query query = new Query(Criteria.where("_id").is(connectionId));
        query.fields().exclude("schema");
        Connections connections = clientMongoOperator.findOne(
            query,
            ConnectorConstant.CONNECTION_COLLECTION,
            Connections.class
        );
        
        // 5️⃣ 存入映射表
        this.sourceConnectionMap.put(mergeProperty.getId(), connections);
        
        // 6️⃣ 递归处理子配置
        initSourceConnectionMap(mergeProperty.getChildren());
    }
}
```

### 🔑 关键依赖方法：getConnectionId()

**源码位置：** HazelcastMergeNode.java 第 1773-1785 行

```java
/**
 * 从 TableNode 中提取连接 ID
 *
 * @param preTableNode 前置表节点
 * @return 连接 ID 字符串
 */
protected String getConnectionId(Node<?> preTableNode) {
    String connectionId;
    if (preTableNode instanceof TableNode) {
        connectionId = ((TableNode) preTableNode).getConnectionId();
        if (StringUtils.isBlank(connectionId)) {
            throw new TapCodeException(
                TaskMergeProcessorExCode_16.CONNECTION_ID_CANNOT_BE_BLANK,
                String.format("Table node: %s", preTableNode)
            ).dynamicDescriptionParameters(
                preTableNode.getId(),
                preTableNode.getName()
            );
        }
    } else {
        throw new RuntimeException(
            preTableNode.getName() + "(" + preTableNode.getId() + ", " +
            preTableNode.getClass().getSimpleName() +
            ") cannot linked to a merge table node"
        );
    }
    return connectionId;
}
```

---

## ⭐ 方法三：initSourcePkOrUniqueFieldMap() - 提取主键字段名【核心】

### 📍 源码位置

**文件：** HazelcastMergeNode.java  
**行号：** 第 987-1028 行

### ✅ 完整源码（带详细注释）

```java
/**
 * ⭐ 核心方法：初始化前置节点的主键或唯一键字段映射表
 *
 * 执行逻辑：
 * 1. 遍历合并配置中的每个前置节点
 * 2. 通过 processorBaseContext.getTapTableMap() 获取该节点的 TapTable（Schema）
 * 3. 从 TapTable 中提取主键字段（primaryKeys）或数组键（arrayKeys）
 * 4. 存储到 sourcePkOrUniqueFieldMap 供后续事件处理使用
 *
 * @param mergeTableProperties 合并配置列表（首次调用传 null）
 */
protected void initSourcePkOrUniqueFieldMap(List<MergeTableProperties> mergeTableProperties) {
    // ═══════════════════════════════════════════════════════════════
    // 步骤 1：首次调用时的初始化逻辑
    // ═══════════════════════════════════════════════════════════════
    if (null == mergeTableProperties) {
        this.sourcePkOrUniqueFieldMap = new HashMap<>();
        Node<?> node = this.processorBaseContext.getNode();
        mergeTableProperties = ((MergeTableNode) node).getMergeProperties();
    }
    
    if (CollectionUtils.isEmpty(mergeTableProperties)) return;
    
    // ═══════════════════════════════════════════════════════════════
    // 步骤 2：获取全局 TapTableMap（包含所有节点的 Schema 信息）
    // ═══════════════════════════════════════════════════════════════
    TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
    
    for (MergeTableProperties mergeProperty : mergeTableProperties) {
        // ───────────────────────────────────────────────────────────
        // 步骤 3：获取前置节点的基本信息
        // ───────────────────────────────────────────────────────────
        String sourceNodeId = mergeProperty.getId();  // 前置节点 ID
        
        // 3.1 从全局节点列表中查找前置节点
        Node<?> preNode = processorBaseContext.getNodes().stream()
            .filter(n -> n.getId().equals(sourceNodeId))
            .findFirst()
            .orElse(null);
        
        if (null == preNode) {
            throw new TapCodeException(
                TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NODE_NOT_FOUND,
                String.format("- Node ID: %s", sourceNodeId)
            ).dynamicDescriptionParameters(sourceNodeId);
        }
        
        // 3.2 跳过禁用的节点
        if (!preNode.disabledNode()) {
            
            // ───────────────────────────────────────────────────────
            // 步骤 4：获取表名和 TapTable（Schema 对象）
            // ───────────────────────────────────────────────────────
            String nodeName = preNode.getName();
            String tableName = getTableName(preNode);  // ⭐ 关键方法调用
            
            // 4.1 从 TapTableMap 中获取完整的表结构定义
            TapTable tapTable = tapTableMap.get(tableName);
            
            // ───────────────────────────────────────────────────────
            // 步骤 5：确定主键字段名的优先级策略
            // ───────────────────────────────────────────────────────
            MergeTableProperties.MergeType mergeType = mergeProperty.getMergeType();
            List<String> arrayKeys = mergeProperty.getArrayKeys();  // 数组关联键
            
            // 5.1 获取表的主键字段集合
            Collection<String> primaryKeys = tapTable.primaryKeys(true);
            
            List<String> fieldNames;
            
            // ⚡ 优先级判断：
            // ① 如果配置了 arrayKeys（数组关联场景）→ 使用 arrayKeys
            if (CollectionUtils.isNotEmpty(arrayKeys)) {
                fieldNames = new ArrayList<>(arrayKeys);
                
            // ② 如果表有主键 → 使用主键
            } else if (CollectionUtils.isNotEmpty(primaryKeys)) {
                fieldNames = new ArrayList<>(primaryKeys);
                
            // ③ 都没有 → 抛出异常（必须有主键才能进行合并操作）
            } else {
                throw new TapCodeException(
                    TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_PRIMARY_KEY,
                    String.format(
                        "- Table name: %s\n- Node name: %s\n- Merge operation: %s",
                        tableName, nodeName, mergeType
                    )
                ).dynamicDescriptionParameters(
                    tableName, nodeName, sourceNodeId, mergeType
                );
            }
            
            // 5.2 特殊校验：updateIntoArray 类型必须配置 arrayKeys
            if (mergeType == MergeTableProperties.MergeType.updateIntoArray
                && CollectionUtils.isEmpty(arrayKeys)) {
                throw new TapCodeException(
                    TaskMergeProcessorExCode_16.TAP_MERGE_TABLE_NO_ARRAY_KEY,
                    String.format("- Table name: %s- Node name: %s\n", tableName, nodeName)
                ).dynamicDescriptionParameters(sourceNodeId, tableName, nodeName);
            }
            
            // ───────────────────────────────────────────────────────
            // 步骤 6：存储结果到映射表
            // ───────────────────────────────────────────────────────
            this.sourcePkOrUniqueFieldMap.put(sourceNodeId, fieldNames);
        }
        
        // ───────────────────────────────────────────────────────────
        // 步骤 7：递归处理子配置（支持多层级联）
        // ───────────────────────────────────────────────────────────
        initSourcePkOrUniqueFieldMap(mergeProperty.getChildren());
    }
}
```

### 🔑 关键依赖方法：getTableName()

**源码位置：** HazelcastProcessorBaseNode.java（基类）第 724-734 行

```java
/**
 * 从节点对象中提取表名
 * 如果是 TableNode，返回 tableName 属性
 * 否则返回节点 ID 作为表名
 *
 * @param node 节点对象
 * @return 表名字符串
 */
protected String getTableName(Node<?> node) {
    String tableName;
    if (node instanceof TableNode) {
        tableName = ((TableNode) node).getTableName();
        if (StringUtils.isBlank(tableName)) {
            throw new TapCodeException(
                TaskMergeProcessorExCode_16.TABLE_NAME_CANNOT_BE_BLANK,
                String.format("Table node: %s", node)
            ).dynamicDescriptionParameters(node.getId(), node.getName());
        }
    } else {
        tableName = node.getId();  // 非 TableNode 使用节点 ID
    }
    return tableName;
}
```

### 🔑 关键依赖方法：getPreNode()

**源码位置：** HazelcastMergeNode.java 第 528-535 行

```java
/**
 * 根据 ID 获取前置节点对象（带缓存机制）
 * 使用 ConcurrentHashMap 缓存已查询的节点，避免重复查找
 *
 * @param preNodeId 前置节点 ID
 * @return 节点对象
 */
protected Node<?> getPreNode(String preNodeId) {
    return preNodeMap.computeIfAbsent(preNodeId, k -> {
        Node<?> foundNode = processorBaseContext.getNodes().stream()
            .filter(n -> n.getId().equals(preNodeId))
            .findFirst()
            .orElse(null);
        if (null == foundNode)
            throw new TapCodeException(
                TaskMergeProcessorExCode_16.CANNOT_GET_PRENODE_BY_ID,
                String.format("Node id: %s", preNodeId)
            ).dynamicDescriptionParameters(preNodeId);
        return foundNode;
    });
}
```

### 📊 输出结果示例

```java
// sourcePkOrUniqueFieldMap 数据结构
Map<String, List<String>> sourcePkOrUniqueFieldMap = {
    "node_id_1" -> ["user_id"],           // users 表的主键
    "node_id_2" -> ["order_id"],          // orders 表的主键
    "node_id_3" -> ["product_id", "sku"]  // products 表的复合主键
};
```

---

## 🔄 完整调用链路图

```
┌─────────────────────────────────────────────────────────────────┐
│                    doInit() [节点初始化入口]                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              initRuntimeParameters() [运行时参数初始化]             │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 1. initSourceNodeMap(null)          ← 步骤 1                  │ │
│  │    └─ getSourceTableNode(id)       ← 获取 TableNode          │ │
│  │                                                             │ │
│  │ 2. initSourceConnectionMap(null)   ← 步骤 2                  │ │
│  │    └─ getConnectionId(node)        ← 获取连接 ID              │ │
│  │    └─ MongoDB 查询 Connections     ← 获取连接详情             │ │
│  │                                                             │ │
│  │ 3. initSourcePkOrUniqueFieldMap(null) ← ⭐ 步骤 3（核心）      │ │
│  │    ├─ getPreNode(id)               ← 获取前置节点             │ │
│  │    ├─ getTableName(preNode)        ← 获取表名                │ │
│  │    ├─ tapTableMap.get(tableName)   ← 获取 TapTable (Schema)  │ │
│  │    ├─ tapTable.primaryKeys(true)   ← 提取主键字段             │ │
│  │    └─ 存储到 sourcePkOrUniqueFieldMap                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              tryProcess() [事件处理阶段]                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 使用 sourcePkOrUniqueFieldMap 获取主键字段名用于：             │ │
│  │ • 构建缓存 Key                                                │ │
│  │ • 数据匹配与合并                                              │ │
│  │ • 事件去重                                                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 💡 TapTable 对象详解

### 📍 什么是 TapTable？

`TapTable` 是 Tapdata 平台中表结构的核心数据模型，包含了表的完整 Schema 信息：

```java
public class TapTable {
    private String name;                          // 表名
    private Map<String, TapField> nameFieldMap;   // 字段名 → 字段定义 映射
    private List<TapIndex> indexes;               // 索引列表
    private List<TapField> primaryKeys;           // 主键字段列表（原始顺序）
    // ... 其他属性
}
```

### 🔧 核心方法：primaryKeys(boolean sorted)

```java
/**
 * 获取主键字段名列表
 *
 * @param sorted 是否按字段名排序（true=排序，false=原始顺序）
 * @return 主键字段名集合
 */
public Collection<String> primaryKeys(boolean sorted) {
    // 内部实现：遍历 indexes 找到 PRIMARY 类型的索引
    // 返回对应的 TapIndexField 名称列表
}
```

### 📊 TapTable 示例数据

假设有一个 `users` 表：

```sql
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(200),
    created_at TIMESTAMP,
    UNIQUE KEY idx_email (email)
);
```

对应的 `TapTable` 对象：

```java
TapTable tapTable = tapTableMap.get("users");

tapTable.getName();                              // → "users"
tapTable.getNameFieldMap().keySet();              // → [user_id, username, email, created_at]
tapTable.primaryKeys(true);                       // → ["user_id"]
tapTable.getNameFieldMap().get("user_id");        // → TapField{name="user_id", type=TapInt, primaryKey=true}
tapTable.getIndexByTypeName("PRIMARY");          // → TapIndex{name="PRIMARY", fields=[user_id]}
tapTable.getIndexByTypeName("UNIQUE");           // → [TapIndex{name="idx_email", fields=[email]}]
```

---

## 🎯 在您的节点中如何使用

### 📝 示例代码模板

基于 HazelcastMergeNode 的实现模式，您可以在自定义节点中这样使用：

```java
public class YourCustomNode extends HazelcastProcessorBaseNode {
    
    // 存储前置节点的主键信息
    private Map<String, List<String>> sourcePkOrUniqueFieldMap;
    
    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        
        // 初始化运行时参数（包括 Schema 和主键提取）
        initRuntimeParameters();
    }
    
    private void initRuntimeParameters() {
        // 步骤 1：获取前置节点映射
        initSourceNodeMap(null);
        
        // 步骤 2：提取主键字段名 ⭐
        initSourcePkOrUniqueFieldMap(null);
    }
    
    /**
     * 自定义实现：提取前置节点的主键字段名
     */
    protected void initSourcePkOrUniqueFieldMap(List<MergeTableProperties> config) {
        if (null == config) {
            this.sourcePkOrUniqueFieldMap = new HashMap<>();
            // TODO: 从您的节点配置中获取输入节点列表
            config = getInputNodeConfig();
        }
        
        if (CollectionUtils.isEmpty(config)) return;
        
        // 获取全局 TapTableMap（所有节点的 Schema 信息）
        TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
        
        for (YourConfigItem item : config) {
            String nodeId = item.getId();
            
            // 1. 获取前置节点
            Node<?> preNode = getPreNode(nodeId);
            
            // 2. 获取表名
            String tableName = getTableName(preNode);
            
            // 3. 获取 TapTable（完整 Schema）
            TapTable tapTable = tapTableMap.get(tableName);
            
            // 4. 提取主键字段名
            Collection<String> primaryKeys = tapTable.primaryKeys(true);
            List<String> pkFields = new ArrayList<>(primaryKeys);
            
            // 5. 存储
            this.sourcePkOrUniqueFieldMap.put(nodeId, pkFields);
            
            // 可选：记录日志
            logger.info("Node {} (table {}) has primary keys: {}", 
                nodeId, tableName, pkFields);
        }
    }
    
    @Override
    protected void tryProcess(List<BatchEventWrapper> events, ...) {
        for (BatchEventWrapper event : events) {
            String sourceNodeId = event.getTapdataEvent().getNodeIds().get(0);
            
            // 使用已提取的主键信息
            List<String> pkFields = sourcePkOrUniqueFieldMap.get(sourceNodeId);
            
            // TODO: 基于 pkFields 处理业务逻辑
        }
    }
}
```

---

## ⚠️ 重要注意事项

### 1️⃣ **调用时机**

这些初始化方法必须在 `doInit()` 或 `tryProcess()` **之前**完成，通常放在 `initRuntimeParameters()` 中。

### 2️⃣ **递归处理嵌套配置**

HazelcastMergeNode 支持多层级的合并配置（父子关系），因此使用了递归调用：
```java
initSourcePkOrUniqueFieldMap(mergeProperty.getChildren());
```
如果您的节点是扁平结构，可以移除递归逻辑。

### 3️⃣ **异常处理**

必须处理以下异常情况：
- ❌ 节点不存在（`TAP_MERGE_TABLE_NODE_NOT_FOUND`）
- ❌ 表无主键（`TAP_MERGE_TABLE_NO_PRIMARY_KEY`）
- ❌ 数组类型缺少数组键（`TAP_MERGE_TABLE_NO_ARRAY_KEY`）

### 4️⃣ **性能优化**

- `preNodeMap` 使用 `ConcurrentHashMap` 缓存节点查询结果
- `sourcePkOrUniqueFieldMap` 只在初始化时计算一次，后续直接读取
- 避免在 `tryProcess()` 高频调用中重复查询数据库

### 5️⃣ **TapTableMap 数据来源**

`processorBaseContext.getTapTableMap()` 的数据来源：
- 任务启动时从源连接器获取 Schema
- 存储在内存中的全局映射表
- Key 为表名，Value 为完整的 TapTable 对象

---

## 📚 相关 API 参考

### 核心类和方法

| 类/接口 | 方法 | 说明 |
|---------|------|------|
| `DataProcessorContext` | `getTapTableMap()` | 获取全局表结构映射 |
| `DataProcessorContext` | `getNodes()` | 获取 DAG 中所有节点 |
| `DataProcessorContext` | `getNode()` | 获取当前处理器节点 |
| `TapTableMap<K, V>` | `get(tableName)` | 根据表名获取 TapTable |
| `TapTable` | `primaryKeys(boolean)` | 获取主键字段名列表 |
| `TapTable` | `getNameFieldMap()` | 获取字段名→字段定义映射 |
| `TapTable` | `getIndexByTypeName(type)` | 按类型获取索引 |
| `TableNode` | `getTableName()` | 获取表名 |
| `TableNode` | `getConnectionId()` | 获取连接 ID |
| `Node` | `getId()` | 获取节点 ID |
| `Node` | `getName()` | 获取节点名称 |
| `GraphUtil` | `predecessors(node, ...)` | DAG 图前驱遍历 |

### 数据结构

```java
// TapTableMap: 全局表结构映射
TapTableMap<String, TapTable> tapTableMap;

// TapTable: 单个表的结构定义
TapTable {
    String name;                                    // 表名
    Map<String, TapField> nameFieldMap;             // 字段映射
    List<TapIndex> indexes;                         // 索引列表
}

// TapField: 字段定义
TapField {
    String name;                                    // 字段名
    TapType tapType;                                // 字段类型（TapInt, TapString...）
    boolean primaryKey;                             // 是否主键
    boolean unique;                                 // 是否唯一键
}

// TapIndex: 索引定义
TapIndex {
    String name;                                    // 索引名
    String indexType;                               // 索引类型（PRIMARY, UNIQUE...）
    List<TapIndexField> indexFieldList;             // 索引字段列表
}
```

---

## 🎉 总结

通过学习 HazelcastMergeNode 的源码实现，我们掌握了在 Tapdata 自定义节点中提取前置节点 Schema 和主键信息的完整方法：

### ✅ **三步法**

1. **initSourceNodeMap()** → 获取前置节点对象（TableNode）
2. **initSourceConnectionMap()** → 获取连接详情（可选）
3. **initSourcePkOrUniqueFieldMap()** → ⭐ 提取主键字段名（核心）

### ✅ **关键技术点**

- 使用 `processorBaseContext.getTapTableMap()` 获取全局 Schema
- 通过 `getTableName()` 从节点提取表名
- 调用 `tapTable.primaryKeys(true)` 获取主键字段
- 支持递归处理多层嵌套配置
- 使用缓存机制提升性能

### ✅ **适用场景**

- 数据合并节点（如 HazelcastMergeNode）
- 数据转换节点（需要根据主键匹配数据）
- 数据聚合节点（需要按主键分组）
- 任何需要访问输入表结构的自定义处理器节点

---

**📖 文档版本：** v1.0
**📅 生成日期：** 2026-05-29
**🎯 参考源码：** HazelcastMergeNode.java (1400+ 行生产级代码)
