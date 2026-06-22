package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 封装前置节点的完整 Schema 信息
 * 
 * <p>用于在初始化阶段一次性加载数据源的表结构信息，
 * 运行时直接从该对象获取，避免重复查询 TapTableMap，
 * 从而达到性能最优。</p>
 * 
 * <h3>包含的信息：</h3>
 * <ul>
 *   <li>基本信息：tableName、qualifiedName、sourceId</li>
 *   <li>主键信息：primaryKeys（有序列表）、primaryKeySet（快速查找）</li>
 *   <li>字段信息：fieldName → TapField 映射、fieldName → TapType 映射</li>
 *   <li>原始对象引用：tapTable（需要更复杂操作时可使用）</li>
 * </ul>
 * 
 * <h3>使用示例：</h3>
 * <pre>
 * NodeSchemaInfo schema = context.getSchema();
 * 
 * // 获取表名
 * String tableName = schema.getTableName();
 * 
 * // 判断是否是主键
 * boolean isPK = schema.isPrimaryKey("id");
 * 
 * // 获取字段类型
 * TapType type = schema.getFieldType("name");
 * 
 * // 获取所有主键
 * List&lt;String&gt; pks = schema.getPrimaryKeys();
 * </pre>
 */
public class NodeSchemaInfo {

    private final String nodeId;
    private final String tableName;
    private final String qualifiedName;
    
    private List<String> primaryKeys;
    private Set<String> primaryKeySet;
    
    private final Map<String, TapField> fieldMap;
    private final Map<String, TapType> fieldTypeMap;
    private final List<String> fieldNames;
    
    private final TapTable tapTable;
    private volatile long initializedTime;
    private volatile int fieldCount;
    
    // 预计算的Arrow Schema
    private final Schema arrowSchema;
    
    // 按 Arrow Schema 顺序排列的字段列表
    private final List<TapField> orderedFields;

    public NodeSchemaInfo(String nodeId, String tableName, String qualifiedName,
                          List<String> primaryKeys, Map<String, TapField> fieldMap,
                          TapTable tapTable, Schema arrowSchema) {
        this.nodeId = nodeId;
        this.tableName = tableName;
        this.qualifiedName = qualifiedName;
        this.tapTable = tapTable;
        initPrimaryKeys(primaryKeys);
        this.fieldMap = fieldMap != null ?
            Collections.unmodifiableMap(new LinkedHashMap<>(fieldMap)) :
            Collections.emptyMap();
        
        Map<String, TapType> typeMapBuilder = new LinkedHashMap<>();
        for (Map.Entry<String, TapField> entry : this.fieldMap.entrySet()) {
            TapField field = entry.getValue();
            if (field != null && field.getTapType() != null) {
                typeMapBuilder.put(entry.getKey(), field.getTapType());
            }
        }
        this.fieldTypeMap = Collections.unmodifiableMap(typeMapBuilder);

        this.fieldNames = new ArrayList<>();
        this.fieldMap.values()
                .stream()
                .peek(field -> {
                    if (field.getPos() != null) {
                        field.setPos(0);
                    }
                })
                .sorted(Comparator.comparing(TapField::getPos))
                .map(TapField::getOriginalFieldName)
                .forEach(this.fieldNames::add);
        this.fieldCount = this.fieldMap.size();
        this.initializedTime = System.currentTimeMillis();
        this.arrowSchema = arrowSchema;
        
        // 按 arrowSchema.fields 的顺序构建 orderedFields
        List<TapField> orderedFieldsBuilder = new ArrayList<>();
        if (arrowSchema != null && arrowSchema.getFields() != null) {
            for (org.apache.arrow.vector.types.pojo.Field arrowField : arrowSchema.getFields()) {
                String fieldName = arrowField.getName();
                TapField tapField = this.fieldMap.get(fieldName);
                if (tapField != null) {
                    orderedFieldsBuilder.add(tapField);
                }
            }
        }
        this.orderedFields = Collections.unmodifiableList(orderedFieldsBuilder);
    }

    public void initPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys != null ?
                Collections.unmodifiableList(new ArrayList<>(primaryKeys)) :
                Collections.emptyList();
        this.primaryKeySet = new HashSet<>(this.primaryKeys);
    }
    
    /**
     * 获取预计算的Arrow Schema
     */
    public Schema getArrowSchema() {
        return arrowSchema;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * 获取 DuckDB 中实际使用的目标表名（任务级唯一）
     * 
     * <p>格式：sourceId__tableName（双下划线分隔）</p>
     * <p>确保同一任务中不同数据源的表名不会冲突</p>
     * 
     * @return 目标表名，如 "mysql_source_1__users"
     */
    public String getTargetTableName() {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalStateException("Cannot generate targetTableName: sourceId is null or blank");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalStateException("Cannot generate targetTableName: tableName is null or blank");
        }
        
        String safeSourceId = DuckDbOperator.sanitizeIdentifier(nodeId);
        String safeTableName = DuckDbOperator.sanitizeIdentifier(tableName);
        
//        return safeSourceId + "__" + safeTableName;
        return safeTableName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    /**
     * 获取主键列表（有序）
     */
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * 快速判断某个字段是否是主键 (O(1))
     */
    public boolean isPrimaryKey(String fieldName) {
        return fieldName != null && primaryKeySet.contains(fieldName);
    }

    /**
     * 获取所有字段名
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    /**
     * Returns schema fields in stable order for normalization (field name and type).
     */
    /**
     * Returns schema fields in stable order for normalization (field name and type).
     * Uses insertion order of fieldMap if present, else fieldNames order.
     */
    public List<Field> getFields() {
        List<Field> result = new ArrayList<>();
        if (fieldMap != null && fieldMap instanceof LinkedHashMap) {
            for (Map.Entry<String, TapField> entry : fieldMap.entrySet()) {
                String name = entry.getKey();
                TapField tapField = entry.getValue();
                String type = tapField != null ? tapField.getDataType() : null;
                result.add(new Field(name, type));
            }
        } else {
            for (String name : getFieldNames()) {
                TapField tapField = getField(name);
                String type = tapField != null ? tapField.getDataType() : null;
                result.add(new Field(name, type));
            }
        }
        return result;
    }

    /**
     * Simple field metadata for normalization.
     */
    public static class Field {
        private final String name;
        private final String type;
        public Field(String name, String type) {
            this.name = name;
            this.type = type;
        }
        public String getName() { return name; }
        public String getType() { return type; }
    }


    /**
     * 获取字段数量
     */
    public int getFieldCount() {
        return fieldCount;
    }

    /**
     * 获取字段定义 (O(1))
     */
    public TapField getField(String fieldName) {
        return fieldMap.get(fieldName);
    }

    /**
     * 获取字段的 TapType (O(1))
     */
    public TapType getFieldType(String fieldName) {
        return fieldTypeMap.get(fieldName);
    }

    /**
     * 获取完整的字段映射
     */
    public Map<String, TapField> getFieldMap() {
        return fieldMap;
    }

    /**
     * 获取完整的字段类型映射
     */
    public Map<String, TapType> getFieldTypeMap() {
        return fieldTypeMap;
    }

    /**
     * 获取原始 TapTable 对象（需要复杂操作时使用）
     */
    public TapTable getTapTable() {
        return tapTable;
    }
    
    /**
     * 获取按 Arrow Schema 顺序排列的字段列表
     * @return 有序的 TapField 列表
     */
    public List<TapField> getOrderedFields() {
        return orderedFields;
    }

    public long getInitializedTime() {
        return initializedTime;
    }

    /**
     * 检查 Schema 是否有效
     */
    public boolean isValid() {
        return StringUtils.isNotBlank(tableName) && 
               fieldMap != null && 
               !fieldMap.isEmpty();
    }

    /**
     * 检查是否包含指定字段
     */
    public boolean hasField(String fieldName) {
        return fieldName != null && fieldMap.containsKey(fieldName);
    }

    @Override
    public String toString() {
        return "NodeSchemaInfo{" +
                "nodeId='" + nodeId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", primaryKeyCount=" + primaryKeys.size() +
                ", fieldCount=" + fieldCount +
                '}';
    }
}
