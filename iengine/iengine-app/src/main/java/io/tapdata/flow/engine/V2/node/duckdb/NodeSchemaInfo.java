package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private final String sourceId;
    private final String tableName;
    private final String qualifiedName;
    
    private final List<String> primaryKeys;
    private final Set<String> primaryKeySet;
    
    private final Map<String, TapField> fieldMap;
    private final Map<String, TapType> fieldTypeMap;
    private final List<String> fieldNames;
    
    private final TapTable tapTable;
    private volatile long initializedTime;
    private volatile int fieldCount;

    public NodeSchemaInfo(String sourceId, String tableName, String qualifiedName,
                         List<String> primaryKeys, Map<String, TapField> fieldMap,
                         TapTable tapTable) {
        this.sourceId = sourceId;
        this.tableName = tableName;
        this.qualifiedName = qualifiedName;
        this.tapTable = tapTable;
        
        this.primaryKeys = primaryKeys != null ? 
            Collections.unmodifiableList(new ArrayList<>(primaryKeys)) : 
            Collections.emptyList();
        
        this.primaryKeySet = new HashSet<>(this.primaryKeys);
        
        this.fieldMap = fieldMap != null ?
            Collections.unmodifiableMap(new ConcurrentHashMap<>(fieldMap)) :
            Collections.emptyMap();
        
        Map<String, TapType> typeMapBuilder = new ConcurrentHashMap<>();
        if (this.fieldMap != null) {
            for (Map.Entry<String, TapField> entry : this.fieldMap.entrySet()) {
                TapField field = entry.getValue();
                if (field != null && field.getTapType() != null) {
                    typeMapBuilder.put(entry.getKey(), field.getTapType());
                }
            }
        }
        this.fieldTypeMap = Collections.unmodifiableMap(typeMapBuilder);
        
        this.fieldNames = this.fieldMap != null ?
            Collections.unmodifiableList(new ArrayList<>(this.fieldMap.keySet())) :
            Collections.emptyList();
        
        this.fieldCount = this.fieldMap.size();
        this.initializedTime = System.currentTimeMillis();
    }

    public String getSourceId() {
        return sourceId;
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
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalStateException("Cannot generate targetTableName: sourceId is null or blank");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalStateException("Cannot generate targetTableName: tableName is null or blank");
        }
        
        String safeSourceId = DuckDbOperator.sanitizeIdentifier(sourceId);
        String safeTableName = DuckDbOperator.sanitizeIdentifier(tableName);
        
        return safeSourceId + "__" + safeTableName;
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
                "sourceId='" + sourceId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", qualifiedName='" + qualifiedName + '\'' +
                ", primaryKeyCount=" + primaryKeys.size() +
                ", fieldCount=" + fieldCount +
                '}';
    }
}
