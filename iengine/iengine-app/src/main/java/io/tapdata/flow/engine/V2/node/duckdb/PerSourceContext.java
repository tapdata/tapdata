package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds isolated DuckDB state for one sourceId:tableId pair.
 * 
 * <p>增强版：包含预加载的 Schema 信息，运行时可直接访问，
 * 无需重复查询 TapTableMap，达到性能最优。</p>
 */
@Getter
@Setter
public class PerSourceContext {

    private final String key;
    private final DuckDbOperator operator;
    private volatile String targetTableName;
    private final List<TapdataEvent> batchBuffer = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean tableInitialized;
    private final AtomicInteger accumulatedRecordCount = new AtomicInteger(0);
    private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
    private final Object commitLock = new Object();
    private int batchSize = 1000;
    private long timeout = 500; //ms
    long lastAcceptTime = -1;

    /**
     * ⭐ 新增：预加载的完整 Schema 信息
     * 包含：tableName、qualifiedName、主键、字段名、字段类型等
     * 
     * 性能优势：
     * - 初始化时一次性加载（O(1) 次查询）
     * - 运行时直接访问（0 次额外查询）
     * - 避免重复的 HashMap/集合操作
     */
    private volatile NodeSchemaInfo schema;
    private final String sourceId;
    private final String tableId;

    public PerSourceContext(String key, DuckDbOperator operator) {
        this(key, operator, null, null, null);
    }

    public PerSourceContext(String key, DuckDbOperator operator,
                            String sourceId, String tableId,
                            NodeSchemaInfo schemaInfo) {
        this.key = key;
        this.operator = operator;
        this.sourceId = sourceId;
        this.tableId = tableId;
        this.schema = schemaInfo;
    }

    public void addEvent(TapdataEvent event) {
        batchBuffer.add(event);
        accumulatedRecordCount.incrementAndGet();
    }

    public List<TapdataEvent> drainBuffer() {
        synchronized (batchBuffer) {
            List<TapdataEvent> copy = new ArrayList<>(batchBuffer);
            resetBuffer();
            return copy;
        }
    }

    public void resetBuffer() {
        synchronized (batchBuffer) {
            batchBuffer.clear();
            accumulatedRecordCount.set(0);
            lastCommitTime.set(System.currentTimeMillis());
        }
    }

    /**
     * 检查是否已加载 Schema 信息
     */
    public boolean hasSchema() {
        return schema != null && schema.isValid();
    }

    /**
     * 快速获取表名（从预加载的 Schema）
     */
    public String getTableNameFromSchema() {
        return schema != null ? schema.getTableName() : null;
    }

    /**
     * 快速获取完全限定名（从预加载的 Schema）
     */
    public String getQualifiedNameFromSchema() {
        return schema != null ? schema.getQualifiedName() : null;
    }

    /**
     * 快速判断字段是否是主键
     */
    public boolean isPrimaryKeyField(String fieldName) {
        return schema != null && schema.isPrimaryKey(fieldName);
    }

    /**
     * 快速获取字段类型
     */
    public io.tapdata.entity.schema.type.TapType getFieldType(String fieldName) {
        return schema != null ? schema.getFieldType(fieldName) : null;
    }

    public void updateAcceptor(int batchSize, long timeout) {
        this.batchSize = batchSize;
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PerSourceContext{");
        sb.append("key='").append(key).append('\'');
        sb.append(", targetTable='").append(targetTableName).append('\'');
        
        if (schema != null) {
            sb.append(", schema={");
            sb.append("table='").append(schema.getTableName()).append('\'');
            sb.append(", qn='").append(schema.getQualifiedName()).append('\'');
            sb.append(", pkCount=").append(schema.getPrimaryKeys().size());
            sb.append(", fieldCount=").append(schema.getFieldCount());
            sb.append('}');
        } else {
            sb.append(", schema=null");
        }
        
        sb.append(", bufferSize=").append(batchBuffer.size());
        sb.append(", initialized=").append(tableInitialized);
        sb.append('}');
        return sb.toString();
    }

    public boolean needAccept() {
        if (getBatchBuffer().size() >= getBatchSize()) {
            lastCommitTime.set(System.currentTimeMillis());
            return true;
        }
        if (lastAcceptTime > 0L && System.currentTimeMillis() - lastAcceptTime > timeout) {
            lastAcceptTime = System.currentTimeMillis();
            return true;
        }
        if (lastAcceptTime <= 0L) {
            lastAcceptTime = System.currentTimeMillis();
        }
        return false;
    }
}
