package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableConfig;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableNameUtil;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableResult;
import com.tapdata.tm.commons.dag.event.WriteEvent;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.task.dto.JoinTable;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:11
 * @description
 */
@NodeType("table")
@Getter
@ToString
@Setter
public class TableNode extends DataNode {
    /** 数据源 */
    private DataSourceConnectionDto connectionDto;
    /** */
    @EqField
    private String tableName;

    /** Do you want to enable dynamic table names and add suffixes according to the rules*/
    @EqField
    private Boolean needDynamicTableName;

    /** Default rule is DateTime{yyyy-mm-dd}
     * @see com.tapdata.tm.commons.dag.dynamic.DynamicTableRule
     * */
    @EqField
    private DynamicTableConfig dynamicTableRule;

    /** 全量自定义sql*/
    @EqField
    private String totalsql;
    /** 增量自定义sql*/
    @EqField
    private String increasesql;
    /** */
    @EqField
    private Boolean isFilter = false;

    /** 全量读取方式，读取全量，自定义sql */
    @EqField
    private String totalReadMethod;
    /** 增量轮询方式， 日志cdc/自定义sql */
    @EqField
    private String increasePoll;
    /** 增量同步间隔 */
    @EqField
    private Integer increaseSyncInterval;
    /** 增量读取条数 */
    /** 全量一批读取条数 */
    /** 事务最大时长 oracle专用  单位小时  */
    @EqField
    private Double maxTransactionDuration = 12.0;
    /** 已有数据处理模式 保持已存在的数据 keepData，运行前删除已存在的数据 removeData，删除表结构 dropTable */
    @EqField
    private String existDataProcessMode = "keepData";

    /**
     * 数据写入策略
     * 插入事件 insertEvent
     *  1目标存在时更新，不存在时插入（默认）existUpdateOrNotExistInsert
     *  2目标存在时丢弃，不存在时插入 existDiscardOrNotExistInsert
     * 更新事件 updateEvent
     *  1目标存在时更新，不存在时插入existUpdateOrNotExistInsert
     *  2目标存在时更新，不存在时丢弃（默认）existUpdateOrNotExistDiscard
     * 删除事件 deleteEvent 目标存在时删除，不存在时丢弃 existDeleteOrNotExistDiscard
     */
    @EqField
    private WriteEvent writeEvent;

    /** sql增量条件*/
    @EqField
    private String initialOffset;
    /** 更新条件字段列表*/
    @EqField
    private List<String> updateConditionFields;

    /** 自定义初始化顺序值*/
    @EqField
    private Integer initialSyncOrder;
    /** 启动自动义初始化顺序 custom专用*/
    @EqField
    private Boolean enableInitialOrder;
    /** */
    @EqField
    private Boolean dataQualityTag;
    /** */
    @EqField
    private List<JoinTable> joinTables;

    /** */
    @EqField
    private Boolean dropTable;

    /** kafka topic */
    @EqField
    private String kafkaTopic;


    //xml类型的配置
    /** xml模型 */
    @EqField
    private String xmlModel;
    /** xml包含文件正则 true为包含文件正则 false为不包含文件正则 */
    @EqField
    private boolean xmlIncludeFile;
    /** xml 匹配正则 */
    @EqField
    private String xmlFileReg;
    /**  */
    @EqField
    private String xmlXPath;

    //ES的相关配置
    /** ES的index*/
    @EqField
    private String esIndex;
    /** ES的分片数量 */
    @EqField
    private Integer esFragmentNum = 3;

    //redis的相关配置
    /** redis 键*/
    @EqField
    private String redisKey;
    /** redis 键前缀 */
    @EqField
    private String redisKeyPrefix;

    private Integer rows;

    private Map<String, Object> nodeConfig;

    /** 自定义sql条件 */
    @EqField
    List<QueryOperator> conditions;
    /** 自定义sql条件 */
    @EqField
    private Integer  limit;
    /** 时差偏移量*/
    @EqField
    private Long offsetHours;

    /** 增量方式  logCdc  polling */
    private String cdcMode;

    /** 增量轮询指定字段名称 */
    private List<CdcPollingField> cdcPollingFields;
    /** 增量轮询排序方式  asc desc*/
    private String cdcPollingOrder;

    /** 增量轮询间隔  单位 毫秒 */
    private int cdcPollingInterval;
    /** 增量轮询的每次读取行数 */
    private int cdcPollingBatchSize;


    private boolean enableCustomCommand;
    private Map<String, Object> customCommand;

    private Boolean incrementExactlyOnceEnable;
    private Integer incrementExactlyOnceEnableTimeWindowDay = 3;

    private String previewQualifiedName;
    private TapTable previewTapTable;
    private static final String MONGODB = "MongoDB";

    private boolean sourceAndTarget;

    public TableNode() {
        super("table");
    }

    @Override
    protected Schema loadSchema(List<String> includes) {

        if (service == null)
            return null;

        Schema schema = service.loadSchema(ownerId(), toObjectId(connectionId), tableName);
        if (schema != null) {
            schema.setSourceNodeDatabaseType(getDatabaseType());
        }
        return schema;

    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        DynamicTableResult dynamicTableResult = this.dynamicTableName(schema);
        if (StringUtils.isBlank(tableName)) {
            return null;
        }

        //把inputSchemas的deleted的field给过滤掉
        SchemaUtils.removeDeleteFields(inputSchemas);


        // 1. 所有源库按照表名称分组合并
        // 2. 根据规则转换源库表名称，匹配目标表并合并
        DataSourceConnectionDto dataSource = service.getDataSource(connectionId);
        String metaType = "table";
        if ("mongodb".equals(dataSource.getDatabase_type())) {
            metaType = "collection";
        }
        final String _metaType = metaType;
        List<SchemaTransformerResult> schemaTransformerResults = new ArrayList<>();
        List<String> inputFields = inputSchemas.stream().map(Schema::getFields).flatMap(Collection::stream).map(Field::getFieldName).collect(Collectors.toList());
        inputSchemas.forEach(s ->
            transformResults(schema == null ? null : schema.getFields(), dataSource, _metaType, schemaTransformerResults, dataSource.getName(), s)
        );

        if (listener != null) {
            listener.schemaTransformResult(getId(), this, schemaTransformerResults);
        }
        Schema outputSchema = super.mergeSchema(inputSchemas, schema, options);

        outputSchema.setFields(transformFields(inputFields, outputSchema, null));
        long count = outputSchema.getFields().stream().filter(Field::isDeleted).count();
        long count1 = outputSchema.getFields().stream().filter(f -> !f.isDeleted()).filter(field -> field.getFieldName().contains(".")).count();
        for (SchemaTransformerResult result : schemaTransformerResults) {
                result.setUserDeletedNum((int) count);
                result.setSourceFieldCount((int) (result.getSourceFieldCount() - count1));
        }
        outputSchema.setOriginalName(tableName);
        updateSchemaAfterDynamicTableName(outputSchema, dynamicTableResult.getOldName(), dynamicTableResult.getDynamicName());
        handleAppendWrite(outputSchema);
        if (dataSource.getDatabase_type().contains(MONGODB)){
            SchemaUtils.addFieldObjectIdIfMongoDatabase(outputSchema, dataSource.getDatabase_type());
        }
        return SchemaUtils.removeSubFieldsForFreeSchemaDatasource(service.getDataSource(connectionId), outputSchema);
    }

    @Override
    protected Schema saveSchema(Collection<String> ids, String nodeId, Schema schema, DAG.Options options) {
        if (service == null) {
            return schema;
        }
        schema.setNodeId(getId());
        //schema.setTaskId(taskId());
        List<Schema> result = service.createOrUpdateSchema(ownerId(), toObjectId(connectionId), Collections.singletonList(schema), options, this);


        //service.upsertTransformTemp(this.listener.getSchemaTransformResult(nodeId), this.getDag().getTaskId().toHexString(), nodeId, 1, result, options.getUuid());
        if (result != null && result.size() > 0)
            return result.get(0);
        return schema;
    }

    @Override
    public boolean validate() {
        return super.validate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof TableNode) {
            Class className = TableNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = fieldEq(f1, f2);
                            if (!b) {
                                return false;
                            }
                        } catch (IllegalAccessException e) {
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }


    @Override
    public void fieldDdlEvent(TapDDLEvent event) throws Exception {
        updateDdlList(updateConditionFields, event);
    }

    @Data
    public static class CdcPollingField implements Serializable {
        /** 指定的轮询字段 */
        private String field;
        /** 指定的轮询字段默认值 */
        private String defaultValue;
    }

    protected DynamicTableResult dynamicTableName(Schema schema) {
        if (!Boolean.TRUE.equals(needDynamicTableName)) {
            this.tableName = getSchemaName(schema);
            updateSchemaAfterDynamicTableName(schema, tableName, null == schema ? null : schema.getAfterDynamicTableName());
            return DynamicTableResult.of();
        }
        if (null == dynamicTableRule) {
            dynamicTableRule = DynamicTableConfig.of();
        }
        String baseTable = Optional.ofNullable(getSchemaName(schema)).orElse(tableName);
        if (null != tableName && null != schema && tableName.equals(schema.getAfterDynamicTableName())) {
            baseTable = Optional.ofNullable(schema.getBeforeDynamicTableName()).orElse(tableName);
        }
        DynamicTableResult dynamicTable = DynamicTableNameUtil.getDynamicTable(
                baseTable,
                dynamicTableRule
        );
        if (null != dynamicTable) {
            this.tableName = dynamicTable.getDynamicName();
            updateSchemaAfterDynamicTableName(schema, dynamicTable.getOldName(), this.tableName);
        }
        return dynamicTable;
    }

    protected String getSchemaName(Schema schema) {
        if (null == schema) return tableName;
        if (null != dynamicTableRule) {
            String afterDynamicTableName = schema.getAfterDynamicTableName();
            if (null != afterDynamicTableName && !afterDynamicTableName.equals(tableName)) {
                return tableName;
            }
        }
        String beforeDynamicTableName = schema.getBeforeDynamicTableName();
        if (null == beforeDynamicTableName) {
            return tableName;
        }
        return beforeDynamicTableName;
    }

    protected void updateSchemaAfterDynamicTableName(Schema schema, String before, String after) {
        if (null != schema) {
            schema.setAfterDynamicTableName(after);
            schema.setBeforeDynamicTableName(before);
        }
    }
}
