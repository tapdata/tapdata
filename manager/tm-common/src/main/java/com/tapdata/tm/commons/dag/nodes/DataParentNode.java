package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

/**
 * @Author: Zed
 * @Date: 2022/1/27
 * @Description:
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Slf4j
@Getter
@Setter
public abstract class DataParentNode<S> extends Node<S> {

    /** 连接id*/
    @EqField
    protected String connectionId;
    /** 数据类型 */
    @EqField
    protected String databaseType;
    @EqField
    protected String tablePrefix;
    @EqField
    protected String tableSuffix;

    @EqField
    protected String fieldsNameTransform;
    @EqField
    protected String tableNameTransform;
    @EqField
    private DmlPolicy dmlPolicy;

    private Boolean enableDDL;

    private DDLConfiguration ddlConfiguration;

    private String ignoredDDLRules;

    private ReadPartitionOptions readPartitionOptions;
    private List<String> disabledEvents;
    @Deprecated
    private Boolean enableDynamicTable;
	/** 是否开启全量并发写入*/
	private Boolean initialConcurrent;
	/** 全量写入线程数*/
	private Integer initialConcurrentWriteNum;
	/** 是否开启增量并发写入*/
	private Boolean cdcConcurrent;
	/** 增量写入线程数*/
	private Integer cdcConcurrentWriteNum;
    /** 并发写入的分区字段配置，格式: {"tableName":["fieldName"]} */
    private Map<String, List<String>> concurrentWritePartitionMap;
    /** 目标节点配置字段修改规则 */
    private List<FieldChangeRule> fieldChangeRules;

    @EqField
    private Integer readBatchSize = 500;
    @EqField
    private Integer increaseReadSize = 1;
    @EqField
    private Integer writeBatchSize;

    /** 写入每批最大等待时间 */
    @EqField
    private Long writeBatchWaitMs;


    /** 数据写入策略配置，数据写入模式： 更新已存在或者插入新数据（updateOrInsert）， 追加写入(appendWrite)， 更新写入(updateWrite) */
    @EqField
    private String writeStrategy = "updateOrInsert";

    /** 是否开启全量同步索引*/
    private Boolean syncIndexEnable;

    /** 根据更新条件创建唯一索引或普通索引，默认为 true，创建唯一索引*/
    private Boolean uniqueIndexEnable;
    /**
     * constructor for node
     *
     * @param type 节点类型
     */
    public DataParentNode(String type) {
        super(type, Node.NodeCatalog.data);
    }


    public String transformTableName(String originTableName) {
        String tableName = String.format("%s%s%s",
                tablePrefix != null ? tablePrefix : "",
                originTableName,
                tableSuffix != null ? tableSuffix : "");
        if (tableNameTransform != null) {
            if ("toUpperCase".equalsIgnoreCase(tableNameTransform)) {
                tableName = tableName.toUpperCase();
            } else if ("toLowerCase".equalsIgnoreCase(tableNameTransform)) {
                tableName = tableName.toLowerCase();
            }
        }
        return tableName;
    }

    protected void  transformResults(List<Field> targetFields, DataSourceConnectionDto dataSource, String _metaType, List<SchemaTransformerResult> schemaTransformerResults, String currentDbName, Schema s) {
        String originTableName = s.getOriginalName();
        String originQualifiedName = s.getQualifiedName();

        SchemaTransformerResult str = new SchemaTransformerResult();
        LinkedList<FieldInfo> fieldInfos = null;
        String syncType = getSyncType();
        if (StringUtils.isBlank(syncType)) {
            syncType = this instanceof DatabaseNode ? TaskDto.SYNC_TYPE_MIGRATE : TaskDto.SYNC_TYPE_SYNC;
        }

        if (TaskDto.SYNC_TYPE_MIGRATE.equals(syncType)) {
            str.setSinkQulifiedName(MetaDataBuilderUtils.generateQualifiedName(_metaType, dataSource, originTableName, getTaskId()));
        } else if (TaskDto.SYNC_TYPE_SYNC.equals(syncType)) {
            String tableName = transformTableName(this instanceof TableNode ? ((TableNode) this).getTableName() : originTableName);
            s.setOriginalName(tableName);
            s.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(_metaType, dataSource, tableName, getTaskId()));
            str.setSinkQulifiedName(s.getQualifiedName());
            str.setSinkObjectName(tableName);
        }

        int userDeleteNum = 0;
        if (CollectionUtils.isNotEmpty(targetFields)) {
            userDeleteNum = (int)targetFields.stream().filter(Field::isDeleted).count();
        }
        str.setSourceNodeId(s.getNodeId());
        str.setSourceDbName(s.getDatabase());
        str.setSourceDbType(s.getSourceNodeDatabaseType());
        str.setSourceFieldCount(s.getFields() != null ? s.getFields().size() : 0);
        str.setSourceObjectName(originTableName);
        str.setSourceQualifiedName(originQualifiedName);
        str.setSourceTableId(s.getId() == null ? null : s.getId().toHexString());
        str.setSinkNodeId(getId());
        str.setSinkObjectName(originTableName);
        str.setSinkDbName(currentDbName);
        str.setSinkStageId(getId());
        str.setSinkDbType(getDatabaseType());
        str.setUserDeletedNum(userDeleteNum);
        str.setMigrateFieldsMapping(fieldInfos);
        schemaTransformerResults.add(str);
    }


    protected List<Field> transformFields(List<String> inputFields, Schema s, List<String> inputFieldOriginalNames) {
        List<Field> fields = s.getFields();

        if (CollectionUtils.isNotEmpty(inputFieldOriginalNames)) {
            LinkedList<Node<?>> preNodes = getDag().nodeMap().get(this.getId());
            if (CollectionUtils.isNotEmpty(preNodes) && preNodes.stream().anyMatch(n -> n instanceof MigrateFieldRenameProcessorNode)) {
                Iterator<Field> iterator = fields.iterator();
                while (iterator.hasNext()) {
                    Field field = iterator.next();
                    String originalFieldName = field.getOriginalFieldName();
                    if (!inputFieldOriginalNames.contains(originalFieldName)) {
                        iterator.remove();
                    }
                }
            }
        }

        List<Node<S>> predecessors = super.predecessors();
        List<FieldProcess> _fieldProcess = null;
        if(predecessors != null && predecessors.size() > 0) {
            _fieldProcess = predecessors.stream().map(node -> {
                if (node instanceof DatabaseNode) {
                    DatabaseNode _dbNode = (DatabaseNode) node;
                    return _dbNode.getFieldProcess();
                }
                return null;
            }).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toList());
        }

        if (_fieldProcess != null && _fieldProcess.size() > 0) {
            List<FieldProcess> currentProcess = _fieldProcess.stream()
                    .filter(f -> s.getOriginalName() != null && s.getOriginalName().equals(transformTableName(f.getTableName())))
                    .filter(f -> f.getOperations() != null && f.getOperations().size() > 0)
                    .collect(Collectors.toList());



            currentProcess.forEach(process -> {

                Set<String> opFields;
                if (CollectionUtils.isNotEmpty(process.getOperations())) {
                    opFields = process.getOperations().stream().filter(p -> "RENAME".equals(p.getOp())).map(FieldProcessorNode.Operation::getField).collect(Collectors.toSet());
                } else {
                    opFields = new HashSet<>();
                }
                List<String> newInputFields = inputFields.stream().filter(f -> !opFields.contains(f)).collect(Collectors.toList());

                process.getOperations().forEach(operation -> {
                    String field = operation.getField();
                    String op = operation.getOp();
                    String operand = operation.getOperand();
                    if ("CREATE".equalsIgnoreCase(op)) {
                        Field f = createField(this.getId(), s.getOriginalName(), operation);
                        fields.add(f);
                    } else if ("REMOVE".equalsIgnoreCase(op)) {
                        fields.forEach(f -> {
                            if (field.equals(f.getFieldName())) {
                                f.setDeleted(true);
                            }
                        });
                    } else if ("RENAME".equalsIgnoreCase(op)) {
                        fields.forEach(f -> {
                            if (field.equals(f.getFieldName())) {
                                f.setFieldName(operation.getOperand());
                            }
                        });
                    } else if ("CONVERT".equalsIgnoreCase(operand)) {
                        fields.forEach(f -> {
                            if (field.equals(f.getFieldName())) {
                                f.setDataType(operation.getType());
                            }
                        });
                    }
                });
                fieldNameReduction(newInputFields, fields, fieldsNameTransform);
                fieldNameUpLow(newInputFields, fields, fieldsNameTransform);
            });
        }

        return fields;
    }

    private void removeField(List<String> inputFields, List<Field> fields) {
        Iterator<Field> iterator = fields.iterator();
        while (iterator.hasNext()) {
            Field field = iterator.next();

        }
    }

    public ReadPartitionOptions getReadPartitionOptions() {
        return readPartitionOptions;
    }

    public void setReadPartitionOptions(ReadPartitionOptions readPartitionOptions) {
        this.readPartitionOptions = readPartitionOptions;
    }

    protected void removeAllPrimaryKeys(Schema schema) {
        if(null == schema) return;
        List<Field> fields = schema.getFields();
        if(null == fields) return;
        fields.forEach(field -> {
            field.setPrimaryKey(false);
            field.setPrimaryKeyPosition(0);
        });
    }

    protected void removeAllUniqueIndex(Schema schema) {
        if(null == schema) return;
        List<TableIndex> indices = schema.getIndices();
        if(null == indices) return;
        Iterator<TableIndex> iterator = indices.iterator();
        while (iterator.hasNext()) {
            TableIndex index = iterator.next();
            if (index.isUnique()) {
                iterator.remove();
            }
        }
    }

    protected void handleAppendWrite(Schema outputSchema) {
        if (getWriteStrategy().equals(MergeTableProperties.MergeType.appendWrite.name())) {
            removeAllPrimaryKeys(outputSchema);
            removeAllUniqueIndex(outputSchema);
        }
    }
}
