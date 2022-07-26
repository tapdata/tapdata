package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
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

    private List<String> disabledEvents;
	/** 是否开启全量并发写入*/
	private Boolean initialConcurrent;
	/** 全量写入线程数*/
	private Integer initialConcurrentWriteNum;
	/** 是否开启增量并发写入*/
	private Boolean cdcConcurrent;
	/** 增量写入线程数*/
	private Integer cdcConcurrentWriteNum;
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
        if (TaskDto.SYNC_TYPE_MIGRATE.equals(syncType)) {
            str.setSinkQulifiedName(MetaDataBuilderUtils.generateQualifiedName(_metaType, dataSource, originTableName));

            // 获取字段编辑节点 的字段映射信息
            LinkedList<MigrateFieldRenameProcessorNode> fieldRenameProcessorNodes = predecessors().stream()
                    .filter(node -> node instanceof MigrateFieldRenameProcessorNode)
                    .map(node -> (MigrateFieldRenameProcessorNode) node)
                    .filter(node -> CollectionUtils.isNotEmpty(node.getFieldsMapping()))
                    .collect(Collectors.toCollection(LinkedList::new));
            if (CollectionUtils.isNotEmpty(fieldRenameProcessorNodes)) {
                MigrateFieldRenameProcessorNode lastFieldNode = fieldRenameProcessorNodes.getLast();
                Map<String, LinkedList<FieldInfo>> fieldInfoMap = lastFieldNode.getFieldsMapping().stream()
                        .collect(Collectors.toMap(TableFieldInfo::getOriginTableName, TableFieldInfo::getFields));
                if (fieldInfoMap.containsKey(originTableName)) {
                    fieldInfos = fieldInfoMap.get(originTableName);
                }
            }

            // 如果是数据复制任务&有表编辑节点的话&有表改名 需要转回上一个节点的表名称
            LinkedList<TableRenameProcessNode> tableRenameProcessNodes = getPreNodes(getId()).stream().filter(node -> node instanceof TableRenameProcessNode)
                    .map(node -> (TableRenameProcessNode) node)
                    .filter(n -> Objects.nonNull(n.getTableNames()) && !n.getTableNames().isEmpty())
                    .collect(Collectors.toCollection(LinkedList::new));

            if (CollectionUtils.isNotEmpty(tableRenameProcessNodes)) {
                Map<String, TableRenameTableInfo> tableNames = tableRenameProcessNodes.getLast().currentMap();
                if (tableNames.containsKey(originTableName)) {
                    String temp = tableNames.get(originTableName).getOriginTableName();
                    originQualifiedName = StringUtils.replaceOnce(originQualifiedName, originTableName, temp);
                    originTableName = temp;
                }
            }

        } else if (TaskDto.SYNC_TYPE_SYNC.equals(syncType)) {
            String tableName = transformTableName(this instanceof TableNode ? ((TableNode) this).getTableName() : originTableName);
            s.setOriginalName(tableName);
            s.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(_metaType, dataSource, tableName));
            str.setSinkQulifiedName(s.getQualifiedName());
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

        fieldNameReduction(inputFields, fields, fieldsNameTransform);

        if (_fieldProcess != null && _fieldProcess.size() > 0) {
            List<FieldProcess> currentProcess = _fieldProcess.stream()
                    .filter(f -> s.getOriginalName() != null && s.getOriginalName().equals(transformTableName(f.getTableName())))
                    .filter(f -> f.getOperations() != null && f.getOperations().size() > 0)
                    .collect(Collectors.toList());
            currentProcess.forEach(process -> {
                process.getOperations().forEach(operation -> {
                    String field = operation.getField();
                    String op = operation.getOp();
                    String operand = operation.getOperand();
                    if ("CREATE".equalsIgnoreCase(op)) {
                        Field f = createField(operation);
                        fields.add(f);
                    } else if ("REMOVE".equalsIgnoreCase(op)) {
                        fields.forEach(f -> {
                            if (field.equals(f.getFieldName())) {
                                f.setDeleted(true);
                            }
                        });
                    } else if ("RENAME".equalsIgnoreCase(op)) {
                        fields.forEach(f -> {
                            if (field.equals(f.getOriginalFieldName())) {
                                f.setFieldName(operation.getOperand());
                            }
                        });
                    } else if ("CONVERT".equalsIgnoreCase(operand)) {
                        fields.forEach(f -> {
                            if (operation.getId().equals(f.getId())) {
                                f.setDataType(operation.getType());
                            }
                        });
                    }
                });
            });
        }

        fieldNameUpLow(inputFields, fields, fieldsNameTransform);
        return fields;
    }

    private void removeField(List<String> inputFields, List<Field> fields) {
        Iterator<Field> iterator = fields.iterator();
        while (iterator.hasNext()) {
            Field field = iterator.next();

        }
    }
}
