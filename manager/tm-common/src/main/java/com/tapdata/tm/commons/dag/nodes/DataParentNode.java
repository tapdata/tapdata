package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
        String tableName = transformTableName(this instanceof TableNode ? ((TableNode) this).getTableName() : originTableName);

        s.setOriginalName(tableName);
        s.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(_metaType, dataSource, tableName));

        SchemaTransformerResult str = new SchemaTransformerResult();
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
        str.setSinkQulifiedName(s.getQualifiedName());

        schemaTransformerResults.add(str);
    }


    protected List<Field> transformFields(List<String> inputFields, Schema s) {
        List<Field> fields = s.getFields();
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
}
