package com.tapdata.tm.commons.dag.process;
import com.tapdata.tm.commons.exception.DDLException;
import com.tapdata.tm.commons.schema.TableIndexColumn;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("join_processor")
@Getter
@Setter
@Slf4j
public class JoinProcessorNode extends ProcessorNode {

    public static final String JOIN_TYPE_LEFT = "left";
    public static final String JOIN_TYPE_RIGHT = "right";
    public static final String JOIN_TYPE_INNER = "inner";
    public static final String JOIN_TYPE_FULL = "full";

    @EqField
    private String joinType;        // 连接类型， left,right,inner,full
    @EqField
    private List<JoinExpression> joinExpressions;
    @EqField
    private Boolean embeddedMode = false;
    @EqField
    private EmbeddedSetting embeddedSetting;
    @EqField
    private String leftNodeId;
    @EqField
    private String rightNodeId;

    private List<String> leftPrimaryKeys;
    private List<String> rightPrimaryKeys;

    public JoinProcessorNode() {
        super("join_processor");
    }


    /**
     *
     *    肖贝贝设计文案
     * 对于 left join, 要求所有表均存在主键/唯一索引
     *
     * 对于非内嵌, 一条左表可能对应多条记录:
     * 1. 建表时, 去除左表的 主键, 唯一键, 索引 约束字段属性, 去除右表的主键, 非空, 索引约束属性
     * 2. 创建 左右 联合 唯一索引, 做 upsert 条件, 并自动填入到下游表
     *
     * 对于内嵌:
     * 1. 建表时, 保持左表结构不变
     * @param inputSchemas
     * @param schema
     * @return
     */
    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        if (leftNodeId == null || rightNodeId == null) {
            log.warn("No left or right nodeId specified in join process node {}.", getId());
            return super.mergeSchema(inputSchemas, schema, options);
        }
        List<Node<Schema>> predecessorNodes = predecessors();
        Node<Schema> leftNode = null;
        Node<Schema> rightNode = null;
        if (leftNodeId != null) {
            leftNode = predecessorNodes.stream().filter(n -> leftNodeId.equals(n.getId())).findFirst().orElse(null);
        }
        if (rightNodeId != null) {
            rightNode = predecessorNodes.stream().filter(n -> rightNodeId.equals(n.getId())).findFirst().orElse(null);
        }

        if (leftNode == null || rightNode == null) {
            log.warn("Left or right nodeId is invalid, not found in predecessor nodes {}.", getId());
            return super.mergeSchema(inputSchemas, schema, options);
        }

        Schema baseSchema = null;
        Schema joinSchema = null;
        if (JOIN_TYPE_RIGHT.equals(joinType)) {
            baseSchema = rightNode.getOutputSchema();
            joinSchema = leftNode.getOutputSchema();

            if (baseSchema != null) {
                rightPrimaryKeys = getPrimaryKeys(baseSchema);
            }
            if (joinSchema != null) {
                leftPrimaryKeys = getPrimaryKeys(joinSchema);
            }

        } else { // inner join, full join
            baseSchema = leftNode.getOutputSchema();
            joinSchema = rightNode.getOutputSchema();
            if (baseSchema != null) {
                leftPrimaryKeys = getPrimaryKeys(baseSchema);
            }
            if (joinSchema != null) {
                rightPrimaryKeys = getPrimaryKeys(joinSchema);
            }
        }

        if (baseSchema == null || joinSchema == null) {
            log.warn("Left or right schema is invalid, not found schema in predecessor nodes {}.", getId());
            return super.mergeSchema(inputSchemas, schema, options);
        }

        if (embeddedMode) {
            String embeddedPath = embeddedSetting == null ? joinSchema.getOriginalName() : embeddedSetting.getPath();
            String embeddedDataType = embeddedSetting == null ? "Array" :
                    (embeddedSetting.getFormat().equals("array") ? "Array" : "Map");
            if (StringUtils.isBlank(embeddedPath)) {
                embeddedPath = joinSchema.getOriginalName();
            }
            Field embeddedField = new Field();
            embeddedField.setOriginalFieldName(embeddedPath);
            embeddedField.setFieldName(embeddedPath);
            embeddedField.setJavaType(embeddedDataType);
            embeddedField.setDataType(embeddedDataType);
            embeddedField.setTableName(baseSchema.getOriginalName());
            embeddedField.setId(MetaDataBuilderUtils.generateFieldId(this.getId(), baseSchema.getOriginalName(), embeddedPath));
            embeddedField.setSource("job_analyze");

            final List<Field> baseFields = baseSchema.getFields();
            baseFields.add(embeddedField);
            for (Field field : joinSchema.getFields()) {
                field.setOriginalFieldName(String.format("%s.%s", embeddedPath, field.getOriginalFieldName()));
                baseFields.add(field);
            }
            return baseSchema;
        } else {
            List<String> basePrimaryKey = new ArrayList<>();
            final List<Field> baseFields = baseSchema.getFields();

            if (CollectionUtils.isNotEmpty(baseFields)) {
                for (Field field : baseFields) {
                    if ((field.getPrimaryKey() != null && field.getPrimaryKey()) ||
                            (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() > 0)) {
                        field.setPrimaryKey(false);
                        field.setPrimaryKeyPosition(0);
                        basePrimaryKey.add(field.getFieldName());
                    }
                }
            }

            List<TableIndex> baseUniqIndices = new ArrayList<>();
            List<TableIndex> baseIndices = baseSchema.getIndices();
            if (CollectionUtils.isNotEmpty(baseIndices)) {
                for (TableIndex index : baseIndices) {
                    if (index.isUnique()) {
                        baseUniqIndices.add(index);
                    }
                }
            }


            List<String> joinPrimaryKey = new ArrayList<>();
            for (Field field : joinSchema.getFields()) {
                if ((field.getPrimaryKey() != null && field.getPrimaryKey()) ||
                        (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() > 0) ) {
                    field.setPrimaryKey(false);
                    field.setPrimaryKeyPosition(0);
                    joinPrimaryKey.add(field.getFieldName());
                }

                field.setIsNullable(true);
                baseFields.add(field);
            }

            List<TableIndex> joinUniqIndices = new ArrayList<>();
            List<TableIndex> joinIndices = joinSchema.getIndices();
            if (CollectionUtils.isNotEmpty(joinIndices)) {
                for (TableIndex index : joinIndices) {
                    if (index.isUnique()) {
                        joinUniqIndices.add(index);
                    }
                }
            }

            TableIndex primaryUniq = getUniq(basePrimaryKey, baseUniqIndices);
            TableIndex joinUniq = getUniq(joinPrimaryKey, joinUniqIndices);
            if (primaryUniq != null && joinUniq != null) {
                primaryUniq.setIndexName("unique_index_"
                        + primaryUniq.getIndexName().replace("unique_index_", "")
                        + "_" + joinUniq.getIndexName().replace("unique_index_", ""));

                List<TableIndexColumn> primaryColumns = primaryUniq.getColumns();
                List<TableIndexColumn> joinUniqColumns = joinUniq.getColumns();
                List<String> primaryNames = primaryColumns.stream().map(TableIndexColumn::getColumnName).collect(Collectors.toList());
                for (TableIndexColumn joinUniqColumn : joinUniqColumns) {
                    if (!primaryNames.contains(joinUniqColumn.getColumnName())) {
                        primaryColumns.add(joinUniqColumn);
                    }
                }
            } else if (joinUniq != null) {
                primaryUniq = joinUniq;
            }

            if (CollectionUtils.isNotEmpty(baseIndices)) {
                baseIndices.clear();
            }

            if (primaryUniq != null) {
                if (baseIndices == null) {
                    baseIndices = new ArrayList<>();
                    baseSchema.setIndices(baseIndices);
                }
                baseIndices.add(primaryUniq);
            }
            return baseSchema;
        }
    }

    private List<String> getPrimaryKeys(Schema baseSchema) {
        List<Field> fields = baseSchema.getFields();
        List<String> primaryKeys = new ArrayList<>();
        for (Field field : fields) {
            if ((field.getPrimaryKey() != null && field.getPrimaryKey()) ||
                    (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() > 0) ) {
                primaryKeys.add(field.getFieldName());
            }
        }
        return primaryKeys;
    }


    private TableIndex getUniq(List<String> primary, List<TableIndex> tableIndices) {
        if (CollectionUtils.isNotEmpty(primary)) {
            StringBuilder builder = new StringBuilder();
            List<TableIndexColumn> tableIndexColumns = new ArrayList<>();
            for (String s : primary) {
                builder.append("_").append(s);
                TableIndexColumn tableIndexColumn = new TableIndexColumn();
                tableIndexColumn.setColumnName(s);
                tableIndexColumn.setColumnPosition(0);
                tableIndexColumn.setColumnIsAsc(true);
                tableIndexColumns.add(tableIndexColumn);
            }
            TableIndex tableIndex = new TableIndex();
            tableIndex.setIndexName("unique_index" + builder);
            tableIndex.setIndexType("BTREE");
            tableIndex.setIndexSourceType("BTREE");
            tableIndex.setUnique(true);

            tableIndex.setColumns(tableIndexColumns);
            return tableIndex;
        }

        if (CollectionUtils.isNotEmpty(tableIndices)) {
            return tableIndices.get(0);
        }
        return null;
    }

    @Data
    public static class EmbeddedSetting implements Serializable {
        private String path;
        /** 打开之后作为文档还是数组的单选项 object, array */
        private String format;
    }

    @Data
    public static class JoinExpression implements Serializable {
        /** 表达式  当前只有 eq */
        private String expression;
        /** 左表的字段名 */
        private String left;
        /** 右表的字段名  */
        private String right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof JoinProcessorNode) {
            Class className = JoinProcessorNode.class;
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
        updateDdlList(leftPrimaryKeys, event);
        updateDdlList(rightPrimaryKeys, event);
//        if (event instanceof TapAlterFieldNameEvent) {
//            ValueChange<String> nameChange = ((TapAlterFieldNameEvent) event).getNameChange();
//            String leftChangeField = null;
//            String rightChangeField = null;
//            for (String left : leftPrimaryKeys) {
//                if (left.equals(nameChange.getBefore())) {
//                    leftChangeField = left;
//                }
//            }
//
//            for (String right : rightPrimaryKeys) {
//                if (right.equals(nameChange.getBefore())) {
//                    rightChangeField = right;
//                }
//            }
//            if (leftChangeField != null) {
//                leftPrimaryKeys.remove(leftChangeField);
//                leftPrimaryKeys.add(nameChange.getAfter());
//            }
//
//            if (rightChangeField != null) {
//                rightPrimaryKeys.remove(leftChangeField);
//                rightPrimaryKeys.add(nameChange.getAfter());
//            }
//
//        } else if (event instanceof TapDropFieldEvent) {
//            String fieldName = ((TapDropFieldEvent) event).getFieldName();
//            if (leftPrimaryKeys.contains(fieldName)) {
//                throw new DDLException("Join node: Ddl drop field link left primary fields");
//            }
//
//            if (rightPrimaryKeys.contains(fieldName)) {
//                throw new DDLException("Join node: Ddl drop field link right primary fields");
//            }
//        }
    }

}
