package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.task.dto.Aggregation;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:37
 * @description
 */
@NodeType("aggregation_processor")
@Getter
@Setter
public class AggregationProcessorNode extends ProcessorNode {

    @EqField
    private List<String> primaryKeys;
    @EqField
    private List<Aggregation> aggregations;

    public AggregationProcessorNode() {
        super("aggregation_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return super.mergeSchema(inputSchemas, schema);
        }

        Schema inputSchema = inputSchemas.get(0);
        if (inputSchema == null || CollectionUtils.isEmpty(inputSchema.getFields())) {
            return super.mergeSchema(inputSchemas, schema);
        }
        Set<Field> fields = new HashSet<>();
        List<Field> inputFields = inputSchema.getFields();
        Map<String, Field> inputFieldMap = inputFields.stream().collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
        Field field = inputFields.get(0);

        List<TableIndex> tableIndices = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(aggregations)) {
            for (Aggregation aggregation : aggregations) {
                List<String> groupByExpressions = aggregation.getGroupByExpression();
                for (String groupByExpression : groupByExpressions) {
                    String parentName = groupByExpression;
                    while (parentName.contains(".")) {
                        int indexOf = parentName.lastIndexOf(".");
                        String parent = parentName.substring(0, indexOf);
                        Field field2 = inputFieldMap.get(parent);
                        if (field2 != null) {
                            field2.setFieldName(field2.getFieldName());
                            field2.setUnique(true);
                            fields.add(field2);
                        }
                        parentName = parent;
                    }
                    Field field2 = inputFieldMap.get(groupByExpression);
                    if (field2 != null) {
                        field2.setFieldName(field2.getFieldName());
                        field2.setUnique(true);
                        fields.add(field2);
                    }
                }

                TableIndex tableIndex = new TableIndex();

                StringBuilder sb = new StringBuilder("index");
                List<TableIndexColumn> tableIndexColumns = new ArrayList<>();
                for (String groupByExpression : groupByExpressions) {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName(groupByExpression);
                    tableIndexColumns.add(tableIndexColumn);
                    sb.append("_").append(groupByExpression);
                }
                tableIndex.setIndexName(sb.toString());
                tableIndex.setUnique(true);
                tableIndex.setColumns(tableIndexColumns);

                tableIndices.add(tableIndex);

                String aggFunction = aggregation.getAggFunction();
                Field functionFiled = createField(aggFunction, field.getTableName(), "String");
                functionFiled.setTapType(FieldModTypeProcessorNode.calTapType(functionFiled.getDataType()));
                fields.add(functionFiled);
            }
        }

        inputSchema.setFields(new ArrayList<>(fields));
        inputSchema.setIndices(tableIndices);
        return inputSchema;
    }
    //@Override
    public Schema mergeSchema1(List<Schema> inputSchemas, Schema schema) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return super.mergeSchema(inputSchemas, schema);
        }

        Schema inputSchema = inputSchemas.get(0);
        if (inputSchema == null || CollectionUtils.isEmpty(inputSchema.getFields())) {
            return super.mergeSchema(inputSchemas, schema);
        }
        Set<Field> fields = new HashSet<>();
        List<Field> inputFields = inputSchema.getFields();
        Map<String, Field> inputFieldMap = inputFields.stream().collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
        Field field = inputFields.get(0);
        Field field1 = createField("_id", field.getTableName(), "Map");
        fields.add(field1);
        if (CollectionUtils.isNotEmpty(aggregations)) {
            Aggregation aggregation = aggregations.get(aggregations.size() - 1);
            List<String> groupByExpressions = aggregation.getGroupByExpression();
            for (String groupByExpression : groupByExpressions) {
                String parentName = groupByExpression;
                while (parentName.contains(".")) {
                    int indexOf = parentName.lastIndexOf(".");
                    String parent = parentName.substring(0, indexOf);
                    Field field2 = inputFieldMap.get(parent);
                    if (field2 != null) {
                        field2.setFieldName("_id." + field2.getFieldName());
                        fields.add(field2);
                    }
                    parentName = parent;
                }
                Field field2 = inputFieldMap.get(groupByExpression);
                if (field2 != null) {
                    field2.setFieldName("_id." + field2.getFieldName());
                    fields.add(field2);
                }
            }

            String aggFunction = aggregation.getAggFunction();
            Field functionFiled = createField(aggFunction, field.getTableName(), "String");
            fields.add(functionFiled);
        }


        inputSchema.setFields(new ArrayList<>(fields));
        inputSchema.setIndices(null);
        return inputSchema;
    }


    public Field createField(String name, String tableName, String dbType) {
        Field field = new Field();
        field.setFieldName(name);
        field.setSource("job_analyze");
        field.setDataType(dbType);
        if (dbType.equals("String")) {
            field.setColumnSize(100);
            field.setOriPrecision(100);
        }
        field.setId(new ObjectId().toHexString());
        field.setTableName(tableName);
        field.setOriginalFieldName(name);
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof AggregationProcessorNode) {
            Class className = AggregationProcessorNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = Node.fieldEq(f1, f2);
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
        updateDdlList(primaryKeys, event);
    }
}
