package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:22
 * @description
 */
@NodeType("field_add_del_processor")
@Getter
@Setter
@Slf4j
public class FieldAddDelProcessorNode extends FieldProcessorNode {

    private boolean deleteAllFields;
    private List<Postion> fieldsAfter;

    public FieldAddDelProcessorNode() {
        super("field_add_del_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = superMergeSchema(inputSchemas, schema);

        //批量删除
        if (deleteAllFields) {

            if (CollectionUtils.isNotEmpty(operations)) {
                List<String> rollbackFields = operations.stream().filter(o -> "REMOVE".equals(o.getOp()) && "false".equals(o.getOperand())).map(Operation::getField).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(outputSchema.getFields())) {
                    List<String> fieldNames = outputSchema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
                    for (Field field : outputSchema.getFields()) {
                        if (!rollbackFields.contains(field.getFieldName())) {
                            field.setDeleted(true);
                        } else {
                            field.setDeleted(false);
                        }
                    }
                    //outputSchema.setFields(outputSchema.getFields().stream().filter(f -> rollbackFields.contains(f.getFieldName())).collect(Collectors.toList()));

                    //rollbackFields.removeAll(fieldNames);
                    operations = operations.stream().filter(o -> !"CREATE".equals(o.getOperand()) || rollbackFields.contains(o.getField())).collect(Collectors.toList());
                }
            } else {
                for (Field field : outputSchema.getFields()) {
                    field.setDeleted(true);
                }
            }

        }

        if (operations != null && operations.size() > 0) {
            operations.forEach(operation -> {
                if (operation.getId() == null) {
                    log.warn("Invalid operation in node {}, id can not be empty.", getId());
                    return;
                }
                String operand = operation.getOp();
                String fieldName = operation.getField();
                if ("CREATE".equalsIgnoreCase(operand)) {
                    operation.setType("String");
                    operation.setJava_type("String");
                    Field field = createField(this.getId(), outputSchema.getOriginalName(), operation);
                    //这种创建的字段不能使用手动，不然修改名称后，后面的节点不会覆盖，应该合并原则都是手动的时候,取原有的
                    field.setSource("job_analyze");
                    field.setTapType(FieldModTypeProcessorNode.calTapType(field.getDataType()));
                    outputSchema.getFields().add(field);
                } else if ("REMOVE".equalsIgnoreCase(operand) && !"false".equals(operation.getOperand())) {
                    for (Field field : outputSchema.getFields()) {
                        if (fieldName.equals(field.getFieldName()))  {
                            field.setDeleted(true);
                            break;
                        }
                    }
//                    outputSchema.getFields().removeIf(field ->
//                            operation.getId().equals(field.getId()));
                }
            });
        }

        // check fieldsAfter sort fields
        if (CollectionUtils.isNotEmpty(fieldsAfter)) {
            Map<String, Integer> positionMap = fieldsAfter.stream()
                    .collect(Collectors.toMap(Postion::getField_name, Postion::getColumnPosition, (e1, e2) -> e1));
            outputSchema.getFields().forEach(field -> {
                if (positionMap.containsKey(field.getFieldName())) {
                    field.setColumnPosition(positionMap.get(field.getFieldName()));
                }
            });
            outputSchema.getFields().sort(Comparator.comparing(Field::getColumnPosition));
        }
        return outputSchema;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof FieldAddDelProcessorNode) {
            Class className = FieldAddDelProcessorNode.class;
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
        for (Operation operation : operations) {
            if (operation.getOp().equals("REMOVE") || operation.getOp().equals("CREATE") ) {
                operation.matchPdkFieldEvent(event);
            }
        }
    }

    @Data
    static class Postion implements Serializable {
        private String field_name;
        private Integer columnPosition;
    }
}
