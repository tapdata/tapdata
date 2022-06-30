package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:22
 * @description
 */
@NodeType("field_rename_processor")
@Getter
@Setter
@Slf4j
public class FieldRenameProcessorNode extends FieldProcessorNode {

    private String fieldsNameTransform = "";

    public FieldRenameProcessorNode() {
        super("field_rename_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        Schema outputSchema = superMergeSchema(inputSchemas, schema);

        List<String> inputFields = inputSchemas.stream().map(Schema::getFields).flatMap(Collection::stream).map(Field::getFieldName).collect(Collectors.toList());
        fieldNameReduction(inputFields, outputSchema.getFields(), fieldsNameTransform);
        fieldNameUpLow(inputFields, outputSchema.getFields(), fieldsNameTransform);
        if (operations != null && operations.size() > 0) {
            operations.forEach(operation -> {
                if (operation == null) {
                    return;
                }
                if (operation.getId() == null) {
                    log.warn("Invalid operation in node {}, id can not be empty.", getId());
                    return;
                }

                String operand = operation.getOp();
                if (StringUtils.isBlank(operand) || "RENAME".equalsIgnoreCase(operand)) {
                    outputSchema.getFields().forEach(field -> {
                        if (operation.getId().equals(field.getId())) {
                            field.setFieldName(operation.getOperand());
                            //field.setOriginalFieldName(operation.getOperand());
                            List<String> oldIdList = field.getOldIdList();
                            if (oldIdList == null) {
                                oldIdList = new ArrayList<>();
                                field.setOldIdList(oldIdList);
                            }
                            oldIdList.add(field.getId());
                            field.setId(new ObjectId().toHexString());
                        }
                    });
                }

            });
        }
        return outputSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof FieldRenameProcessorNode) {
            Class className = FieldRenameProcessorNode.class;
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
}
