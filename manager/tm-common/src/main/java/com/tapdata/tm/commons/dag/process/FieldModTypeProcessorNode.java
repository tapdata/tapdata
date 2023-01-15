package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:22
 * @description
 */
@NodeType("field_mod_type_processor")
@Getter
@Setter
@Slf4j
public class FieldModTypeProcessorNode extends FieldProcessorNode {

    public FieldModTypeProcessorNode() {
        super("field_mod_type_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = superMergeSchema(inputSchemas, schema);

        if (operations != null && operations.size() > 0) {
            operations.forEach(operation -> {
                if (operation.getId() == null) {
                    log.warn("Invalid operation in node {}, id can not be empty.", getId());
                    return;
                }
                String operand = operation.getOp();
                if (StringUtils.isBlank(operand) || "CONVERT".equalsIgnoreCase(operand)) {
                    outputSchema.getFields().forEach(field -> {
                        if (operation.getId().equals(field.getId())) {
                            field.setTapType(calTapType(operation.getOperand()));
                            field.setDataTypeTemp(null);
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

        if (o instanceof FieldModTypeProcessorNode) {
            Class className = FieldModTypeProcessorNode.class;
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


    public static String calTapType(String javaType) {
        TapType tapType = JavaTypesToTapTypes.toTapType(javaType);
        if (tapType != null) {
            return JsonUtil.toJsonUseJackson(tapType);
        } else {
            return null;
        }
    }
}
