package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.exception.DDLException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.List;

import static com.tapdata.tm.commons.schema.SchemaUtils.createField;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:22
 * @description
 */
@NodeType("field_processor")
@Getter
@Setter
@Slf4j
public class FieldProcessorNode extends ProcessorNode {

    @EqField
    protected List<Operation> operations;
    @EqField
    protected List<Script> scripts;


    public FieldProcessorNode() {
        super("field_processor");
    }

    public FieldProcessorNode(String type) {
        super(type);
    }

    protected Schema superMergeSchema(List<Schema> inputSchemas, Schema schema) {
        return super.mergeSchema(inputSchemas, schema, null);
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = super.mergeSchema(inputSchemas, schema, options);

        if (operations != null && operations.size() > 0) {
            operations.forEach(operation -> {
                if (operation.getId() == null) {
                    log.warn("Invalid operation in node {}, id can not be empty.", getId());
                    return;
                }
                String operand = operation.getOp();
                if ("CREATE".equalsIgnoreCase(operand)) {
                    outputSchema.getFields().add(createField(operation));
                } else if ("REMOVE".equalsIgnoreCase(operand)) {
                    outputSchema.getFields().removeIf(field ->
                            operation.getId().equals(field.getId()));
                } else if ("RENAME".equalsIgnoreCase(operand)) {
                    for (Field field : outputSchema.getFields()) {
                        if (operation.getId().equals(field.getId())) {
                            field.setOriginalFieldName(operation.getOperand());
                            field.setFieldName(operation.getOperand());
                            break;
                        }
                    }
                } else if ("CONVERT".equalsIgnoreCase(operand)) {
                    outputSchema.getFields().forEach(field -> {
                        if (operation.getId().equals(field.getId())) {
                            field.setDataType(operation.getType());
                        }
                    });
                }
            });
        }
        return outputSchema;
    }

    @Data
    public static class Operation implements Serializable {
        private String color;
        @EqField
        private String field;
        @EqField
        private String id;
        private String label;
        /** CREATE,CONVERT,REMOVE,RENAME */
        @EqField
        private String op;
        /** op === 'CONVERT' , operand 记录转换后的类型
         op === 'REMOVE',  不需要其他属性配置
         op === 'RENAME',  operand 记录新字段名
         op === 'CREATE', */
        @EqField
        private String operand;
        private Integer primary_key_position;
        @EqField
        private String tableName;
        @EqField
        private String type;
        @EqField
        private String originalDataType;
        @EqField
        private String original_field_name;
        private String java_type;


        public void matchPdkFieldEvent(TapDDLEvent event) throws Exception{
            if (StringUtils.isNotBlank((tableName))) {
                if (!tableName.equals(event.getTableId())) {
                    return;
                }
            }
            if (event instanceof TapNewFieldEvent) {
                List<TapField> newFields = ((TapNewFieldEvent) event).getNewFields();
                if (op.equals("CREATE")) {
                    for (TapField newField : newFields) {
                        if (newField.getName().equals(field)) {
                            throw new DDLException("ddl add field name repeat");
                        }
                    }

                } else if (op.equals("RENAME")) {
                    for (TapField newField : newFields) {
                        if (newField.getName().equals(operand)) {
                            throw new DDLException("ddl add field name repeat");
                        }
                    }
                }

            } else if (event instanceof TapDropFieldEvent) {
                String fieldName = ((TapDropFieldEvent) event).getFieldName();
                if (op.equals("REMOVE")) {
                    if (fieldName.equals(field)) {
                        //这个地方不用修改，会导致不能回撤
                        throw new DDLException("ddl drop field with drop op field");
                    }
                } else if (op.equals("RENAME")) {
                    if (fieldName.equals(field)) {
                        throw new DDLException("ddl drop field with drop op field");
                    }

                }

            } else if (event instanceof TapAlterFieldNameEvent) {
                ValueChange<String> nameChange = ((TapAlterFieldNameEvent) event).getNameChange();
                if (op.equals("REMOVE") || op.equals("RENAME")) {
                    if (nameChange.getBefore().equals(field)) {
                        field = nameChange.getAfter();
                    }
                }
            }
        }



        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof Operation) {
                java.lang.reflect.Field[] declaredFields = Operation.class.getDeclaredFields();
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
                            e.printStackTrace();
                        }
                    }
                }
                return true;
            }
            return false;
        }
    }

    @Data
    public static class Script implements Serializable {
        private String color;
        @EqField
        private String field;
        @EqField
        private String id;
        private String label;
        private Integer primary_key_position;
        @EqField
        private String script;
        @EqField
        private String scriptType = "js";
        @EqField
        private String tableName;
        @EqField
        private String type;
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o instanceof Script) {
                java.lang.reflect.Field[] declaredFields = Script.class.getDeclaredFields();
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
                            e.printStackTrace();
                        }
                    }
                }
                return true;
            }
            return false;
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof FieldProcessorNode) {
            Class className = FieldProcessorNode.class;
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

    }
}
