package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("custom_processor")
@Getter
@Setter
public class CustomProcessorNode extends ProcessorNode {
    @EqField
    private String customNodeId;

    private Map<String, Object> form;


    @Override
    protected Schema loadSchema(List<String> includes) {
        //用于预跑数据得到模型
        TapTable tapTable =  service.loadTapTable(getInputSchema(), null, getId(), null, customNodeId, null, null);
        Schema schema = PdkSchemaConvert.fromPdkSchema(tapTable);
        return schema;
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        //js节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        if (schema != null) {
            return schema;
        }

        return SchemaUtils.mergeSchema(inputSchemas, null);
    }

     CustomProcessorNode() {
        super("custom_processor");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof CustomProcessorNode) {
            Class className = CustomProcessorNode.class;
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
