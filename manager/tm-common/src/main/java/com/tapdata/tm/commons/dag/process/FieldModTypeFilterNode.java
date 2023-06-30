package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.RemoveBracketsUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;


@NodeType("field_mod_type_filter_processor")
@Getter
@Setter
@Slf4j
public class FieldModTypeFilterNode extends ProcessorNode{
    /**
     * 创建处理器节点
     *
     **/
    public FieldModTypeFilterNode() {
        super("field_mod_type_filter_processor");
    }

    private List<String> filterTypes;

    private  Map<String, Map<String, FieldInfo>> fieldTypeFilterMap = new HashMap<>();


    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Schema outputSchema = super.mergeSchema(inputSchemas, schema, options);
            List<Field> fields = outputSchema.getFields();
            String ancestorsName = outputSchema.getName();
            Map<String, FieldInfo> filterFields = new HashMap<>();
            for (Field field : fields) {
                Boolean show = filterTypes.contains(RemoveBracketsUtil.removeBrackets(field.getDataType()));
                if (Objects.nonNull(show) && show) {
                    field.setDeleted(true);
                    FieldInfo fieldInfo = new FieldInfo(field.getFieldName(),null,false,field.getDataType());
                    filterFields.put(field.getFieldName(),fieldInfo);
                }
            }
        fieldTypeFilterMap.put(ancestorsName,filterFields);
        return outputSchema;
    }



}
