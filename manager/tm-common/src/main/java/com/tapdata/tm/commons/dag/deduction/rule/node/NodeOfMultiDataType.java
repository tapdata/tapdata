package com.tapdata.tm.commons.dag.deduction.rule.node;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.List;
import java.util.Map;


/**
 * @author GavinXiao
 * */
public class NodeOfMultiDataType implements InNodeStage {
    @Override
    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap) {
        if (null == changeRule || null == fieldMap) return;
        String accept = changeRule.getAccept();
        if (null == accept) return;
        List<Field> fields = fieldMap.get(accept);
        if (null == fields) return;
        for (Field field : fields) {
            if (null == field) continue;
            field.setSource(Field.SOURCE_MANUAL);
        }
    }
}
