package com.tapdata.tm.commons.dag.deduction.rule.field;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.List;
import java.util.Map;

public class FieldOfDataType implements InFieldStage {

    @Override
    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap) {
        if (null == changeRule || null == fieldMap) return;
        String accept = changeRule.getAccept();
        if (null == accept) return;
        String fieldName = changeRule.getFieldName();
        List<Field> fields = fieldMap.get(fieldName);
        if (null == fields) return;
        for (Field field : fields) {
            if (accept.equals(field.getDataType())) {
                field.setSource(Field.SOURCE_MANUAL);
            }
        }
    }
}
