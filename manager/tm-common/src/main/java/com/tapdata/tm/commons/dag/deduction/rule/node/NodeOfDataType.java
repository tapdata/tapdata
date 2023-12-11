package com.tapdata.tm.commons.dag.deduction.rule.node;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.List;
import java.util.Map;


/**
 * @author GavinXiao
 * */
public class NodeOfDataType implements InNodeStage {

    @Override
    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap) {
        if (null == changeRule || null == fieldMap) return;
        Map<String, String> result = changeRule.getResult();
        if (null == result) return;
        String dataTypeTemp = result.get("dataTypeTemp");
        if (null == dataTypeTemp) {
            dataTypeTemp = result.get("accept");
        }
        if (null == dataTypeTemp) return;
        List<Field> fields = fieldMap.get(dataTypeTemp);
        if (null == fields || fields.isEmpty()) return;
        for (Field field : fields) {
            if (null == field) continue;
            field.setSource(Field.SOURCE_MANUAL);
        }
    }
}
