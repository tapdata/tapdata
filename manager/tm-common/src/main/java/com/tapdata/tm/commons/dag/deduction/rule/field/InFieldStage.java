package com.tapdata.tm.commons.dag.deduction.rule.field;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface InFieldStage {
    Map<FieldChangeRule.Type, InFieldStage> stage = new HashMap<>();

    public static void startStage(List<FieldChangeRule> changeRules, Map<String, List<Field>> fieldMap) {
        if (null == changeRules) return;
        for (FieldChangeRule changeRule : changeRules) {
            if (null == changeRule) continue;
            FieldChangeRule.Type type = changeRule.getType();
            if (null == type) continue;
            InFieldStage fieldStage = null;
            if (!stage.containsKey(type) || null == stage.get(type)) {
                fieldStage = createNodeStage(type.name());
                if (null == fieldStage) continue;
                stage.put(type, fieldStage);
            } else {
                fieldStage = stage.get(type);
            }
            fieldStage.change(changeRule, fieldMap);
        }
    }

    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap);

    public static InFieldStage createNodeStage(String type){
        if (null == type) return null;
        switch (type) {
            case "DataType": return new FieldOfDataType();
            case "MutiDataType":return new FieldOfMultiDataType();
        }
        return null;
    }
}
