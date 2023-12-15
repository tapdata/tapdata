package com.tapdata.tm.commons.dag.deduction.rule.node;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author GavinXiao
 * */
public interface InNodeStage {
    Map<FieldChangeRule.Type, InNodeStage> stage = new HashMap<>();

    public static void startStage(List<FieldChangeRule> changeRules, Map<String, List<Field>> fieldMap) {
        if (null == changeRules) return;
        for (FieldChangeRule changeRule : changeRules) {
            if (null == changeRule) continue;
            FieldChangeRule.Type type = changeRule.getType();
            if (null == type) continue;
            InNodeStage nodeStage = null;
            if (!stage.containsKey(type) || null == stage.get(type)) {
                nodeStage = createNodeStage(type.name());
                if (null == nodeStage) continue;
                stage.put(type, nodeStage);
            } else {
                nodeStage = stage.get(type);
            }
            nodeStage.change(changeRule, fieldMap);
        }
    }

    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap);

    public static InNodeStage createNodeStage(String type){
        if (null == type) return null;
        switch (type) {
            case "DataType": return new NodeOfDataType();
            case "MutiDataType":return new NodeOfMultiDataType();
        }
        return null;
    }
}
