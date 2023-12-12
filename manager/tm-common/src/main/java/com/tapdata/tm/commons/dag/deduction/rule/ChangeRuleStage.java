package com.tapdata.tm.commons.dag.deduction.rule;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * */
public interface ChangeRuleStage {
    Map<FieldChangeRule.Scope, ChangeRuleStage> stages = new HashMap<FieldChangeRule.Scope, ChangeRuleStage>(){{
        put(FieldChangeRule.Scope.Node, new NodeStage());
        put(FieldChangeRule.Scope.Field, new FieldStage());
        put(FieldChangeRule.Scope.Table, new TableStage());
    }};

    static void changeStart(MetadataInstancesDto metadataInstancesDto, DAG.Options options) {
        if (null == metadataInstancesDto || null == options || !options.isIsomorphismTask()) return;
        FieldChangeRuleGroup fieldChangeRules = options.getFieldChangeRules();
        if (null == fieldChangeRules) {
            return;
        }
        Map<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> rules = fieldChangeRules.getRules();
        //同构任务只能有一个源和一个目标，FieldChangeRuleGroup里面最多只有一个nodeId的配置
        if (null == rules || rules.size() != 1) {
            return;
        }
        for (Map.Entry<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> entry : rules.entrySet()) {
            Map<FieldChangeRule.Scope, List<FieldChangeRule>> scopeListMap = entry.getValue();
            Set<FieldChangeRule.Scope> scopes = scopeListMap.keySet();
            for (FieldChangeRule.Scope scope : scopes) {
                if (null == scope) continue;
                ChangeRuleStage changeRuleStage;
                if (!stages.containsKey(scope) || null == stages.get(scope)) {
                    changeRuleStage = create(scope);
                    if (null == changeRuleStage) {
                        continue;
                    }
                    stages.put(scope, changeRuleStage);
                } else {
                    changeRuleStage = stages.get(scope);
                }
                changeRuleStage.change(scopeListMap.get(scope), metadataInstancesDto);
            }
        }
    }

    static ChangeRuleStage create(FieldChangeRule.Scope scope) {
        if (null == scope) return null;
        switch (scope) {
            case Node: return new NodeStage();
            case Field: return new FieldStage();
            default: return new TableStage();
        }
    }

    default Map<String, List<Field>> groupField(MetadataInstancesDto metadataInstancesDto) {
        return metadataInstancesDto.getFields().stream().filter(Objects::nonNull).collect(Collectors.groupingBy(groupBy()));
    }

    void change(List<FieldChangeRule> scopeList, MetadataInstancesDto metadataInstancesDto);

    Function<Field, String> groupBy();
}