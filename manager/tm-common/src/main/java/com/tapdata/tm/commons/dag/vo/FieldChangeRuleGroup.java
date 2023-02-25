package com.tapdata.tm.commons.dag.vo;

import com.tapdata.tm.commons.schema.Field;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * 字段变更规则分组
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/11/24 11:41 Create
 */
@Data
@NoArgsConstructor
public class FieldChangeRuleGroup {

    private Map<String, Map<FieldChangeRule.Scope, Collection<FieldChangeRule>>> rules = new HashMap<>();

    public void add(String nodeId, FieldChangeRule rule) {
        rules.computeIfAbsent(nodeId, k -> new HashMap<>())
                .computeIfAbsent(rule.getScope(), k -> new ArrayList<>()).add(rule);
    }

    public void addAll(String nodeId, Collection<FieldChangeRule> rules) {
        for (FieldChangeRule r : rules) add(nodeId, r);
    }

    public FieldChangeRule getRule(String nodeId, String qualifiedName, String fieldName, String dataType) {
        return Optional.ofNullable(rules.get(nodeId)).map(nodeRules -> Optional.ofNullable(nodeRules.get(FieldChangeRule.Scope.Field)).map(ruleSet -> {
            for (FieldChangeRule r : ruleSet) {
                if (fieldName.equals(r.getFieldName()) && qualifiedName.equals(r.getQualifiedName())) return r;
            }
            return null;
        }).orElse(Optional.ofNullable(nodeRules.get(FieldChangeRule.Scope.Node)).map(ruleSet -> {
            for (FieldChangeRule r : ruleSet) {
                if (r.getAccept().equals(dataType)) return r;
            }
            return null;
        }).orElse(null))).orElse(null);
    }

    public void process(String nodeId, String qualifiedName, Field f) {
        FieldChangeRule rule = getRule(nodeId, qualifiedName, f.getFieldName(), f.getDataType());
        if (null == rule) return;

        switch (rule.getType()) {
            case DataType:
                Map<String, String> result = rule.getResult();
                f.setDataType(result.get("dataType"));
                f.setTapType(result.get("tapType"));
                f.setChangeRuleId(rule.getId());
                break;
            default:
                break;
        }
    }
}
