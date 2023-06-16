package com.tapdata.tm.commons.dag.vo;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapStringMapping;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.commons.collections4.CollectionUtils;

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

    private Map<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> rules = new HashMap<>();

    public void add(String nodeId, FieldChangeRule rule) {
        rules.computeIfAbsent(nodeId, k -> new HashMap<>())
                .computeIfAbsent(rule.getScope(), k -> new ArrayList<>()).add(rule);
    }

    public void addAll(String nodeId, List<FieldChangeRule> rules) {
        for (FieldChangeRule r : rules) add(nodeId, r);
    }

    public FieldChangeRule getRule(String nodeId, String qualifiedName, String fieldName, String dataType, TypeExprResult<DataMap> exprResult) {
        Map<FieldChangeRule.Scope, List<FieldChangeRule>> scopeListMap = rules.get(nodeId);

        if (scopeListMap == null) {
            return null;
        }

        List<FieldChangeRule> fieldChangeRules = scopeListMap.get(FieldChangeRule.Scope.Field);
        if (CollectionUtils.isNotEmpty(fieldChangeRules)) {
            fieldChangeRules = new ArrayList<>(fieldChangeRules);
            Collections.reverse(fieldChangeRules);
            for (FieldChangeRule fieldChangeRule : fieldChangeRules) {
                if (fieldName.equals(fieldChangeRule.getFieldName()) && qualifiedName.equals(fieldChangeRule.getQualifiedName())) {
                    return fieldChangeRule;
                }
            }
        }

        List<FieldChangeRule> nodeChangeRules = scopeListMap.get(FieldChangeRule.Scope.Node);
        if (CollectionUtils.isNotEmpty(nodeChangeRules)) {
            nodeChangeRules = new ArrayList<>(nodeChangeRules);
            Collections.reverse(nodeChangeRules);
            for (FieldChangeRule fieldChangeRule : nodeChangeRules) {
                if (fieldChangeRule.getType().equals(FieldChangeRule.Type.MutiDataType)) {
                    if (exprResult != null && exprResult.getParams() != null
                            && exprResult.getValue().get("_tapMapping") != null && exprResult.getValue().get("_tapMapping") instanceof TapStringMapping) {
                        return fieldChangeRule;
                    }
                }

                if (fieldChangeRule.getAccept().equals(dataType)) {
                    return fieldChangeRule;
                }
            }
        }

//        return Optional.ofNullable(rules.get(nodeId)).map(nodeRules -> Optional.ofNullable(nodeRules.get(FieldChangeRule.Scope.Field)).map(ruleSet -> {
//            for (FieldChangeRule r : ruleSet) {
//                if (fieldName.equals(r.getFieldName()) && qualifiedName.equals(r.getQualifiedName())) return r;
//            }
//            return null;
//        }).orElse(Optional.ofNullable(nodeRules.get(FieldChangeRule.Scope.Node)).map(ruleSet -> {
//            for (FieldChangeRule r : ruleSet) {
//                if (r.getType().equals(FieldChangeRule.Type.MutiDataType)) {
//                    return r;
//                }
//                if (r.getAccept().equals(dataType)) return r;
//            }
//            return null;
//        }).orElse(null))).orElse(null);
        return null;
    }

    public void process(String nodeId, String qualifiedName, Field f, DefaultExpressionMatchingMap map) {
        TypeExprResult<DataMap> exprResult = map.get(f.getDataType());
        FieldChangeRule rule = getRule(nodeId, qualifiedName, f.getFieldName(), f.getDataType(), exprResult);
        if (null == rule) return;

        switch (rule.getType()) {
            case DataType:
                Map<String, String> result = rule.getResult();
                f.setDataType(result.get("dataType"));
                f.setTapType(result.get("tapType"));
                f.setChangeRuleId(rule.getId());
                break;
            case MutiDataType:
                Double multiple = rule.getMultiple();
                if (multiple != 0 && multiple > 1) {
                    if (exprResult.getParams() != null && !exprResult.getParams().isEmpty()) {
                        TapType tapType = PdkSchemaConvert.getJsonParser().fromJson(f.getTapType(), TapType.class);
                        if (tapType instanceof TapString) {
                            String byteObj = exprResult.getParams().get("byte");
                            if(byteObj != null) {
                                CommonUtils.ignoreAnyError(() -> {
                                    Long bytes = Long.parseLong(byteObj);
                                    TapString newTypeTable = ((TapString) tapType).bytes((long)(bytes * multiple));
                                    TapStringMapping tapMapping = (TapStringMapping) exprResult.getValue().get("_tapMapping");
                                    TapResult<String> stringTapResult = tapMapping.fromTapType(exprResult.getExpression(), tapType);
                                    String newDataType = stringTapResult.getData();
                                    f.setDataType(newDataType);
                                    f.setTapType(PdkSchemaConvert.getJsonParser().toJson(newTypeTable));
                                    f.setChangeRuleId(rule.getId());
                                }, "XXX");

                            }
                        }

                    }
                }
            default:
                break;
        }
    }
}
