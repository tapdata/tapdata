package com.tapdata.tm.commons.dag.deduction.rule;

import com.tapdata.tm.commons.dag.deduction.rule.node.InNodeStage;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;

import java.util.List;
import java.util.function.Function;


/**
 * @author GavinXiao
 * */
public class NodeStage implements ChangeRuleStage {
    @Override
    public void change(List<FieldChangeRule> changeRules, MetadataInstancesDto metadataInstancesDto) {
        InNodeStage.startStage(changeRules, groupField(metadataInstancesDto));
    }

    @Override
    public Function<Field, String> groupBy() {
        return Field::getDataType;
    }
}