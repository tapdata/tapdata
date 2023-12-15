package com.tapdata.tm.commons.dag.deduction.rule;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;

import java.util.List;
import java.util.function.Function;

/**
 * @author GavinXiao
 * */
public class TableStage implements ChangeRuleStage {

    @Override
    public void change(List<FieldChangeRule> scopeList, MetadataInstancesDto metadataInstancesDto) {
        //Nothing need to do now
    }

    @Override
    public Function<Field, String> groupBy() {
        return Field::getFieldName;
    }
}