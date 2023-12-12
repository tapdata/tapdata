package com.tapdata.tm.commons.dag.deduction.rule.field;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;

import java.util.List;
import java.util.Map;

public class FieldOfMultiDataType implements InFieldStage {
    @Override
    public void change(FieldChangeRule changeRule, Map<String, List<Field>> fieldMap) {
        //Nothing need to do now
    }
}
