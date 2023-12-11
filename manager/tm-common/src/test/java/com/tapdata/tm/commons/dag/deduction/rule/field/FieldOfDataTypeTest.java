package com.tapdata.tm.commons.dag.deduction.rule.field;

import com.tapdata.tm.commons.dag.deduction.rule.ChangeRuleStage;
import com.tapdata.tm.commons.dag.deduction.rule.FieldStage;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FieldOfDataTypeTest {
    FieldOfDataType stage;
    @BeforeEach
    void init() {
        stage = mock(FieldOfDataType.class);
    }

    @Nested
    class ChangeTest {
        FieldChangeRule changeRule;
        Map<String, List<Field>> fieldMap;
        String accept;
        String fieldName;
        List<Field> fields;
        Field field;
        String dataType;
        @BeforeEach
        void init() {
            accept = "accept";
            fieldName = "fieldName";
            dataType = "accept";

            changeRule = mock(FieldChangeRule.class);
            fieldMap = mock(Map.class);

            field = mock(Field.class);
            fields = new ArrayList<>();
            fields.add(field);

            when(changeRule.getAccept()).thenReturn(accept);
            when(changeRule.getFieldName()).thenReturn(fieldName);
            when(fieldMap.get(anyString())).thenReturn(fields);
            when(field.getDataType()).thenReturn(dataType);
            doNothing().when(field).setSource(Field.SOURCE_MANUAL);

            doCallRealMethod().when(stage).change(any(FieldChangeRule.class), anyMap());
            doCallRealMethod().when(stage).change(changeRule, fieldMap);
            doCallRealMethod().when(stage).change(null, fieldMap);
            doCallRealMethod().when(stage).change(changeRule, null);
            doCallRealMethod().when(stage).change(null, null);
        }
        void assertVerify(FieldChangeRule ruleItem, Map<String, List<Field>> fieldMapTemp,
                          int getAcceptTimes, int getFieldNameTimes, int fieldMapGetTimes, int getDataTypeTimes, int setCreateSourceTimes) {
            stage.change(ruleItem, fieldMapTemp);
            verify(changeRule, times(getAcceptTimes)).getAccept();
            verify(changeRule, times(getFieldNameTimes)).getFieldName();
            verify(fieldMap, times(fieldMapGetTimes)).get(anyString());
            verify(field, times(getDataTypeTimes)).getDataType();
            verify(field, times(setCreateSourceTimes)).setSource(Field.SOURCE_MANUAL);
        }
        @Test
        void testChangeNormal() {
            assertVerify(changeRule, fieldMap, 1, 1, 1, 1, 1);
        }
        @Test
        void testNullChangeRules() {
            assertVerify(null, fieldMap, 0, 0, 0, 0, 0);
        }
        @Test
        void nullFieldMap() {
            assertVerify(changeRule, null, 0, 0, 0, 0, 0);
        }
        @Test
        void nullAccept() {
            accept = null;
            when(changeRule.getAccept()).thenReturn(accept);
            assertVerify(changeRule, fieldMap, 1, 0, 0, 0, 0);
        }
        @Test
        void nullFields() {
            when(fieldMap.get(anyString())).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 1, 1, 0, 0);
        }
        @Test
        void acceptNotEqualsFieldsDataType() {
            dataType = "dataType";
            when(field.getDataType()).thenReturn(dataType);
            assertVerify(changeRule, fieldMap, 1, 1, 1, 1, 0);
        }
    }
}
