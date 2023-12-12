package com.tapdata.tm.commons.dag.deduction.rule.node;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NodeOfMultiDataTypeTest {
    NodeOfMultiDataType multiDataType;
    @BeforeEach
    void init() {
        multiDataType = mock(NodeOfMultiDataType.class);
    }

    @Nested
    class ChangeTest {
        FieldChangeRule changeRule;
        Map<String, List<Field>> fieldMap;
        Field field;
        List<Field> fields;
        String accept;
        @BeforeEach
        void init() {
            changeRule = mock(FieldChangeRule.class);
            fieldMap = mock(HashMap.class);
            accept = "accept";
            field = mock(Field.class);
            doNothing().when(field).setSource(Field.SOURCE_MANUAL);

            fields = new ArrayList<>();
            fields.add(field);

            when(changeRule.getAccept()).thenReturn(accept);
            when(fieldMap.get(anyString())).thenReturn(fields);

            doCallRealMethod().when(multiDataType).change(changeRule, fieldMap);
            doCallRealMethod().when(multiDataType).change(null, fieldMap);
            doCallRealMethod().when(multiDataType).change(changeRule, null);
            doCallRealMethod().when(multiDataType).change(null, null);
        }

        @Test
        void testChangeNormal() {
            assertVerify(changeRule, fieldMap, 1, 1, 1);
        }
        @Test
        void testChangeNullChangeRule() {
            assertVerify(null, fieldMap, 0, 0, 0);
        }
        @Test
        void testChangeNullFieldMap() {
            assertVerify(changeRule, null, 0, 0, 0);
        }
        @Test
        void testChangeNullAccept() {
            when(changeRule.getAccept()).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 0, 0);
        }
        @Test
        void testChangeNullFields() {
            when(fieldMap.get(anyString())).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 1, 0);
        }
        @Test
        void testChangeEmptyFields() {
            when(fieldMap.get(anyString())).thenReturn(new ArrayList<>());
            assertVerify(changeRule, fieldMap, 1, 1, 0);
        }
        @Test
        void testChangeOnlyOneNullField() {
            fields = new ArrayList<>();
            fields.add(null);
            when(fieldMap.get(anyString())).thenReturn(fields);
            assertVerify(changeRule, fieldMap, 1, 1, 0);
        }
        @Test
        void testChangeWithAnNullField() {
            fields.add(null);
            when(fieldMap.get(anyString())).thenReturn(fields);
            assertVerify(changeRule, fieldMap, 1, 1, 1);
        }

        void assertVerify(FieldChangeRule tempChangeRule, Map<String, List<Field>> tempFieldMap,
                          int getAcceptTimes, int fieldMapGetTimes, int setCreateSourceTimes) {
            multiDataType.change(tempChangeRule, tempFieldMap);
            verify(changeRule, times(getAcceptTimes)).getAccept();
            verify(fieldMap, times(fieldMapGetTimes)).get(anyString());
            verify(field, times(setCreateSourceTimes)).setSource(Field.SOURCE_MANUAL);
        }
    }
}