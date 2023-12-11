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


class NodeOfDataTypeTest {
    NodeOfDataType nodeOfDataType;
    @BeforeEach
    void init() {
        nodeOfDataType = mock(NodeOfDataType.class);
    }

    @Nested
    class ChangeTest {
        FieldChangeRule changeRule;
        Map<String, List<Field>> fieldMap;
        Field field;
        List<Field> fields;
        String dataTypeTemp;
        Map<String, String> result;
        @BeforeEach
        void init() {
            changeRule = mock(FieldChangeRule.class);
            fieldMap = mock(HashMap.class);
            result = mock(HashMap.class);
            field = mock(Field.class);
            doNothing().when(field).setSource(Field.SOURCE_MANUAL);

            fields = new ArrayList<>();
            fields.add(field);
            dataTypeTemp = "mock-dataTypeTemp";

            when(result.get("dataTypeTemp")).thenReturn(dataTypeTemp);
            when(result.get("accept")).thenReturn(dataTypeTemp);
            when(changeRule.getResult()).thenReturn(result);
            when(fieldMap.get(dataTypeTemp)).thenReturn(fields);

            doCallRealMethod().when(nodeOfDataType).change(changeRule, fieldMap);
            doCallRealMethod().when(nodeOfDataType).change(null, fieldMap);
            doCallRealMethod().when(nodeOfDataType).change(changeRule, null);
            doCallRealMethod().when(nodeOfDataType).change(null, null);
        }

        @Test
        void testChangeNormal() {
            assertVerify(changeRule, fieldMap, 1, 1, 1, 1);
        }
        @Test
        void testChangeNullChangeRule() {
            assertVerify(null, fieldMap, 0, 0, 0, 0);
        }
        @Test
        void testChangeNullFieldMap() {
            assertVerify(changeRule, null, 0, 0, 0, 0);
        }
        @Test
        void testChangeNullResult() {
            when(changeRule.getResult()).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 0, 0, 0);
        }
        @Test
        void testChangeNullDataTypeTemp() {
            when(result.get("dataTypeTemp")).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 2, 1, 1);
        }
        @Test
        void testChangeNullDataTypeAndDataTypeTemp() {
            when(result.get("dataTypeTemp")).thenReturn(null);
            when(result.get("accept")).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 2, 0, 0);
        }
        @Test
        void testChangeNullFields() {
            when(fieldMap.get(dataTypeTemp)).thenReturn(null);
            assertVerify(changeRule, fieldMap, 1, 1, 1, 0);
        }
        @Test
        void testChangeEmptyFields() {
            when(fieldMap.get(dataTypeTemp)).thenReturn(new ArrayList<>());
            assertVerify(changeRule, fieldMap, 1, 1, 1, 0);
        }
        @Test
        void testChangeOnlyOneNullField() {
            fields = new ArrayList<>();
            fields.add(null);
            when(fieldMap.get(dataTypeTemp)).thenReturn(fields);
            assertVerify(changeRule, fieldMap, 1, 1, 1, 0);
        }
        @Test
        void testChangeWithAnNullField() {
            fields.add(null);
            when(fieldMap.get(dataTypeTemp)).thenReturn(fields);
            assertVerify(changeRule, fieldMap, 1, 1, 1, 1);
        }

        void assertVerify(FieldChangeRule tempChangeRule, Map<String, List<Field>> tempFieldMap,
                          int getResultTimes, int resultGetTimes, int fieldMapGetTimes, int setCreateSourceTimes) {
            nodeOfDataType.change(tempChangeRule, tempFieldMap);
            verify(changeRule, times(getResultTimes)).getResult();
            verify(result, times(resultGetTimes)).get(anyString());
            verify(fieldMap, times(fieldMapGetTimes)).get(dataTypeTemp);
            verify(field, times(setCreateSourceTimes)).setSource(Field.SOURCE_MANUAL);
        }
    }
}