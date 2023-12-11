package com.tapdata.tm.commons.dag.deduction.rule.node;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class InNodeStageTest {

    @Nested
    class StartStageTest {
        List<FieldChangeRule> changeRules;
        Map<String, List<Field>> fieldMap;
        FieldChangeRule changeRule;
        FieldChangeRule.Type type;
        InNodeStage nodeStage;
        @BeforeEach
        void init() {
            nodeStage = mock(InNodeStage.class);
            type = mock(FieldChangeRule.Type.class);
            changeRule = mock(FieldChangeRule.class);
            fieldMap = mock(Map.class);
            changeRules = new ArrayList<>();
            changeRules.add(changeRule);

            when(changeRule.getType()).thenReturn(type);
            when(type.name()).thenReturn("xxx");
            doNothing().when(nodeStage).change(any(FieldChangeRule.class), anyMap());
        }

        @Test
        void testStartStageNormal() {
            assertVerify(changeRules, fieldMap, nodeStage, 1, 1, 1);
        }

        @Test
        void testStartStageChangeRulesIsNull() {
            assertVerify(null, fieldMap, nodeStage, 0, 0, 0);
        }

        @Test
        void testStartStageChangeRuleIsNull() {
            changeRules = new ArrayList<>();
            changeRules.add(null);
            assertVerify(changeRules, fieldMap, nodeStage, 0, 0, 0);
        }

        @Test
        void testStartStageChangeRuleTypeIsNull() {
            when(changeRule.getType()).thenReturn(null);
            assertVerify(changeRules, fieldMap, nodeStage, 1, 0, 0);
        }

        @Test
        void testStartStageContainsKeyButNull() {
            try {
                InNodeStage.stage.put(type, null);
                assertVerify(changeRules, fieldMap, nodeStage, 1, 1, 1);
            } finally {
                InNodeStage.stage.remove(type);
            }
        }

        @Test
        void testStartStageContainsKeyAndNotNull() {
            try {
                InNodeStage.stage.put(type, nodeStage);
                assertVerify(changeRules, fieldMap, nodeStage, 1, 1, 0);
            } finally {
                InNodeStage.stage.remove(type);
            }
        }

        @Test
        void testStartStageNodeStageNull() {
            assertVerify(changeRules, fieldMap, null, 1, 0, 1);

        }

        void assertVerify(List<FieldChangeRule> changeR, Map<String, List<Field>> fieldM,
                          InNodeStage node,
                          int changeRuleTimes, int nodeStageTimes,
                          int createNodeStageTimes) {
            try (MockedStatic<InNodeStage> mockedStatic = mockStatic(InNodeStage.class)) {
                mockedStatic.when(() -> InNodeStage.createNodeStage(anyString())).thenReturn(node);
                mockedStatic.when(() -> InNodeStage.startStage(anyList(), anyMap())).thenCallRealMethod();
                mockedStatic.when(() -> InNodeStage.startStage(null, fieldMap)).thenCallRealMethod();
                InNodeStage.startStage(changeR, fieldM);
                mockedStatic.verify(() -> InNodeStage.createNodeStage(anyString()), times(createNodeStageTimes));
            } finally {
                verify(changeRule, times(changeRuleTimes)).getType();
                verify(nodeStage, times(nodeStageTimes)).change(any(FieldChangeRule.class), anyMap());
            }
        }
    }

    @Nested
    class CreateNodeStageTest {
        @Test
        void testCreateNodeStageNodeOfDataType() {
            InNodeStage stage = assertVerify(FieldChangeRule.Type.DataType.name());
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(NodeOfDataType.class, stage.getClass());
        }
        @Test
        void testCreateNodeStageNodeOfMultiDataType() {
            InNodeStage stage = assertVerify(FieldChangeRule.Type.MutiDataType.name());
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(NodeOfMultiDataType.class, stage.getClass());
        }
        @Test
        void testCreateNodeStageNullType() {
            InNodeStage stage = assertVerify(null);
            Assertions.assertNull(stage);
        }

        @Test
        void testCreateNodeStageOtherType() {
            InNodeStage stage = assertVerify("xxx");
            Assertions.assertNull(stage);
        }


        InNodeStage assertVerify(String type) {
            InNodeStage stage = null;
            try (MockedStatic<InNodeStage> mockedStatic = mockStatic(InNodeStage.class)) {
                mockedStatic.when(() -> InNodeStage.createNodeStage(anyString())).thenCallRealMethod();
                mockedStatic.when(() -> InNodeStage.createNodeStage(null)).thenCallRealMethod();
                stage = InNodeStage.createNodeStage(type);
            }
            return stage;
        }
    }
}