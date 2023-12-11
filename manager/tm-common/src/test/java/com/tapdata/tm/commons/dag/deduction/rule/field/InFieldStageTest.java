package com.tapdata.tm.commons.dag.deduction.rule.field;

import com.tapdata.tm.commons.dag.deduction.rule.node.NodeOfDataType;
import com.tapdata.tm.commons.dag.deduction.rule.node.NodeOfMultiDataType;
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

public class InFieldStageTest {
    @Nested
    class StartStageTest {
        List<FieldChangeRule> changeRules;
        Map<String, List<Field>> fieldMap;
        FieldChangeRule changeRule;
        FieldChangeRule.Type type;
        InFieldStage nodeStage;
        @BeforeEach
        void init() {
            nodeStage = mock(InFieldStage.class);
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
                InFieldStage.stage.put(type, null);
                assertVerify(changeRules, fieldMap, nodeStage, 1, 1, 1);
            } finally {
                InFieldStage.stage.remove(type);
            }
        }

        @Test
        void testStartStageContainsKeyAndNotNull() {
            try {
                InFieldStage.stage.put(type, nodeStage);
                assertVerify(changeRules, fieldMap, nodeStage, 1, 1, 0);
            } finally {
                InFieldStage.stage.remove(type);
            }
        }

        @Test
        void testStartStageNodeStageNull() {
            assertVerify(changeRules, fieldMap, null, 1, 0, 1);

        }

        void assertVerify(List<FieldChangeRule> changeR, Map<String, List<Field>> fieldM,
                          InFieldStage node,
                          int changeRuleTimes, int nodeStageTimes,
                          int createNodeStageTimes) {
            try (MockedStatic<InFieldStage> mockedStatic = mockStatic(InFieldStage.class)) {
                mockedStatic.when(() -> InFieldStage.createNodeStage(anyString())).thenReturn(node);
                mockedStatic.when(() -> InFieldStage.startStage(anyList(), anyMap())).thenCallRealMethod();
                mockedStatic.when(() -> InFieldStage.startStage(null, fieldMap)).thenCallRealMethod();
                InFieldStage.startStage(changeR, fieldM);
                mockedStatic.verify(() -> InFieldStage.createNodeStage(anyString()), times(createNodeStageTimes));
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
            InFieldStage stage = assertVerify(FieldChangeRule.Type.DataType.name());
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(FieldOfDataType.class, stage.getClass());
        }
        @Test
        void testCreateNodeStageNodeOfMultiDataType() {
            InFieldStage stage = assertVerify(FieldChangeRule.Type.MutiDataType.name());
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(FieldOfMultiDataType.class, stage.getClass());
        }
        @Test
        void testCreateNodeStageNullType() {
            InFieldStage stage = assertVerify(null);
            Assertions.assertNull(stage);
        }

        @Test
        void testCreateNodeStageOtherType() {
            InFieldStage stage = assertVerify("xxx");
            Assertions.assertNull(stage);
        }


        InFieldStage assertVerify(String type) {
            InFieldStage stage = null;
            try (MockedStatic<InFieldStage> mockedStatic = mockStatic(InFieldStage.class)) {
                mockedStatic.when(() -> InFieldStage.createNodeStage(anyString())).thenCallRealMethod();
                mockedStatic.when(() -> InFieldStage.createNodeStage(null)).thenCallRealMethod();
                stage = InFieldStage.createNodeStage(type);
            }
            return stage;
        }
    }
}
