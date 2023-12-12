package com.tapdata.tm.commons.dag.deduction.rule;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChangeRuleStageTest {
    ChangeRuleStage stage;
    @BeforeEach
    void init() {
        stage = mock(ChangeRuleStage.class);
    }

    @Nested
    class ChangeStartTest {
        MetadataInstancesDto metadataInstancesDto;
        DAG.Options options;
        FieldChangeRuleGroup fieldChangeRules;
        Map<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> rules;
        Set<Map.Entry<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>>> entrySet;
        Map.Entry<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> entry;
        Map<FieldChangeRule.Scope, List<FieldChangeRule>> scopeListMap;
        Set<FieldChangeRule.Scope> scopes;
        ChangeRuleStage changeRuleStage;
        FieldChangeRule.Scope scope;
        List<FieldChangeRule> scopeList;
        @BeforeEach
        void init() {
            metadataInstancesDto = mock(MetadataInstancesDto.class);
            options = mock(DAG.Options.class);
            fieldChangeRules = mock(FieldChangeRuleGroup.class);
            rules = mock(Map.class);
            entry = mock(Map.Entry.class);
            entrySet = new HashSet<>();
            entrySet.add(entry);

            scopeListMap = mock(Map.class);
            changeRuleStage = mock(ChangeRuleStage.class);
            scope = mock(FieldChangeRule.Scope.class);
            scopes = new HashSet<>();
            scopes.add(scope);

            scopeList = mock(List.class);

            when(options.isIsomorphismTask()).thenReturn(true);
            when(options.getFieldChangeRules()).thenReturn(fieldChangeRules);

            when(fieldChangeRules.getRules()).thenReturn(rules);
            when(rules.size()).thenReturn(1);
            when(rules.entrySet()).thenReturn(entrySet);
            when(entry.getValue()).thenReturn(scopeListMap);
            when(scopeListMap.keySet()).thenReturn(scopes);
            when(scopeListMap.get(any(FieldChangeRule.Scope.class))).thenReturn(scopeList);

            doNothing().when(changeRuleStage).change(anyList(), any(MetadataInstancesDto.class));
        }
        void acceptVerify(ChangeRuleStage changeRuleStagByCreate, MetadataInstancesDto me, DAG.Options op,
                          int createTimes, int isIsomorphismTaskTimes, int getFieldChangeRulesTimes,
                          int getRulesTimes, int getRulesSizeTimes, int entrySetTimes, int getValueTimes,
                          int keySetTimes, int getTimes, int changeTimes) {
            try (MockedStatic<ChangeRuleStage> mocked = mockStatic(ChangeRuleStage.class)) {
                mocked.when(() -> ChangeRuleStage.changeStart(metadataInstancesDto, options)).thenCallRealMethod();
                mocked.when(() -> ChangeRuleStage.changeStart(null, options)).thenCallRealMethod();
                mocked.when(() -> ChangeRuleStage.changeStart(null, null)).thenCallRealMethod();
                mocked.when(() -> ChangeRuleStage.changeStart(metadataInstancesDto, null)).thenCallRealMethod();
                mocked.when(() -> ChangeRuleStage.create(any(FieldChangeRule.Scope.class))).thenReturn(changeRuleStagByCreate);
                ChangeRuleStage.changeStart(me, op);
                mocked.verify(() -> ChangeRuleStage.create(any(FieldChangeRule.Scope.class)), times(createTimes));
            } finally {
                verify(options, times(isIsomorphismTaskTimes)).isIsomorphismTask();
                verify(options, times(getFieldChangeRulesTimes)).getFieldChangeRules();
                verify(fieldChangeRules, times(getRulesTimes)).getRules();
                verify(rules, times(getRulesSizeTimes)).size();
                verify(rules, times(entrySetTimes)).entrySet();
                verify(entry, times(getValueTimes)).getValue();
                verify(scopeListMap, times(keySetTimes)).keySet();
                verify(scopeListMap, times(getTimes)).get(any(FieldChangeRule.Scope.class));
                verify(changeRuleStage, times(changeTimes)).change(anyList(), any(MetadataInstancesDto.class));
            }
        }
        @Test
        void testNormal() {
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1);
        }
        @Test
        void nullMetadataInstancesDto() {
            acceptVerify(changeRuleStage, null, options,
                    0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void nullOptions() {
            acceptVerify(changeRuleStage, metadataInstancesDto, null,
                    0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void notIsomorphismTask(){
            when(options.isIsomorphismTask()).thenReturn(false);
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    0, 1, 0,
                    0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void nullFieldChangeRuleGroup() {
            when(options.getFieldChangeRules()).thenReturn(null);
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    0, 1, 1,
                    0, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void rulesIsNull() {
            when(fieldChangeRules.getRules()).thenReturn(null);
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    0, 1, 1,
                    1, 0, 0, 0, 0, 0, 0);
        }
        @Test
        void rulesSizeNotOne() {
            when(rules.size()).thenReturn(2);
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    0, 1, 1,
                    1, 1, 0, 0, 0, 0, 0);
        }
        @Test
        void nullScopeInScopeList() {
            scopes.add(null);
            acceptVerify(changeRuleStage, metadataInstancesDto, options,
                    1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1);
        }
        @Test
        void stagesMapContainsKey() {
            try {
                ChangeRuleStage.stages.put(scope, changeRuleStage);
                acceptVerify(changeRuleStage, metadataInstancesDto, options,
                        0, 1, 1,
                        1, 1, 1, 1, 1, 1, 1);
            } finally {
                ChangeRuleStage.stages.remove(scope);
            }
        }
        @Test
        void stagesMapContainsKeyButValueIsNull() {
            try {
                ChangeRuleStage.stages.put(scope, null);
                acceptVerify(changeRuleStage, metadataInstancesDto, options,
                        1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1);
            } finally {
                ChangeRuleStage.stages.remove(scope);
            }
        }
        @Test
        void whenCreateReturnNullChangeRuleStage() {
            acceptVerify(null, metadataInstancesDto, options,
                    1, 1, 1,
                    1, 1, 1, 1, 1, 0, 0);
        }
    }

    @Nested
    class CreateTest {
        ChangeRuleStage assertVerify(FieldChangeRule.Scope scope) {
            try(MockedStatic<ChangeRuleStage> mockedStatic = mockStatic(ChangeRuleStage.class)) {
                mockedStatic.when(() -> ChangeRuleStage.create(scope)).thenCallRealMethod();
                return ChangeRuleStage.create(scope);
            }
        }
        @Test
        void testNode() {
            ChangeRuleStage stage = assertVerify(FieldChangeRule.Scope.Node);
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(NodeStage.class, stage.getClass());
        }
        @Test
        void testField() {
            ChangeRuleStage stage = assertVerify(FieldChangeRule.Scope.Field);
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(FieldStage.class, stage.getClass());
        }
        @Test
        void testTable() {
            ChangeRuleStage stage = assertVerify(FieldChangeRule.Scope.Table);
            Assertions.assertNotNull(stage);
            Assertions.assertEquals(TableStage.class, stage.getClass());
        }
        @Test
        void testNullScope() {
            ChangeRuleStage stage = assertVerify(null);
            Assertions.assertNull(stage);
        }
    }
}