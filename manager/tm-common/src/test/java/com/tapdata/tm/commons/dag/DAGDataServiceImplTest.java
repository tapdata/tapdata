package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class DAGDataServiceImplTest {
    DAGDataServiceImpl service;
    @BeforeEach
    void init() {
        service = mock(DAGDataServiceImpl.class);
    }

    @Nested
    class ModelDeductionTest {
        MetadataInstancesDto metadataInstancesDto;
        Schema schema;
        DataSourceConnectionDto dataSource;
        boolean needPossibleDataTypes;
        DAG.Options options;
        @BeforeEach
        void init() {
            metadataInstancesDto = mock(MetadataInstancesDto.class);
            schema = mock(Schema.class);
            dataSource = mock(DataSourceConnectionDto.class);
            needPossibleDataTypes = true;
            options = mock(DAG.Options.class);
            when(options.isIsomorphismTask()).thenReturn(true);

            when(service.modelDeduction(null, schema, dataSource, needPossibleDataTypes, options)).thenCallRealMethod();
            when(service.modelDeduction(any(MetadataInstancesDto.class), any(Schema.class), any(DataSourceConnectionDto.class), anyBoolean(), any(DAG.Options.class))).thenCallRealMethod();
            doNothing().when(service).setFieldChangeRuleToMetadata(any(MetadataInstancesDto.class), any(DAG.Options.class));
            when(service.processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean())).thenReturn(metadataInstancesDto);
            when(service.processFieldToDB(schema, null, dataSource, needPossibleDataTypes)).thenReturn(metadataInstancesDto);
        }

        @Test
        void testModelDeductionNormal() {
            MetadataInstancesDto dto = service.modelDeduction(this.metadataInstancesDto, schema, dataSource, needPossibleDataTypes, options);
            Assertions.assertEquals(metadataInstancesDto, dto);
//            verify(options, times(1)).isIsomorphismTask();
            verify(service, times(1)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
//            verify(service, times(1)).setFieldChangeRuleToMetadata(any(MetadataInstancesDto.class), any(DAG.Options.class));
        }
//        @Test
//        void testModelDeductionNullMetadataInstances() {
//            MetadataInstancesDto dto = service.modelDeduction(null, schema, dataSource, needPossibleDataTypes, options);
//            Assertions.assertEquals(metadataInstancesDto, dto);
//            verify(options, times(0)).isIsomorphismTask();
//            verify(service, times(1)).processFieldToDB(schema, null, dataSource, needPossibleDataTypes);
//            verify(service, times(1)).setFieldChangeRuleToMetadata(any(MetadataInstancesDto.class), any(DAG.Options.class));
//        }
//        @Test
//        void testModelDeductionNotIsomorphismTask() {
//            when(options.isIsomorphismTask()).thenReturn(false);
//            MetadataInstancesDto dto = service.modelDeduction(metadataInstancesDto, schema, dataSource, needPossibleDataTypes, options);
//            Assertions.assertEquals(metadataInstancesDto, dto);
//            verify(options, times(1)).isIsomorphismTask();
//            verify(service, times(1)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
//            verify(service, times(1)).setFieldChangeRuleToMetadata(any(MetadataInstancesDto.class), any(DAG.Options.class));
//        }
    }

    @Nested
    class SetFieldChangeRuleToMetadataTest {
        MetadataInstancesDto metadataInstancesDto;
        DAG.Options options;
        List<Field> fields;
        Field field;
        String fieldName;
        FieldChangeRuleGroup fieldChangeRules;
        Map<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> rules;
        Set<Map.Entry<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>>> ruleEntries;
        Map.Entry<String, Map<FieldChangeRule.Scope, List<FieldChangeRule>>> entry;
        Map<FieldChangeRule.Scope, List<FieldChangeRule>> entryValue;
        List<FieldChangeRule> changeRules;
        FieldChangeRule changeRule;
        String accept;

        @BeforeEach
        void init() {
            options = mock(DAG.Options.class);
            fields = new ArrayList<>();
            field = mock(Field.class);
            fields.add(field);

            fieldName = "fieldName";
            fieldChangeRules = mock(FieldChangeRuleGroup.class);
            rules = mock(Map.class);

            entry = mock(Map.Entry.class);
            ruleEntries = new HashSet<>();
            ruleEntries.add(entry);

            entryValue = mock(Map.class);
            changeRule = mock(FieldChangeRule.class);
            accept = "accept";

            changeRules = new ArrayList<FieldChangeRule>();
            changeRules.add(changeRule);

            metadataInstancesDto = mock(MetadataInstancesDto.class);
            when(metadataInstancesDto.getFields()).thenReturn(fields);


            when(options.isIsomorphismTask()).thenReturn(true);
            when(options.getFieldChangeRules()).thenReturn(fieldChangeRules);

            when(fieldChangeRules.getRules()).thenReturn(rules);

            when(rules.size()).thenReturn(1);
            when(rules.entrySet()).thenReturn(ruleEntries);

            when(entry.getValue()).thenReturn(entryValue);

            when(entryValue.get(FieldChangeRule.Scope.Field)).thenReturn(changeRules);

            when(changeRule.getAccept()).thenReturn(accept);
            when(changeRule.getFieldName()).thenReturn(fieldName);

            when(field.getDataType()).thenReturn(accept);
            doNothing().when(field).setSource(Field.SOURCE_MANUAL);
            when(field.getFieldName()).thenReturn(fieldName);

            doCallRealMethod().when(service).setFieldChangeRuleToMetadata(metadataInstancesDto, options);
            doCallRealMethod().when(service).setFieldChangeRuleToMetadata(null, options);
        }

        @Test
        void testSetFieldChangeRuleToMetadataNormal() {
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1);
        }

        @Test
        void testSetFieldChangeRuleToMetadataNullMetadata() {
            assertVerify(null,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataNotIsIsomorphismTask() {
            when(options.isIsomorphismTask()).thenReturn(false);
            assertVerify(metadataInstancesDto,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataNullFieldChangeRules() {
            when(options.getFieldChangeRules()).thenReturn(null);
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataRulesSizeNotOne() {
            when(rules.size()).thenReturn(6);
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataRulesEntrySetIsEmpty() {
            when(rules.entrySet()).thenReturn(new HashSet<>());
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataChangeRulesIsEmpty() {
            when(entryValue.get(FieldChangeRule.Scope.Field)).thenReturn(new ArrayList<>());
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0);
        }


        @Test
        void testSetFieldChangeRuleToMetadataChangeRulesNotContainsTheSameFiled() {
            when(changeRule.getFieldName()).thenReturn("mock-fieldName");
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataChangeRulesAcceptIsNull() {
            when(changeRule.getAccept()).thenReturn(null);
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0);
        }

        @Test
        void testSetFieldChangeRuleToMetadataChangeRulesAcceptNotEqualsFieldDataType() {
            when(field.getDataType()).thenReturn("mock-accept");
            assertVerify(metadataInstancesDto,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0);
        }

        void assertVerify(MetadataInstancesDto dto,
                          int isIsomorphismTaskTime,
                          int getFieldsTimes,
                          int getFieldNameTimes,
                          int getFieldChangeRulesTimes,
                          int getRulesTimes,
                          int sizeTimes,
                          int entrySetTimes,
                          int getValueTimes,
                          int valueGetTimes,
                          int getAcceptTimes,
                          int changeRuleGetFieldNameTimes,
                          int setSourceTimes) {
            service.setFieldChangeRuleToMetadata(dto, options);
            verify(options, times(isIsomorphismTaskTime)).isIsomorphismTask();
            verify(metadataInstancesDto, times(getFieldsTimes)).getFields();
            verify(field, times(getFieldNameTimes)).getFieldName();
            verify(options, times(getFieldChangeRulesTimes)).getFieldChangeRules();
            verify(fieldChangeRules, times(getRulesTimes)).getRules();
            verify(rules, times(sizeTimes)).size();
            verify(rules, times(entrySetTimes)).entrySet();
            verify(entry, times(getValueTimes)).getValue();
            verify(entryValue, times(valueGetTimes)).get(FieldChangeRule.Scope.Field);
            verify(changeRule, times(getAcceptTimes)).getAccept();
            verify(changeRule, times(changeRuleGetFieldNameTimes)).getFieldName();
            verify(field, times(setSourceTimes)).setSource(Field.SOURCE_MANUAL);
        }
    }
}