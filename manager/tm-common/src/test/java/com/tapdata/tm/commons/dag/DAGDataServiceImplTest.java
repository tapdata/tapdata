package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.deduction.rule.ChangeRuleStage;
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
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
            when(service.processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean())).thenReturn(metadataInstancesDto);
            when(service.processFieldToDB(schema, null, dataSource, needPossibleDataTypes)).thenReturn(metadataInstancesDto);
        }

        @Test
        void testModelDeductionNormal() {
            try (MockedStatic<ChangeRuleStage> mockedStatic = mockStatic(ChangeRuleStage.class)) {
                mockedStatic.when(() -> ChangeRuleStage.changeStart(metadataInstancesDto, options)).thenAnswer(w->null);
                MetadataInstancesDto dto = service.modelDeduction(this.metadataInstancesDto, schema, dataSource, needPossibleDataTypes, options);
                Assertions.assertEquals(metadataInstancesDto, dto);
                verify(options, times(1)).isIsomorphismTask();
                verify(service, times(0)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(metadataInstancesDto, options), times(1));
            }
        }
        @Test
        void testModelDeductionNullMetadataInstances() {
            try (MockedStatic<ChangeRuleStage> mockedStatic = mockStatic(ChangeRuleStage.class)) {
                mockedStatic.when(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class))).thenAnswer(w->null);
                MetadataInstancesDto dto = service.modelDeduction(null, schema, dataSource, needPossibleDataTypes, options);
                Assertions.assertEquals(metadataInstancesDto, dto);
                verify(options, times(0)).isIsomorphismTask();
                verify(service, times(1)).processFieldToDB(schema, null, dataSource, needPossibleDataTypes);
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class)), times(1));
            }
        }
        @Test
        void testModelDeductionNotIsomorphismTask() {
            try (MockedStatic<ChangeRuleStage> mockedStatic = mockStatic(ChangeRuleStage.class)) {
                mockedStatic.when(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class))).thenAnswer(w->null);
                when(options.isIsomorphismTask()).thenReturn(false);
                MetadataInstancesDto dto = service.modelDeduction(metadataInstancesDto, schema, dataSource, needPossibleDataTypes, options);
                Assertions.assertEquals(metadataInstancesDto, dto);
                verify(options, times(1)).isIsomorphismTask();
                verify(service, times(1)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class)), times(1));
            }
        }
    }

}