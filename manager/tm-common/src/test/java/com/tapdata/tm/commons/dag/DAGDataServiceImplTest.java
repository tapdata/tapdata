package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.deduction.rule.ChangeRuleStage;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.springframework.test.util.ReflectionTestUtils;


import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;


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
                verify(options, times(0)).isIsomorphismTask();
                verify(service, times(1)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(metadataInstancesDto, options), times(0));
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
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class)), times(0));
            }
        }
        @Test
        void testModelDeductionNotIsomorphismTask() {
            try (MockedStatic<ChangeRuleStage> mockedStatic = mockStatic(ChangeRuleStage.class)) {
                mockedStatic.when(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class))).thenAnswer(w->null);
                when(options.isIsomorphismTask()).thenReturn(false);
                MetadataInstancesDto dto = service.modelDeduction(metadataInstancesDto, schema, dataSource, needPossibleDataTypes, options);
                Assertions.assertEquals(metadataInstancesDto, dto);
                verify(options, times(0)).isIsomorphismTask();
                verify(service, times(1)).processFieldToDB(any(Schema.class), any(MetadataInstancesDto.class), any(DataSourceConnectionDto.class), anyBoolean());
                mockedStatic.verify(() -> ChangeRuleStage.changeStart(any(MetadataInstancesDto.class), any(DAG.Options.class)), times(0));
            }
        }
    }
    @Nested
    class clearTransformerTest{
        DAGDataServiceImpl dagDataService = new DAGDataServiceImpl(mock(TransformerWsMessageDto.class));
        @Test
        void test(){
            dagDataService.clearTransformer();
            Assertions.assertTrue(dagDataService.getBatchMetadataUpdateMap().isEmpty());
            Assertions.assertTrue(dagDataService.getBatchInsertMetaDataList().isEmpty());
            Assertions.assertTrue(dagDataService.getUpsertItems().isEmpty());
        }
    }
    @Nested
    class logCollectorMetadataInstancesDtoTest{
        DAGDataServiceImpl dagDataService = new DAGDataServiceImpl(mock(TransformerWsMessageDto.class));
        Map<String, MetadataInstancesDto> metadataMap = new HashMap<>();
        @Test
        void test(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("LogCollectorNode");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("test");
            metadataInstancesDto2.setOriginalName("name2");
            metadataInstancesDto2.setQualifiedName("test2");
            metadataInstancesDto2.setMetaType("table");
            metadataMap.put("node1",metadataInstancesDto1);
            metadataMap.put("node2",metadataInstancesDto2);
            ReflectionTestUtils.setField(dagDataService,"metadataMap",metadataMap);
            Assertions.assertEquals(2,dagDataService.getLogCollectorMetadataInstancesDto().size());
        }

        @Test
        void testMetaDataMapIsNull(){
            ReflectionTestUtils.setField(dagDataService,"metadataMap",metadataMap);
            Assertions.assertEquals(0,dagDataService.getLogCollectorMetadataInstancesDto().size());
        }

        @Test
        void testQualifiedNameIsNullOne(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("LogCollectorNode");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("test");
            metadataInstancesDto2.setOriginalName("name2");
            metadataInstancesDto2.setQualifiedName("test2");
            metadataInstancesDto2.setMetaType("table");
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            metadataInstancesDto3.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto3.setNodeId("test");
            metadataInstancesDto3.setOriginalName("name2");
            metadataInstancesDto3.setQualifiedName("test2");
            metadataInstancesDto3.setMetaType("table");
            metadataMap.put("node1",metadataInstancesDto1);
            metadataMap.put("node2",metadataInstancesDto2);
            metadataMap.put("node3",metadataInstancesDto3);
            ReflectionTestUtils.setField(dagDataService,"metadataMap",metadataMap);
            Assertions.assertEquals(2,dagDataService.getLogCollectorMetadataInstancesDto().size());
        }
    }

}