package io.tapdata.common;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.schema.SchemaApplyResult;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastSchemaTargetNode;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskClient;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.schema.TapTableMap;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.*;

public class DAGDataEngineServiceImplTest {
    DAGDataEngineServiceImpl dagDataEngineService;
    TransformerWsMessageDto transformerWsMessageDto;
    HazelcastTaskService taskService;
    HttpClientMongoOperator clientMongoOperator;
    @BeforeEach
    void setUp() {
        transformerWsMessageDto = new TransformerWsMessageDto();
        transformerWsMessageDto.setMetadataInstancesDtoList(new ArrayList<>());
        transformerWsMessageDto.setUserId("test");
        DAG.Options options = new DAG.Options();
        options.setUuid("test");
        transformerWsMessageDto.setOptions(options);
        transformerWsMessageDto.setDefinitionDtoMap(new HashMap<>());
        transformerWsMessageDto.setDataSourceMap(new HashMap<>());
        transformerWsMessageDto.setTransformerDtoMap(new HashMap<>());
        taskService = mock(HazelcastTaskService.class);
        clientMongoOperator = mock(HttpClientMongoOperator.class);
    }
    @Nested
    class loadTapTableTest{
        @Test
        @DisplayName("test loadTapTable main process ")
        void testLoadTapTable(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<HazelcastSchemaTargetNode> hazelcastSchemaTargetNodeMockedStatic = mockStatic(HazelcastSchemaTargetNode.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                when(obsLogger.isDebugEnabled()).thenReturn(true);
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                when(taskService.startTestTask(any(TaskDto.class))).thenReturn(mock(HazelcastTaskClient.class));
                TapTable tapTable = mock(TapTable.class);
                hazelcastSchemaTargetNodeMockedStatic.when(()->HazelcastSchemaTargetNode.getTapTable(any())).thenReturn(tapTable);
                TapTable result = dagDataEngineService.loadTapTable("test","test",taskDto);
                Assertions.assertEquals(tapTable,result);
            }
        }

        @Test
        @DisplayName("test loadTapTable getTapTable error ")
        void testLoadTapTableError(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                TapTable result = dagDataEngineService.loadTapTable("test","test",taskDto);
                Assertions.assertNull(result);
            }
        }
    }
    @Nested
    class getJsResultTest{
        @Test
        @DisplayName("Get JsResult main process ")
        void GetJsResultTest(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<HazelcastSchemaTargetNode> hazelcastSchemaTargetNodeMockedStatic = mockStatic(HazelcastSchemaTargetNode.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                when(obsLogger.isDebugEnabled()).thenReturn(true);
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                when(taskService.startTestTask(any(TaskDto.class))).thenReturn(mock(HazelcastTaskClient.class));
                List<SchemaApplyResult> schemaApplyResults = new ArrayList<>();
                SchemaApplyResult schemaApplyResult = new SchemaApplyResult("test","test",new TapField("test","String"));
                schemaApplyResult.setTapIndex(new TapIndex());
                schemaApplyResults.add(schemaApplyResult);
                hazelcastSchemaTargetNodeMockedStatic.when(()->HazelcastSchemaTargetNode.getSchemaApplyResultList(any())).thenReturn(schemaApplyResults);
                List<MigrateJsResultVo> result = dagDataEngineService.getJsResult("test","test",taskDto);
                Assertions.assertEquals(1,result.size());
            }
        }

        @Test
        @DisplayName("Get JsResult error")
        void GetJsResultTestError(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                List<MigrateJsResultVo> result = dagDataEngineService.getJsResult("test","test",taskDto);
                Assertions.assertTrue(result.isEmpty());
            }
        }
    }
    @Nested
    class convertTapTableTest{
        MetadataInstancesDto metadataInstancesDto;

        @Test
        @DisplayName("convert TapTable Test main process ")
        void testConvertTapTable(){
            metadataInstancesDto = new MetadataInstancesDto();
            List<Field> fields = new ArrayList<>();
            Field field1 = new Field();
            field1.setFieldName("test1");
            field1.setTapType("{\"type\":8}");
            field1.setDeleted(false);
            Field field2 = new Field();
            field2.setFieldName("test2");
            field2.setTapType("{\"type\":8}");
            field2.setDeleted(false);
            Field field3 = new Field();
            field3.setFieldName("test3");
            field3.setTapType("{\"type\":8}");
            field3.setDeleted(false);
            fields.add(field1);
            fields.add(field2);
            fields.add(field3);
            metadataInstancesDto.setFields(fields);
            List<TableIndex> indices = new ArrayList<>();
            TableIndex tableIndex = new TableIndex();
            List<TableIndexColumn> columns = new ArrayList<>();
            TableIndexColumn column1 = new TableIndexColumn();
            column1.setColumnName("test1");
            TableIndexColumn column2 = new TableIndexColumn();
            column2.setColumnName("test2");
            columns.add(column1);
            columns.add(column2);
            tableIndex.setColumns(columns);
            indices.add(tableIndex);
            metadataInstancesDto.setIndices(indices);
            Map<String, PossibleDataTypes> dataTypes = new HashMap<>();
            dataTypes.put("test1",new PossibleDataTypes().dataType("String"));
            dataTypes.put("test3",new PossibleDataTypes());
            metadataInstancesDto.setFindPossibleDataTypes(dataTypes);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}",TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                TapTable result = dagDataEngineService.convertTapTable(metadataInstancesDto);
                Assertions.assertTrue(result.getNameFieldMap().containsKey("test1"));
            }

        }

        @Test
        @DisplayName("convert TapTable Test DataTypes Is Null ")
        void testConvertTapTableDataTypesIsNull(){
            metadataInstancesDto = new MetadataInstancesDto();
            List<Field> fields = new ArrayList<>();
            Field field1 = new Field();
            field1.setFieldName("test1");
            field1.setTapType("{\"type\":8}");
            field1.setDeleted(false);
            Field field2 = new Field();
            field2.setFieldName("test2");
            field2.setTapType("{\"type\":8}");
            field2.setDeleted(false);
            Field field3 = new Field();
            field3.setFieldName("test3");
            field3.setTapType("{\"type\":8}");
            field3.setDeleted(false);
            fields.add(field1);
            fields.add(field2);
            fields.add(field3);
            metadataInstancesDto.setFields(fields);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}",TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                TapTable result = dagDataEngineService.convertTapTable(metadataInstancesDto);
                Assertions.assertTrue(result.getNameFieldMap().containsKey("test1"));
                Assertions.assertTrue(result.getNameFieldMap().containsKey("test2"));
                Assertions.assertTrue(result.getNameFieldMap().containsKey("test3"));
            }

        }

        @Test
        @DisplayName("convert TapTable Test Fields is null  ")
        void testConvertTapTableFieldsIsNull(){
            metadataInstancesDto = new MetadataInstancesDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<JSON> jsonMockedStatic = mockStatic(JSON.class)){
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                jsonMockedStatic.when(()->JSON.parseObject("{\"type\":8}",TapType.class)).thenReturn(new TapString()).thenReturn(new TapRaw()).thenReturn(new TapString());
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new ConcurrentHashMap<>(),clientMongoOperator);
                TapTable result = dagDataEngineService.convertTapTable(metadataInstancesDto);
                Assertions.assertTrue(result.getNameFieldMap().isEmpty());
            }
        }
    }
    @Nested
    class initializeModelTest{
        Map<String, MetadataInstancesDto> batchMetadataUpdateMap = new LinkedHashMap<>();
        List<MetadataInstancesDto> batchInsertMetaDataList = new ArrayList<>();
        @Test
        @DisplayName("InitializeModel Test main process")
        void testInitializeModel(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("test1");
            metadataInstancesDto1.setOriginalName("test1");
            metadataInstancesDto1.setQualifiedName("test1");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("test2");
            metadataInstancesDto2.setOriginalName("test2");
            metadataInstancesDto2.setQualifiedName("test2");
            batchMetadataUpdateMap.put("test1",metadataInstancesDto1);
            batchMetadataUpdateMap.put("test2",metadataInstancesDto2);
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            metadataInstancesDto3.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto3.setNodeId("test2");
            metadataInstancesDto3.setOriginalName("test3");
            metadataInstancesDto3.setQualifiedName("test3");
            batchInsertMetaDataList.add(metadataInstancesDto3);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,tapTableMapHashMap,clientMongoOperator);
                ReflectionTestUtils.setField(dagDataEngineService,"batchMetadataUpdateMap",batchMetadataUpdateMap);
                ReflectionTestUtils.setField(dagDataEngineService,"batchInsertMetaDataList",batchInsertMetaDataList);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                dagDataEngineService.initializeModel();
                Assertions.assertTrue(tapTableMapHashMap.get("test1").containsKey("test1"));
                Assertions.assertTrue(tapTableMapHashMap.get("test2").containsKey("test2"));
                Assertions.assertTrue(tapTableMapHashMap.get("test2").containsKey("test3"));
            }
        }

        @Test
        @DisplayName("InitializeModel Test getBatchMetadataUpdateMap and getBatchInsertMetaDataList is null")
        void testInitializeModeMetadataInstancesDtoIsNull(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,tapTableMapHashMap,clientMongoOperator);
                ReflectionTestUtils.setField(dagDataEngineService,"batchMetadataUpdateMap",batchMetadataUpdateMap);
                ReflectionTestUtils.setField(dagDataEngineService,"batchInsertMetaDataList",batchInsertMetaDataList);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                dagDataEngineService.initializeModel();
                Assertions.assertTrue(tapTableMapHashMap.isEmpty());
            }
        }


    }
    @Nested
    class uploadModelTest{
        @Test
        @DisplayName("UploadModel Test main process")
        void testUploadModel(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                List<Node> nodes = new ArrayList<>();
                nodes.add(new DatabaseNode());
                when(dag.getNodes()).thenReturn(nodes);
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,new HashMap<>(),clientMongoOperator);
                dagDataEngineService.uploadModel(new HashMap<>());
                verify(clientMongoOperator,times(1)).insertOne(any(),any());
            }
        }
    }
}
