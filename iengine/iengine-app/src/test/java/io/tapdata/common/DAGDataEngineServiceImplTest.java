package io.tapdata.common;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.schema.SchemaApplyResult;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceDto;
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
import org.springframework.data.mongodb.core.query.Query;
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
        Map<String, MetadataInstancesDto> metadataMap = new HashMap<>();
        @Test
        @DisplayName("InitializeModel Test main process")
        void testInitializeModel(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("node1");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("node2");
            metadataInstancesDto2.setOriginalName("name2");
            metadataInstancesDto2.setQualifiedName("test2");
            metadataInstancesDto2.setMetaType("table");
            batchMetadataUpdateMap.put("node1",metadataInstancesDto1);
            batchMetadataUpdateMap.put("node2",metadataInstancesDto2);
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            metadataInstancesDto3.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto3.setNodeId("node2");
            metadataInstancesDto3.setOriginalName("name3");
            metadataInstancesDto3.setQualifiedName("test3");
            metadataInstancesDto3.setMetaType("processor_node");
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
                dagDataEngineService.initializeModel(true);
                Assertions.assertTrue(tapTableMapHashMap.get("node1").containsKey("name1"));
                Assertions.assertTrue(tapTableMapHashMap.get("node2").containsKey("name2"));
                Assertions.assertTrue(tapTableMapHashMap.get("node2").containsKey("node2"));
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
                dagDataEngineService.initializeModel(false);
                Assertions.assertTrue(tapTableMapHashMap.isEmpty());
            }
        }

        @Test
        @DisplayName("InitializeModel Test main process")
        void testInitializeModelMergeNode(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("node1");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("node2");
            metadataInstancesDto2.setOriginalName("name2");
            metadataInstancesDto2.setQualifiedName("test2");
            metadataInstancesDto2.setMetaType("table");
            batchMetadataUpdateMap.put("node1",metadataInstancesDto1);
            batchMetadataUpdateMap.put("node2",metadataInstancesDto2);
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            metadataInstancesDto3.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto3.setNodeId("mergeNode");
            metadataInstancesDto3.setOriginalName("name3");
            metadataInstancesDto3.setQualifiedName("test3");
            metadataInstancesDto3.setMetaType("processor_node");
            batchInsertMetaDataList.add(metadataInstancesDto3);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG mocDag = mock(DAG.class);
            taskDto.setDag(mocDag);
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
                List<Node> nodes = new ArrayList<>();
                MergeTableNode mergeTableNode = new MergeTableNode();
                mergeTableNode.setId("mergeNode");
                nodes.add(mergeTableNode);
                when(mocDag.getNodes()).thenReturn(nodes);
                List<Node> predecessors = new ArrayList<>();
                TableNode tableNode1 = new TableNode();
                tableNode1.setId("node1");
                TableNode tableNode2 = new TableNode();
                tableNode2.setId("node2");
                predecessors.add(tableNode1);
                predecessors.add(tableNode2);
                when(mocDag.predecessors("mergeNode")).thenReturn(predecessors);
                dagDataEngineService.initializeModel(true);
                Assertions.assertEquals(3,tapTableMapHashMap.get("mergeNode").size());
            }
        }

        @Test
        void testInitializeModelLogCollectorNode(){
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
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG mocDag = mock(DAG.class);
            taskDto.setDag(mocDag);
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,tapTableMapHashMap,clientMongoOperator);
                ReflectionTestUtils.setField(dagDataEngineService,"metadataMap",metadataMap);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                List<Node> nodes = new ArrayList<>();
                LogCollectorNode logCollectorNode = new LogCollectorNode();
                logCollectorNode.setId("LogCollectorNode");
                nodes.add(logCollectorNode);
                when(mocDag.getNodes()).thenReturn(nodes);
                dagDataEngineService.initializeModel(true);
                Assertions.assertEquals(2,tapTableMapHashMap.get("LogCollectorNode").size());
            }
        }

        @Test
        void testInitializeModelLogCollectorNodeConnConfigsIsNotNull(){
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("LogCollectorNode");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            SourceDto sourceDto1 = new SourceDto();
            sourceDto1.set_id("s_1");
            metadataInstancesDto1.setSource(sourceDto1);
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto2.setNodeId("test");
            metadataInstancesDto2.setOriginalName("name2");
            metadataInstancesDto2.setQualifiedName("test2");
            metadataInstancesDto2.setMetaType("table");
            SourceDto sourceDto2 = new SourceDto();
            sourceDto2.set_id("s_2");
            metadataInstancesDto2.setSource(sourceDto2);
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            metadataInstancesDto3.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto3.setNodeId("test3");
            metadataInstancesDto3.setOriginalName("name3");
            metadataInstancesDto3.setQualifiedName("test3");
            metadataInstancesDto3.setMetaType("table");
            SourceDto sourceDto3 = new SourceDto();
            sourceDto3.set_id("s_3");
            metadataInstancesDto3.setSource(sourceDto3);
            metadataMap.put("node1",metadataInstancesDto1);
            metadataMap.put("node2",metadataInstancesDto2);
            metadataMap.put("node3",metadataInstancesDto3);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG mocDag = mock(DAG.class);
            taskDto.setDag(mocDag);
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,tapTableMapHashMap,clientMongoOperator);
                ReflectionTestUtils.setField(dagDataEngineService,"metadataMap",metadataMap);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                List<Node> nodes = new ArrayList<>();
                LogCollectorNode logCollectorNode = new LogCollectorNode();
                logCollectorNode.setId("LogCollectorNode");
                Map<String, LogCollecotrConnConfig> connConfigs = new HashMap<>();
                connConfigs.put("test1",new LogCollecotrConnConfig());
                logCollectorNode.setLogCollectorConnConfigs(connConfigs);
                Connections connections1 = new Connections();
                connections1.setId("s_1");
                connections1.setNamespace(Arrays.asList("C##Test1"));
                Connections connections2 = new Connections();
                connections2.setId("s_2");
                connections2.setNamespace(Arrays.asList("C##Test2"));
                connections2.setNamespace(Arrays.asList("C##Test2"));
                List<Connections>connections = new ArrayList<>();
                connections.add(connections1);
                connections.add(connections2);
                nodes.add(logCollectorNode);
                when(clientMongoOperator.find(any(Query.class),any(),any())).thenReturn(new ArrayList<>(connections));
                when(mocDag.getNodes()).thenReturn(nodes);
                dagDataEngineService.initializeModel(true);
                Assertions.assertEquals(2,tapTableMapHashMap.get("LogCollectorNode").size());
            }
        }
        @Test
        void testInitializeModelLogCollectorNodeConnConfigsIsEmpt(){
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
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG mocDag = mock(DAG.class);
            taskDto.setDag(mocDag);
            transformerWsMessageDto.setTaskDto(taskDto);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            try(MockedStatic<ObsLoggerFactory> mockedStatic = mockStatic(ObsLoggerFactory.class);
                MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
                mockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
                ObsLogger obsLogger = mock(ObsLogger.class);
                when(obsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
                Map<String, TapTableMap<String, TapTable>> tapTableMapHashMap = new HashMap<>();
                dagDataEngineService = new DAGDataEngineServiceImpl(transformerWsMessageDto,taskService,tapTableMapHashMap,clientMongoOperator);
                ReflectionTestUtils.setField(dagDataEngineService,"metadataMap",metadataMap);
                ConfigurationCenter configurationCenter = mock(ConfigurationCenter.class);
                beanUtilMockedStatic.when(()->BeanUtil.getBean(any())).thenReturn(configurationCenter);
                when(configurationCenter.getConfig(any())).thenReturn("agentId");
                List<Node> nodes = new ArrayList<>();
                LogCollectorNode logCollectorNode = new LogCollectorNode();
                logCollectorNode.setId("LogCollectorNode");
                logCollectorNode.setLogCollectorConnConfigs(new HashMap<>());
                nodes.add(logCollectorNode);
                when(mocDag.getNodes()).thenReturn(nodes);
                dagDataEngineService.initializeModel(true);
                Assertions.assertEquals(2,tapTableMapHashMap.get("LogCollectorNode").size());
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
