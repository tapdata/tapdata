package com.tapdata.tm.task.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.utils.GZIPUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

class TransformSchemaServiceTest {
    private TransformSchemaService transformSchemaService;
    private DataSourceService dataSourceService;
    private MetadataInstancesService metadataInstancesService;
    private MetadataTransformerService metadataTransformerService;
    private DataSourceDefinitionService definitionService;
    MessageQueueService messageQueueService;
    AgentGroupService agentGroupService;
    WorkerService workerService;
    UserDetail user;
    TaskService taskService;

    @BeforeEach
    void buildTransformSchemaService(){
        transformSchemaService = mock(TransformSchemaService.class);
        dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(transformSchemaService,"dataSourceService",dataSourceService);
        metadataInstancesService = mock(MetadataInstancesService.class);
        ReflectionTestUtils.setField(transformSchemaService,"metadataInstancesService",metadataInstancesService);
        metadataTransformerService = mock(MetadataTransformerService.class);
        ReflectionTestUtils.setField(transformSchemaService,"metadataTransformerService",metadataTransformerService);
        definitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(transformSchemaService,"definitionService",definitionService);
        messageQueueService = mock(MessageQueueService.class);
        ReflectionTestUtils.setField(transformSchemaService,"messageQueueService",messageQueueService);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(transformSchemaService,"agentGroupService",agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(transformSchemaService,"workerService",workerService);
        taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(transformSchemaService,"taskService",taskService);
        user = mock(UserDetail.class);
    }

    @Nested
    class getTransformParamTest {
        @Test
        void testGetTransformParamSimple(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            List<String> includes = new ArrayList<>();
            boolean allParam = true;
            doNothing().when(dag).addNodeEventListener(any());
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            TransformerWsMessageDto actual = transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
        @Test
        void testGetTransformParamWithTableNode(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            UserDetail user = mock(UserDetail.class);
            List<String> includes = new ArrayList<>();
            boolean allParam = false;
            doNothing().when(dag).addNodeEventListener(any());
            List<Node> dagNodes = new ArrayList<>();
            Node node1 = new TableNode();
            List<DataSourceConnectionDto> dataSources = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = mock(DataSourceConnectionDto.class);
            when(dataSourceConnectionDto.getId()).thenReturn(new ObjectId());
            when(dataSourceConnectionDto.getDatabase_type()).thenReturn("mongodb");
            List<DataSourceDefinitionDto> definitionDtos = new ArrayList<>();
            DataSourceDefinitionDto dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            definitionDtos.add(dataSourceDefinitionDto);
            when(dataSourceDefinitionDto.getType()).thenReturn("mongodb");
            when(definitionService.getByDataSourceType(anyList(),any())).thenReturn(definitionDtos);
            when(metadataInstancesService.getQualifiedNameByNodeId(node1, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString())).thenReturn("qualified_name");
            dataSources.add(dataSourceConnectionDto);
            Map<String, DataSourceConnectionDto> dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
            node1.setDag(mock(DAG.class));
            for (String s : dataSourceMap.keySet()) {
                ((TableNode)node1).setConnectionId(s);
            }
            dagNodes.add(node1);
            List<String> connectionIds = dagNodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());
            Criteria idCriteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(idCriteria);
            when(dataSourceService.findAll(query)).thenReturn(dataSources);
            when(dag.getNodes()).thenReturn(dagNodes);
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
        @Test
        void testGetTransformParamWithDatabaseNode(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            List<String> includes = new ArrayList<>();
            boolean allParam = false;
            doNothing().when(dag).addNodeEventListener(any());
            List<Node> dagNodes = new ArrayList<>();
            Node node1 = new DatabaseNode();
            List<DataSourceConnectionDto> dataSources = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = mock(DataSourceConnectionDto.class);
            when(dataSourceConnectionDto.getId()).thenReturn(new ObjectId());
            when(dataSourceConnectionDto.getDatabase_type()).thenReturn("mongodb");
            List<DataSourceDefinitionDto> definitionDtos = new ArrayList<>();
            DataSourceDefinitionDto dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            definitionDtos.add(dataSourceDefinitionDto);
            when(dataSourceDefinitionDto.getType()).thenReturn("mongodb");
            when(definitionService.getByDataSourceType(anyList(),any())).thenReturn(definitionDtos);
            when(metadataInstancesService.getQualifiedNameByNodeId(node1, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString())).thenReturn("qualified_name");
            dataSources.add(dataSourceConnectionDto);
            Map<String, DataSourceConnectionDto> dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
            node1.setDag(mock(DAG.class));
            for (String s : dataSourceMap.keySet()) {
                ((DatabaseNode)node1).setConnectionId(s);
            }
            dagNodes.add(node1);
            List<String> connectionIds = dagNodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());
            Criteria idCriteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(idCriteria);
            when(dataSourceService.findAll(query)).thenReturn(dataSources);
            when(dag.getNodes()).thenReturn(dagNodes);
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
    }

    @Nested
    class SendTransformerTest {
        TransformerWsMessageDto wsMessageDto;
        TaskDto taskDto;
        Map<String, DataSourceDefinitionDto> definitionDtoMap;
        DataSourceDefinitionDto dataSourceDefinitionDto;
        List<Worker> availableAgent;
        Worker worker;

        Base64.Encoder encoder;

        @BeforeEach
        void init() {
            encoder = mock(Base64.Encoder.class);

            wsMessageDto = mock(TransformerWsMessageDto.class);
            taskDto = mock(TaskDto.class);
            when(wsMessageDto.getTaskDto()).thenReturn(taskDto);
            definitionDtoMap = new HashMap<>();
            dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            doNothing().when(dataSourceDefinitionDto).setProperties(null);
            definitionDtoMap.put("key", dataSourceDefinitionDto);
            when(wsMessageDto.getDefinitionDtoMap()).thenReturn(definitionDtoMap);

            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());

            doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
            doCallRealMethod().when(transformSchemaService).sendTransformer(wsMessageDto, user);
            worker = mock(Worker.class);
            availableAgent = new ArrayList<>();
            availableAgent.add(worker);
            when(worker.getProcessId()).thenReturn("id");
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(mock(List.class));
            when(workerService.findAvailableAgentByAccessNode(any(UserDetail.class), anyList())).thenReturn(availableAgent);
            when(workerService.findAvailableAgent(user)).thenReturn(availableAgent);
        }

        void assertVerify(int definitionDtoMapTimes,String json,
                          int setProperties, int getAccessNodeType,
                          int getProcessNodeListWithGroup,
                          int findAvailableAgentByAccessNode,
                          int findAvailableAgent,
                          int getProcessId) {
            when(encoder.encode(any(byte[].class))).thenReturn(new byte[0]);
            try(MockedStatic<JsonUtil> ju = mockStatic(JsonUtil.class);
                MockedStatic<GZIPUtil> gu = mockStatic(GZIPUtil.class);
                MockedStatic<Base64> b6 = mockStatic(Base64.class)) {
                ju.when(() -> JsonUtil.toJsonUseJackson(wsMessageDto)).thenReturn(json);
                gu.when(() -> GZIPUtil.gzip(any(byte[].class))).thenReturn(new byte[0]);
                b6.when(Base64::getEncoder).thenReturn(encoder);
                Assertions.assertDoesNotThrow(() -> transformSchemaService.sendTransformer(wsMessageDto, user));
                verify(wsMessageDto, times(1)).getTaskDto();
                verify(wsMessageDto, times(definitionDtoMapTimes)).getDefinitionDtoMap();
                verify(dataSourceDefinitionDto, times(setProperties * definitionDtoMap.size())).setProperties(null);
                verify(taskDto, times(getAccessNodeType)).getAccessNodeType();
                verify(agentGroupService, times(getProcessNodeListWithGroup)).getProcessNodeListWithGroup(taskDto, user);
                verify(workerService, times(findAvailableAgentByAccessNode)).findAvailableAgentByAccessNode(any(UserDetail.class), anyList());
                verify(workerService, times(findAvailableAgent)).findAvailableAgent(user);
                verify(worker, times(getProcessId)).getProcessId();
            }
        }

        @Test
        void testNormal() {
            assertVerify(1, "{}",
                    1, 2,
                    1,
                    1,
                    0,
                    1);
        }

        @Test
        void testTaskDtoIsNull() {
            when(wsMessageDto.getTaskDto()).thenReturn(null);
            assertVerify(0, "{}",
                    0, 0,
                    0,
                    0,
                    0,
                    0);
        }

        @Test
        void testDataSourceDefinitionMapIsNull() {
            when(wsMessageDto.getDefinitionDtoMap()).thenReturn(null);
            assertVerify(1, "{}",
                    0, 2,
                    1,
                    1,
                    0,
                    1);
        }

        @Test
        void testAccessNodeTypeIsBlank() {
            when(taskDto.getAccessNodeType()).thenReturn(null);
            assertVerify(1, "{}",
                    1, 1,
                    0,
                    0,
                    1,
                    1);
        }

        @Test
        void testAccessNodeTypeNotBlankButNotIsManually() {
            when(taskDto.getAccessNodeType()).thenReturn("accessNodeType");
            assertVerify(1, "{}",
                    1, 2,
                    0,
                    0,
                    1,
                    1);
        }

        @Test
        void testAvailableAgentIsEmpty() {
            when(workerService.findAvailableAgentByAccessNode(any(UserDetail.class), anyList())).thenReturn(null);
            assertVerify(1, "{}",
                    1, 2,
                    1,
                    1,
                    0,
                    0);
        }

        @Test
        void testWsMessageDtoJsonIsNull() {
            assertVerify(1, null,
                    1, 2,
                    1,
                    1,
                    0,
                    1);
        }
    }

    @Nested
    class TransformSchemaTest {
        TaskDto taskDto;
        DAG dag;
        List<Node> targets;
        TableNode target;
        CacheNode cacheNode;
        ObjectId taskId;
        @BeforeEach
        void init() {
            taskId = mock(ObjectId.class);
            target = mock(TableNode.class);
            when(target.getType()).thenReturn(NodeEnum.database.name());
            when(target.getNeedDynamicTableName()).thenReturn(true);
            targets = new ArrayList<>();
            targets.add(target);
            cacheNode = mock(CacheNode.class);
            when(cacheNode.getType()).thenReturn(NodeEnum.mem_cache.name());
            targets.add(cacheNode);

            taskDto = mock(TaskDto.class);
            dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(dag.getTargets()).thenReturn(targets);
            when(taskDto.getId()).thenReturn(taskId);
            when(taskDto.getSyncType()).thenReturn(TaskDto.SYNC_TYPE_SYNC);
        }
        @Test
        void testTransformSchema() {
            TransformerWsMessageDto transformParam = new TransformerWsMessageDto();
            transformParam.setMetadataInstancesDtoList(new ArrayList<>());
            transformParam.setOptions(mock(DAG.Options.class));
            doCallRealMethod().when(transformSchemaService).transformSchema(taskDto, transformParam, false, user);
            transformSchemaService.transformSchema(taskDto, transformParam, false, user);
            verify(taskService).update(Query.query(Criteria.where("_id").is(taskDto.getId())), Update.update("transformUuid", transformParam.getOptions().getUuid()).set("transformed", false));
        }

        @Test
        void transformSchema() {
            when(taskService.checkExistById(taskId)).thenReturn(taskDto);
            doCallRealMethod().when(transformSchemaService).transformSchema(dag, user, taskId);
            assertDoesNotThrow(() -> transformSchemaService.transformSchema(dag, user, taskId));
        }

        @Nested
        class TransformSchemaBeforeDynamicTableNameTest {
            @BeforeEach
            void init() {
                doNothing().when(transformSchemaService).transformSchemaAndUpdateTask(taskDto, user);
                doCallRealMethod().when(transformSchemaService).transformSchemaBeforeDynamicTableName(taskDto, user);
            }

            @Test
            void testNormal() {
                transformSchemaService.transformSchemaBeforeDynamicTableName(taskDto, user);
                verify(taskDto).getSyncType();
                verify(taskDto).getDag();
                verify(dag).getTargets();
                verify(target).getNeedDynamicTableName();
                verify(transformSchemaService).transformSchemaAndUpdateTask(taskDto, user);
            }
            @Test
            void testTaskNotSync() {
                when(taskDto.getSyncType()).thenReturn(TaskDto.SYNC_TYPE_MIGRATE);
                transformSchemaService.transformSchemaBeforeDynamicTableName(taskDto, user);
                verify(taskDto).getSyncType();
                verify(taskDto, times(0)).getDag();
                verify(dag, times(0)).getTargets();
                verify(target, times(0)).getNeedDynamicTableName();
                verify(transformSchemaService, times(0)).transformSchemaAndUpdateTask(taskDto, user);
            }
            @Test
            void testNotNeedDynamicTableName() {
                when(target.getNeedDynamicTableName()).thenReturn(false);
                transformSchemaService.transformSchemaBeforeDynamicTableName(taskDto, user);
                verify(taskDto).getSyncType();
                verify(taskDto).getDag();
                verify(dag).getTargets();
                verify(target).getNeedDynamicTableName();
                verify(transformSchemaService, times(0)).transformSchemaAndUpdateTask(taskDto, user);
            }
        }

        @Nested
        class TransformSchemaAndUpdateTaskTest {

            @BeforeEach
            void init() {
                when(dag.getNodes()).thenReturn(targets);
                doNothing().when(transformSchemaService).transformSchema(dag, user, taskId);
                doCallRealMethod().when(transformSchemaService).transformSchemaAndUpdateTask(taskDto, user);
                when(taskService.updateById(any(ObjectId.class), any(Update.class), any(UserDetail.class))).thenReturn(mock(UpdateResult.class));
            }

            @Test
            void testNormal() {
                transformSchemaService.transformSchemaAndUpdateTask(taskDto, user);
                verify(taskDto).getDag();
                verify(taskDto).getId();
                verify(transformSchemaService).transformSchema(dag, user, taskId);
                verify(dag).getNodes();
                verify(target).getType();
                verify(cacheNode).getType();
                verify(taskService, times(0)).updateById(any(ObjectId.class), any(Update.class), any(UserDetail.class));
            }

            @Test
            void testContainsJs() {
                when(cacheNode.getType()).thenReturn(NodeEnum.join_processor.name());
                transformSchemaService.transformSchemaAndUpdateTask(taskDto, user);
                verify(taskDto).getDag();
                verify(taskDto).getId();
                verify(transformSchemaService).transformSchema(dag, user, taskId);
                verify(dag).getNodes();
                verify(target).getType();
                verify(cacheNode).getType();
                verify(taskService).updateById(any(ObjectId.class), any(Update.class), any(UserDetail.class));
            }
        }
        @Nested
        class getLogCollectorMetadataInstancesDtoTest{
            @Test
            void testConnConfigMapIsNotNull(){
                DAG dag1 = mock(DAG.class);
                List<Node> nodes = new ArrayList<>();
                LogCollectorNode logCollectorNode = new LogCollectorNode();
                logCollectorNode.setId("LogCollectorNode");
                Map<String, LogCollecotrConnConfig> connConfigMap = new HashMap<>();
                LogCollecotrConnConfig logCollecotrConnConfig1 = new LogCollecotrConnConfig();
                logCollecotrConnConfig1.setConnectionId("c_1");
                logCollecotrConnConfig1.setTableNames(Arrays.asList("test1"));
                LogCollecotrConnConfig logCollecotrConnConfig2 = new LogCollecotrConnConfig();
                logCollecotrConnConfig2.setConnectionId("c_2");
                logCollecotrConnConfig2.setTableNames(Arrays.asList("test2"));
                connConfigMap.put("c_1",logCollecotrConnConfig1);
                connConfigMap.put("c_2",logCollecotrConnConfig2);
                logCollectorNode.setLogCollectorConnConfigs(connConfigMap);
                nodes.add(logCollectorNode);
                when(dag1.getNodes()).thenReturn(nodes);
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
                when(metadataInstancesService.findAllDto(any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2));
                List<MetadataInstancesDto> result = new ArrayList<>();
                UserDetail userDetail = mock(UserDetail.class);
                doCallRealMethod().when(transformSchemaService).getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
                transformSchemaService.getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
                Assertions.assertEquals(2,result.size());
            }
        }

        @Test
        void testConnConfigMapIsNull(){
            DAG dag1 = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            LogCollectorNode logCollectorNode = new LogCollectorNode();
            logCollectorNode.setId("LogCollectorNode");
            logCollectorNode.setConnectionIds(Arrays.asList("c_1"));
            logCollectorNode.setTableNames(Arrays.asList("test1"));
            nodes.add(logCollectorNode);
            when(dag1.getNodes()).thenReturn(nodes);
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("LogCollectorNode");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            when(metadataInstancesService.findAllDto(any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1));
            List<MetadataInstancesDto> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(transformSchemaService).getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
            transformSchemaService.getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
            Assertions.assertEquals(1,result.size());
        }
        @Test
        void testConnConfigMapIsEmpty(){
            DAG dag1 = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            LogCollectorNode logCollectorNode = new LogCollectorNode();
            logCollectorNode.setId("LogCollectorNode");
            List<String> connectionIds = new ArrayList<>();
            logCollectorNode.setConnectionIds(connectionIds);
            logCollectorNode.setTableNames(Arrays.asList("test1"));
            logCollectorNode.setLogCollectorConnConfigs(new HashMap<>());
            nodes.add(logCollectorNode);
            when(dag1.getNodes()).thenReturn(nodes);
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
            metadataInstancesDto1.setNodeId("LogCollectorNode");
            metadataInstancesDto1.setOriginalName("name1");
            metadataInstancesDto1.setQualifiedName("test1");
            metadataInstancesDto1.setMetaType("table");
            when(metadataInstancesService.findAllDto(any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1));
            List<MetadataInstancesDto> result = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(transformSchemaService).getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
            transformSchemaService.getLogCollectorMetadataInstancesDto(dag1,userDetail,result);
            Assertions.assertEquals(0,result.size());
        }
    }
    @Nested
    class GetTableNodeSameMetadataInstancesTest{
        @Test
        void test_main(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            TableNode tableNode1 = new TableNode();
            tableNode1.setId("tableNode1");
            TableNode tableNode2 = new TableNode();
            tableNode2.setId("tableNode2");
            nodes.add(tableNode1);
            nodes.add(tableNode2);
            when(dag.getSourceNodes()).thenReturn(nodes);
            when(metadataInstancesService.getQualifiedNameByNodeId(any(),any(),any(),any(),any())).thenReturn("test");
            taskDto.setDag(dag);
            doCallRealMethod().when(transformSchemaService).getTableNodeSameMetadataInstances(any(),any());
            Map<String,List<String>> result = transformSchemaService.getTableNodeSameMetadataInstances(taskDto,mock(UserDetail.class));
            result.get("test").forEach(s -> Assertions.assertTrue(s.equals("tableNode1") || s.equals("tableNode2")));
        }

        @Test
        void test_tableNodeIsEmpty(){
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            DatabaseNode databaseNode = new DatabaseNode();
            nodes.add(databaseNode);
            when(dag.getSourceNodes()).thenReturn(nodes);
            when(metadataInstancesService.getQualifiedNameByNodeId(any(),any(),any(),any(),any())).thenReturn("test");
            taskDto.setDag(dag);
            doCallRealMethod().when(transformSchemaService).getTableNodeSameMetadataInstances(any(),any());
            Map<String,List<String>> result = transformSchemaService.getTableNodeSameMetadataInstances(taskDto,mock(UserDetail.class));
            Assertions.assertEquals(0,result.size());
        }

    }
    @Nested
    class transformerResultTest {
        UserDetail user;
        TransformerWsMessageResult result;
        boolean saveHistory;
        LdpService ldpService;
        @BeforeEach
        void beforeEach() {
            user = mock(UserDetail.class);
            result = mock(TransformerWsMessageResult.class);
            saveHistory = true;
            ldpService = mock(LdpService.class);
            ReflectionTestUtils.setField(transformSchemaService, "ldpService", ldpService);
        }
        @Test
        void testTransformerResultSimple() {
            when(result.getTaskId()).thenReturn("6720c4a18c6b586b9e1b493b");
            when(taskService.checkExistById(any(ObjectId.class), anyString())).thenReturn(mock(TaskDto.class));
            doCallRealMethod().when(transformSchemaService).transformerResult(user, result, saveHistory);
            transformSchemaService.transformerResult(user, result, saveHistory);
            verify(ldpService, new Times(1)).afterLdpTask(anyString(), any(UserDetail.class));
        }
    }

}