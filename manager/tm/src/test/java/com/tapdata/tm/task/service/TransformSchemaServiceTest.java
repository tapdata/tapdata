package com.tapdata.tm.task.service;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
}