package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Array;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LdpServiceImplTest {

    LdpServiceImpl ldpService;
    AgentGroupService agentGroupService;
    WorkerService workerService;

    UserDetail user;
    MetadataInstancesService metadataInstancesService;

    @BeforeEach
    void init() {
        ldpService = mock(LdpServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(ldpService, "agentGroupService", agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(ldpService, "workerService", workerService);
        metadataInstancesService = mock(MetadataInstancesServiceImpl.class);
        ReflectionTestUtils.setField(ldpService, "metadataInstancesService", metadataInstancesService);
        user = mock(UserDetail.class);
    }

    @Nested
    class FindAgentTest {
        DataSourceConnectionDto connectionDto;
        List<String> processNodeListWithGroup;
        List<Worker> availableAgent;
        Worker worker;

        @BeforeEach
        void init() {
            availableAgent = new ArrayList<>();
            processNodeListWithGroup = new ArrayList<>();
            processNodeListWithGroup.add("id");

            worker = mock(Worker.class);
            when(worker.getProcessId()).thenReturn("id");
            availableAgent.add(worker);

            connectionDto = mock(DataSourceConnectionDto.class);
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());

            when(agentGroupService.getProcessNodeListWithGroup(connectionDto, user)).thenReturn(processNodeListWithGroup);
            when(workerService.findAvailableAgent(user)).thenReturn(availableAgent);

            when(ldpService.findAgent(connectionDto, user)).thenCallRealMethod();
        }
        String assertVerify(int getProcessIdTimes, int getProcessNodeListWithGroupTimes, int getAccessNodeTypeTimes) {
            String agent = ldpService.findAgent(connectionDto, user);
            verify(agentGroupService, times(getProcessNodeListWithGroupTimes)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionDto, times(getAccessNodeTypeTimes)).getAccessNodeType();
            verify(workerService, times(1)).findAvailableAgent(user);
            verify(worker, times(getProcessIdTimes)).getProcessId();
            return agent;
        }

        @Test
        void testNormal() {
            String nodeId = assertVerify(1, 1, 1);
            assertEquals("id", nodeId);
        }
        @Test
        void testAvailableAgentIsNull() {
            when(workerService.findAvailableAgent(user)).thenReturn(null);
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull( nodeId);
        }
        @Test
        void testAvailableAgentIsEmpty() {
            when(workerService.findAvailableAgent(user)).thenReturn(new ArrayList<>());
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull( nodeId);
        }
        @Test
        void testProcessIdsEmpty() {
            Worker w = null;
            ArrayList<Worker> ws = new ArrayList<>();
            ws.add(w);
            when(workerService.findAvailableAgent(user)).thenReturn(ws);
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull(nodeId);
        }
        @Test
        void testIsNotManually() {
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            String nodeId = assertVerify(1, 1, 1);
            assertEquals("id", nodeId);
        }
        @Test
        void testProcessNodeListWithGroupIsEmpty() {
            when(agentGroupService.getProcessNodeListWithGroup(connectionDto, user)).thenReturn(new ArrayList<>());
            String nodeId = assertVerify(1, 1, 1);
            assertEquals("id", nodeId);
        }
        @Test
        void testProcessNodeListWithGroupNotContainsId() {
            when(worker.getProcessId()).thenReturn("ids");
            String nodeId = assertVerify(1, 1, 1);
            Assertions.assertNull(nodeId);
        }
    }
    @Nested
    class CleanLdpMeta{
        @Test
        void test_main(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            metadataInstancesDto1.setAncestorsName("test");
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(1)).deleteAll(any(),any());
        }

        @Test
        void test_tagIdIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setAncestorsName("test");
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

        @Test
        void test_SourceIdIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            metadataInstancesDto1.setAncestorsName("test");
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

        @Test
        void test_AncestorsNameIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

    }
    @Nested
    class ldpTableStatusTest{
        private TaskService taskService;

        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(ldpService,"taskService",taskService);
        }

        @Test
        void testLdpTableStatus_FDM_RunningState() {
            String connectionId = "connection1";
            List<String> tableNames = Arrays.asList("table1", "table2");
            String ldpType = TaskDto.LDP_TYPE_FDM;
            UserDetail user = mock(UserDetail.class);

            TaskDto taskDto = mock(TaskDto.class);
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);
            when(taskDto.getLdpNewTables()).thenReturn(Collections.emptyList());

            DatabaseNode databaseNode = mock(DatabaseNode.class);
            when(databaseNode.getConnectionId()).thenReturn(connectionId);
            SyncObjects syncObjects = mock(SyncObjects.class);
            LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
            tableNameRelation.put("FDM_table1","table1");
            when(syncObjects.getTableNameRelation()).thenReturn(tableNameRelation);
            when(databaseNode.getSyncObjects()).thenReturn(Collections.singletonList(syncObjects));

            LinkedList<DatabaseNode> targetNode = new LinkedList<>();
            targetNode.add(databaseNode);
            when(dag.getTargetNode()).thenReturn(targetNode);

            when(taskService.findAllDto(any(Query.class), eq(user)))
                    .thenReturn(Collections.singletonList(taskDto));
            when(metadataInstancesService.checkTableExist(anyString(), anyString(), eq(user)))
                    .thenReturn(true);

            doCallRealMethod().when(ldpService).ldpTableStatus(connectionId, tableNames, ldpType, user);
            Map<String, String> result = ldpService.ldpTableStatus(connectionId, tableNames, ldpType, user);

            assertEquals("running", result.get("table1"));
            assertEquals("noRunning", result.get("table2"));
        }

        @Test
        void testLdpTableStatus_FDM_NoRunningState() {
            String connectionId = "connection2";
            List<String> tableNames = Arrays.asList("table3");
            String ldpType = TaskDto.LDP_TYPE_FDM;
            UserDetail user = mock(UserDetail.class);

            TaskDto taskDto = mock(TaskDto.class);
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_STOP);
            when(taskDto.getLdpNewTables()).thenReturn(Collections.emptyList());

            DatabaseNode databaseNode = mock(DatabaseNode.class);
            when(databaseNode.getConnectionId()).thenReturn(connectionId);
            SyncObjects syncObjects = mock(SyncObjects.class);
            LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
            tableNameRelation.put("FDM_table3","table3");
            when(syncObjects.getTableNameRelation()).thenReturn(tableNameRelation);
            when(databaseNode.getSyncObjects()).thenReturn(Collections.singletonList(syncObjects));


            LinkedList<DatabaseNode> targetNode = new LinkedList<>();
            targetNode.add(databaseNode);
            when(dag.getTargetNode()).thenReturn(targetNode);

            when(taskService.findAllDto(any(Query.class), eq(user)))
                    .thenReturn(Collections.singletonList(taskDto));
            when(metadataInstancesService.checkTableExist(anyString(), anyString(), eq(user)))
                    .thenReturn(false);
            doCallRealMethod().when(ldpService).ldpTableStatus(connectionId, tableNames, ldpType, user);
            Map<String, String> result = ldpService.ldpTableStatus(connectionId, tableNames, ldpType, user);

            assertEquals("noRunning", result.get("table3"));
        }
        @Test
        void testLdpTableStatus_FDM_DoneState() {
            String connectionId = "connection2";
            List<String> tableNames = Arrays.asList("table3");
            String ldpType = TaskDto.LDP_TYPE_FDM;
            UserDetail user = mock(UserDetail.class);

            TaskDto taskDto = mock(TaskDto.class);
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_STOP);
            when(taskDto.getLdpNewTables()).thenReturn(Collections.emptyList());

            DatabaseNode databaseNode = mock(DatabaseNode.class);
            when(databaseNode.getConnectionId()).thenReturn(connectionId);
            SyncObjects syncObjects = mock(SyncObjects.class);
            LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
            tableNameRelation.put("FDM_table3","table3");
            when(syncObjects.getTableNameRelation()).thenReturn(tableNameRelation);
            when(databaseNode.getSyncObjects()).thenReturn(Collections.singletonList(syncObjects));


            LinkedList<DatabaseNode> targetNode = new LinkedList<>();
            targetNode.add(databaseNode);
            when(dag.getTargetNode()).thenReturn(targetNode);

            when(taskService.findAllDto(any(Query.class), eq(user)))
                    .thenReturn(Collections.singletonList(taskDto));
            when(metadataInstancesService.checkTableExist(anyString(), anyString(), eq(user)))
                    .thenReturn(true);
            doCallRealMethod().when(ldpService).ldpTableStatus(connectionId, tableNames, ldpType, user);
            Map<String, String> result = ldpService.ldpTableStatus(connectionId, tableNames, ldpType, user);

            assertEquals("done", result.get("table3"));
        }
    }
}