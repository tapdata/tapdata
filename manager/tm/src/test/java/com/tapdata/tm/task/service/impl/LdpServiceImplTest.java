package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LdpServiceImplTest {

    LdpServiceImpl ldpService;
    AgentGroupService agentGroupService;
    WorkerService workerService;

    UserDetail user;
    MetadataInstancesService metadataInstancesService;
    private TaskService taskService;

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
        taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(ldpService,"taskService",taskService);
        // Ensure protected criteria method returns non-null in tests
        when(ldpService.fdmTaskCriteria(anyString())).thenCallRealMethod();
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
        void testLdpTableStatus_FDM_CompleteState() {
            String connectionId = "connection2";
            List<String> tableNames = Arrays.asList("table3");
            String ldpType = TaskDto.LDP_TYPE_FDM;
            UserDetail user = mock(UserDetail.class);

            TaskDto taskDto = mock(TaskDto.class);
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_COMPLETE);
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
            doCallRealMethod().when(ldpService).ldpTableStatus(connectionId, tableNames, ldpType, user);
            Map<String, String> result = ldpService.ldpTableStatus(connectionId, tableNames, ldpType, user);

            assertEquals("running", result.get("table3"));
        }
        @Test
        void testLdpTableStatus_MDM_CompleteState() {
            String connectionId = "connection2";
            List<String> tableNames = Arrays.asList("table3");
            String ldpType = TaskDto.LDP_TYPE_MDM;
            UserDetail user = mock(UserDetail.class);

            TaskDto taskDto = mock(TaskDto.class);
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getStatus()).thenReturn(TaskDto.STATUS_COMPLETE);
            when(taskDto.getLdpNewTables()).thenReturn(Collections.emptyList());

            TableNode tableNode = mock(TableNode.class);
            when(tableNode.getConnectionId()).thenReturn(connectionId);
            when(tableNode.getTableName()).thenReturn("table3");

            List<Node> targetNode = new LinkedList<>();
            targetNode.add(tableNode);
            when(dag.getTargets()).thenReturn(targetNode);

            when(taskService.findAllDto(any(Query.class), eq(user)))
                    .thenReturn(Collections.singletonList(taskDto));
            doCallRealMethod().when(ldpService).ldpTableStatus(connectionId, tableNames, ldpType, user);
            Map<String, String> result = ldpService.ldpTableStatus(connectionId, tableNames, ldpType, user);

            assertEquals("running", result.get("table3"));
        }
    }

    @Nested
    class afterLdpTaskTest {
        LiveDataPlatformService liveDataPlatformService;
        @BeforeEach
        void beforeEach() {
            liveDataPlatformService = mock(LiveDataPlatformService.class);
            ReflectionTestUtils.setField(ldpService, "liveDataPlatformService", liveDataPlatformService);
        }
        @Test
        void testAfterLdpTaskSimple() {
            user = mock(UserDetail.class);
            String taskId = "6720c4a18c6b586b9e1b493b";
            when(taskService.findByTaskId(any(),any())).thenReturn(mock(TaskDto.class));
            doCallRealMethod().when(ldpService).afterLdpTask(taskId, user);
            ldpService.afterLdpTask(taskId, user);
        }
    }

    @Test
    void fuzzySearchTest() {
        String key = "TEST";
        List<String> connectType = new ArrayList<>();
        UserDetail user = mock(UserDetail.class);
        Pattern pattern = Pattern.compile(key, Pattern.CASE_INSENSITIVE);
        Criteria criteria = Criteria.where("original_name").regex(pattern).and("sourceType").is(SourceTypeEnum.SOURCE.name());
        doCallRealMethod().when(ldpService).fuzzySearch(key, connectType, user);
        ldpService.fuzzySearch(key, connectType, user);
        verify(ldpService, new Times(1)).getLdpFuzzySearchVos(user, criteria);
    }

    @Test
    void getLdpFuzzySearchVosTest() {
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(ldpService, "dataSourceService", dataSourceService);
        UserDetail user = mock(UserDetail.class);
        Criteria criteria = Criteria.where("original_name").regex(Pattern.compile("TEST", Pattern.CASE_INSENSITIVE)).and("sourceType").is(SourceTypeEnum.SOURCE.name());
        doCallRealMethod().when(ldpService).getLdpFuzzySearchVos(user, criteria);
        ldpService.getLdpFuzzySearchVos(user, criteria);
        verify(dataSourceService, new Times(1)).findAll(any(Query.class));
    }

    @Nested
    class CreateFdmTaskTest {
        private TaskSaveService taskSaveService;
        private TaskDto inputTask;
        private TaskDto existingTask;
        private DAG dag;
        private DatabaseNode sourceNode;
        private DatabaseNode targetNode;
        private UserDetail user;
        private String connectionId;
        private String fdmConnId;

        @BeforeEach
        void setUp() {
            taskSaveService = mock(TaskSaveService.class);
            ReflectionTestUtils.setField(ldpService, "taskSaveService", taskSaveService);

            user = mock(UserDetail.class);
            connectionId = "conn123";
            fdmConnId = "fdmConn456";

            // 设置输入任务
            inputTask = createMockTask();
            existingTask = createMockExistingTask();

            // 设置真实方法调用
            doCallRealMethod().when(ldpService).createFdmTask(any(TaskDto.class), anyBoolean(), any(UserDetail.class));

            // 设置依赖方法的模拟
            when(ldpService.checkFdmTask(any(TaskDto.class), any(UserDetail.class))).thenReturn(fdmConnId);
            when(ldpService.generateLdpTaskType(anyString(), any(UserDetail.class))).thenReturn(TaskDto.TYPE_INITIAL_SYNC_CDC);
            when(ldpService.fdmTaskCriteria(anyString())).thenReturn(Criteria.where("connectionIds").in(connectionId));
            doNothing().when(ldpService).flushPrefix(any(DAG.class), any(DAG.class));
            doNothing().when(ldpService).mergeAllTable(any(UserDetail.class), anyString(), any(TaskDto.class), any());
            when(ldpService.createNew(any(TaskDto.class), any(DAG.class), any(TaskDto.class))).thenReturn(inputTask);
            doNothing().when(ldpService).repeatTable(any(), anyString(), anyString(), any(UserDetail.class));
            doNothing().when(ldpService).createFdmTags(any(TaskDto.class), any(UserDetail.class));
            doNothing().when(taskSaveService).supplementAlarm(any(TaskDto.class), any(UserDetail.class));
        }



        @Test
        void testCreateFdmTask_WithExistingTask_TableNamesContained_ShouldThrowException() {
            // Given
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(existingTask);

            // 设置新任务的表名被现有任务包含
            sourceNode.setTableNames(Arrays.asList("table1", "table2"));
            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setTableNames(Arrays.asList("table1", "table2", "table3"));

            // When & Then
            assertThrows(BizException.class, () -> {
                ldpService.createFdmTask(inputTask, false, user);
            });
        }


        @Test
        void testCreateFdmTask_WithExistingTask_NewExpressionMode() {
            // Given
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(existingTask);
            when(taskService.updateById(any(TaskDto.class), any(UserDetail.class))).thenReturn(existingTask);

            // 设置新任务为表达式模式，现有任务不是
            sourceNode.setMigrateTableSelectType("expression");
            sourceNode.setTableExpression("new_*");

            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setMigrateTableSelectType("manual");
            existingSourceNode.setTableExpression(null);

            // 设置不重叠的表名
            sourceNode.setTableNames(Arrays.asList("new_table1"));
            existingSourceNode.setTableNames(Arrays.asList("existing_table1"));

            // When
            TaskDto result = ldpService.createFdmTask(inputTask, false, user);

            // Then
            assertNotNull(result);
            verify(ldpService).mergeAllTable(user, connectionId, inputTask, Arrays.asList("existing_table1"));
            verify(taskService).updateById(any(TaskDto.class), any(UserDetail.class));
        }

        @Test
        void testCreateFdmTask_WithExistingTask_CreateNew() {
            // Given
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(existingTask);
            when(taskService.updateById(any(TaskDto.class), any(UserDetail.class))).thenReturn(inputTask);

            // 设置非表达式模式
            sourceNode.setMigrateTableSelectType("manual");
            sourceNode.setTableExpression(null);

            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setMigrateTableSelectType("manual");
            existingSourceNode.setTableExpression(null);

            // 设置不重叠的表名
            sourceNode.setTableNames(Arrays.asList("new_table1"));
            existingSourceNode.setTableNames(Arrays.asList("existing_table1"));

            // When
            TaskDto result = ldpService.createFdmTask(inputTask, false, user);

            // Then
            assertNotNull(result);
            verify(ldpService).createNew(inputTask, dag, existingTask);
            verify(taskService).updateById(any(TaskDto.class), any(UserDetail.class));
        }

        @Test
        void testCreateFdmTask_TaskTypeError_ShouldThrowException() {
            // Given
            when(ldpService.generateLdpTaskType(anyString(), any(UserDetail.class))).thenReturn("DIFFERENT_TYPE");
            inputTask.setType("ORIGINAL_TYPE");

            // When & Then
            assertThrows(BizException.class, () -> {
                ldpService.createFdmTask(inputTask, false, user);
            });
        }

        @Test
        void testCreateFdmTask_WithExistingTask_RunningStatus_WithStart() {
            // Given
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(existingTask);
            when(taskService.updateById(any(TaskDto.class), any(UserDetail.class))).thenReturn(existingTask);
            existingTask.setStatus(TaskDto.STATUS_RUNNING);

            // 设置不重叠的表名
            sourceNode.setTableNames(Arrays.asList("new_table1"));
            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setTableNames(Arrays.asList("existing_table1"));

            // When
            TaskDto result = ldpService.createFdmTask(inputTask, true, user);

            // Then
            assertNotNull(result);
            verify(taskService).pause(existingTask, user, false, true);
            verify(taskService, never()).start(any(TaskDto.class), any(UserDetail.class), anyString());
        }

        @Test
        void testCreateFdmTask_WithExistingTask_NotRunningStatus_WithStart() {
            // Given
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(existingTask);
            when(taskService.updateById(any(TaskDto.class), any(UserDetail.class))).thenReturn(existingTask);
            existingTask.setStatus(TaskDto.STATUS_STOP);

            // 设置不重叠的表名
            sourceNode.setTableNames(Arrays.asList("new_table1"));
            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setTableNames(Arrays.asList("existing_table1"));

            // When
            TaskDto result = ldpService.createFdmTask(inputTask, true, user);

            // Then
            assertNotNull(result);
            verify(taskService).start(existingTask, user, "00");
            verify(taskService, never()).pause(any(TaskDto.class), any(UserDetail.class), anyBoolean(), anyBoolean());
        }

        @Test
        void testCreateFdmTask_FallbackToCloneTask() {
            // Given
            // 第一次查询返回null，第二次查询返回Clone任务
            when(taskService.findOne(any(Query.class), any(UserDetail.class)))
                .thenReturn(null)
                .thenReturn(existingTask);
            when(taskService.updateById(any(TaskDto.class), any(UserDetail.class))).thenReturn(existingTask);

            // 设置不重叠的表名
            sourceNode.setTableNames(Arrays.asList("new_table1"));
            DatabaseNode existingSourceNode = (DatabaseNode) existingTask.getDag().getSources().get(0);
            existingSourceNode.setTableNames(Arrays.asList("existing_table1"));

            // When
            TaskDto result = ldpService.createFdmTask(inputTask, false, user);

            // Then
            assertNotNull(result);
            verify(taskService, times(2)).findOne(any(Query.class), any(UserDetail.class));
            verify(taskService).updateById(any(TaskDto.class), any(UserDetail.class));
        }


        private TaskDto createMockTask() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
            task.setCrontabExpression("0 0 12 * * ?");
            task.setCrontabExpressionFlag(true);

            // 创建DAG
            dag = mock(DAG.class);

            // 创建源节点
            sourceNode = new DatabaseNode();
            sourceNode.setId("source1");
            sourceNode.setConnectionId(connectionId);
            sourceNode.setTableNames(Arrays.asList("table1", "table2"));
            sourceNode.setMigrateTableSelectType("manual");

            // 创建目标节点
            targetNode = new DatabaseNode();
            targetNode.setId("target1");
            targetNode.setConnectionId("targetConn");

            SyncObjects syncObjects = new SyncObjects();
            syncObjects.setObjectNames(Arrays.asList("target_table1", "target_table2"));
            targetNode.setSyncObjects(Arrays.asList(syncObjects));

            when(dag.getSources()).thenReturn(Arrays.asList(sourceNode));
            when(dag.getTargets()).thenReturn(Arrays.asList(targetNode));

            task.setDag(dag);
            return task;
        }

        private TaskDto createMockExistingTask() {
            TaskDto existingTask = new TaskDto();
            existingTask.setId(new ObjectId());
            existingTask.setFdmMain(true);
            existingTask.setStatus(TaskDto.STATUS_STOP);

            // 创建现有任务的DAG
            DAG existingDag = mock(DAG.class);

            // 创建现有源节点
            DatabaseNode existingSourceNode = new DatabaseNode();
            existingSourceNode.setId("existingSource1");
            existingSourceNode.setConnectionId(connectionId);
            existingSourceNode.setTableNames(Arrays.asList("existing_table1", "existing_table2"));
            existingSourceNode.setMigrateTableSelectType("manual");

            // 创建现有目标节点
            DatabaseNode existingTargetNode = new DatabaseNode();
            existingTargetNode.setId("existingTarget1");
            existingTargetNode.setConnectionId("targetConn");

            SyncObjects existingSyncObjects = new SyncObjects();
            existingSyncObjects.setObjectNames(Arrays.asList("existing_target_table1", "existing_target_table2"));
            existingTargetNode.setSyncObjects(Arrays.asList(existingSyncObjects));
            when(existingDag.getTargets()).thenReturn(Arrays.asList(existingTargetNode));
            when(existingDag.getSources()).thenReturn(Arrays.asList(existingSourceNode));
            existingTask.setDag(existingDag);
            return existingTask;
        }
    }
}
