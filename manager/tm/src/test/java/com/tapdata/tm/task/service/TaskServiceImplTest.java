package com.tapdata.tm.task.service;

import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.DataPermissionHelperImpl;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.dto.CheckEchoOneNodeParam;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.utils.TaskServiceUtil;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.quartz.CronScheduleBuilder;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.beans.BeanUtils.copyProperties;


class TaskServiceImplTest {
    TaskServiceImpl taskService;
    AgentGroupService agentGroupService;
    WorkerService workerService;

    TaskDto taskDto;
    UserDetail user;
    @BeforeEach
    void init() {
        taskService = mock(TaskServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(taskService, "agentGroupService", agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(taskService, "workerService", workerService);

        taskDto = mock(TaskDto.class);
        user = mock(UserDetail.class);
    }

    @Nested
    class ConfirmByIdTest {
        TaskDto temp;
        @BeforeEach
        void init() {
            temp = mock(TaskDto.class);
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));

            when(taskService.findById(any(ObjectId.class))).thenReturn(temp);

            doNothing().when(taskService).checkTaskInspectFlag(taskDto);
            doNothing().when(taskService).checkDagAgentConflict(taskDto,user,true);
            doNothing().when(taskService).checkDDLConflict(taskDto);
            when(taskService.confirmById(taskDto, user, true, false)).thenReturn(mock(TaskDto.class));

            when(taskService.confirmById(taskDto, user, true)).thenCallRealMethod();
        }

        void assertVerify(int getIdTimes, int findByIdTimes) {
            try(MockedStatic<TaskServiceUtil> tsu = mockStatic(TaskServiceUtil.class)) {
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(temp, taskDto)).then(a->null);
                Assertions.assertDoesNotThrow(() -> taskService.confirmById(taskDto, user, true));
                tsu.verify(() -> TaskServiceUtil.copyAccessNodeInfo(temp, taskDto), times(findByIdTimes));
            }
            verify(taskDto, times(getIdTimes)).getId();
            verify(taskService, times(findByIdTimes)).findById(any(ObjectId.class));
            verify(taskService, times(1)).checkTaskInspectFlag(taskDto);
            verify(taskService, times(1)).checkDagAgentConflict(taskDto, user, true);
            verify(taskService, times(1)).checkDDLConflict(taskDto);
            verify(taskService, times(1)).confirmById(taskDto, user, true, false);
        }

        @Test
        void testNormal() {
            assertVerify(2, 1);
        }
        @Test
        void testNullObjectId() {
            when(taskDto.getId()).thenReturn(null);
            assertVerify(1, 0);
        }
        @Test
        void testGetAccessNodeTypeNotBlank() {
            when(taskDto.getAccessNodeType()).thenReturn("type");
            assertVerify(2, 1);
        }
    }

    @Nested
    class ContrastTest {
        AtomicReference<String> ato;
        String nodeId;
        String atoValue;
        Map<String, List<Message>> validateMessage;
        Message message;
        @BeforeEach
        void init() {
            message = mock(Message.class);
            validateMessage = mock(Map.class);
            atoValue = "id";
            nodeId = "nid";
            ato = mock(AtomicReference.class);
            when(ato.get()).thenReturn(atoValue);
            doNothing().when(ato).set(atoValue);
            when(validateMessage.put(anyString(), anyList())).thenReturn(mock(List.class));
            when(taskService.contrast(ato, nodeId, atoValue, validateMessage, message)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertFalse(taskService.contrast(ato, nodeId, atoValue, validateMessage, message));
            verify(ato, times(2)).get();
            verify(ato, times(0)).set(atoValue);
            verify(validateMessage, times(0)).put(anyString(), anyList());
        }

        @Test
        void testNull() {
            when(ato.get()).thenReturn(null);
            Assertions.assertFalse(taskService.contrast(ato, nodeId, atoValue, validateMessage, message));
            verify(ato, times(1)).get();
            verify(ato, times(1)).set(atoValue);
            verify(validateMessage, times(0)).put(anyString(), anyList());
        }

        @Test
        void testNotEqual() {
            when(ato.get()).thenReturn("xx");
            Assertions.assertTrue(taskService.contrast(ato, nodeId, atoValue, validateMessage, message));
            verify(ato, times(2)).get();
            verify(ato, times(0)).set(atoValue);
            verify(validateMessage, times(1)).put(anyString(), anyList());
        }
    }

    @Nested
    class CheckEchoOneNodeTest {
        TaskDto taskDto;
        DataSourceConnectionDto connectionDto;
        DataParentNode<?> dataParentNode;
        List<String> taskProcessIdList;
        Map<String, List<Message>> validateMessage;
        Message message;
        AtomicReference<String> nodeType;
        AtomicReference<String> nodeId;
        UserDetail user;

        List<String> connectionProcessIds;

        String parentNodeId;
        String accessNodeType;
        String taskType;
        String accessNodeProcessId;
        @BeforeEach
        void init() {
            accessNodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
            taskType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name();
            parentNodeId = "parentNodeId";
            accessNodeProcessId = "nodeId";

            taskDto = mock(TaskDto.class);
            connectionDto = mock(DataSourceConnectionDto.class);
            dataParentNode = mock(DataParentNode.class);
            taskProcessIdList = mock(List.class);
            validateMessage = mock(Map.class);
            message = mock(Message.class);
            nodeType = mock(AtomicReference.class);
            nodeId = mock(AtomicReference.class);
            user = mock(UserDetail.class);

            when(connectionDto.getAccessNodeType()).thenReturn(accessNodeType);
            when(dataParentNode.getId()).thenReturn(parentNodeId);
            when(taskService.contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message)).thenReturn(false);

            connectionProcessIds = mock(List.class);
            when(agentGroupService.getProcessNodeListWithGroup(connectionDto, user)).thenReturn(connectionProcessIds);
            when(connectionProcessIds.removeAll(taskProcessIdList)).thenReturn(false);

            when(taskDto.getAccessNodeType()).thenReturn(taskType);
            when(connectionProcessIds.isEmpty()).thenReturn(false);
            when(validateMessage.put(anyString(), anyList())).thenReturn(mock(List.class));

            when(connectionDto.getAccessNodeProcessId()).thenReturn(accessNodeProcessId);
            when(taskService.contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message)).thenReturn(false);

            when(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertFalse(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(1)).getId();
            verify(taskService, times(1)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(1)).removeAll(taskProcessIdList);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(connectionProcessIds, times(1)).isEmpty();
            verify(validateMessage, times(1)).put(anyString(), anyList());
            verify(connectionDto, times(0)).getAccessNodeProcessId();
            verify(taskService, times(0)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
        @Test
        void testNodeTypeNotEqualTaskType() {
            taskType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name();
            when(taskDto.getAccessNodeType()).thenReturn(taskType);
            Assertions.assertFalse(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(1)).getId();
            verify(taskService, times(1)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(1)).removeAll(taskProcessIdList);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(connectionProcessIds, times(0)).isEmpty();
            verify(validateMessage, times(1)).put(anyString(), anyList());
            verify(connectionDto, times(0)).getAccessNodeProcessId();
            verify(taskService, times(0)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
        @Test
        void testConnectionProcessIdsIsEmpty() {
            when(connectionProcessIds.isEmpty()).thenReturn(true);
            Assertions.assertFalse(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(1)).getId();
            verify(taskService, times(1)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(1)).removeAll(taskProcessIdList);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(connectionProcessIds, times(1)).isEmpty();
            verify(validateMessage, times(0)).put(anyString(), anyList());
            verify(connectionDto, times(0)).getAccessNodeProcessId();
            verify(taskService, times(0)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
        @Test
        void testConnectionDtoIsAUTOMATIC_PLATFORM_ALLOCATION() {
            accessNodeType = AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name();
            when(connectionDto.getAccessNodeType()).thenReturn(accessNodeType);
            when(taskService.contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message)).thenReturn(false);
            Assertions.assertTrue(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(0)).getId();
            verify(taskService, times(0)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(0)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(0)).removeAll(taskProcessIdList);
            verify(taskDto, times(0)).getAccessNodeType();
            verify(connectionProcessIds, times(0)).isEmpty();
            verify(validateMessage, times(0)).put(anyString(), anyList());
            verify(connectionDto, times(0)).getAccessNodeProcessId();
            verify(taskService, times(0)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
        @Test
        void testContrastIsTrue() {
            when(taskService.contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message)).thenReturn(true);
            Assertions.assertTrue(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(1)).getId();
            verify(taskService, times(1)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(0)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(0)).removeAll(taskProcessIdList);
            verify(taskDto, times(0)).getAccessNodeType();
            verify(connectionProcessIds, times(0)).isEmpty();
            verify(validateMessage, times(0)).put(anyString(), anyList());
            verify(connectionDto, times(0)).getAccessNodeProcessId();
            verify(taskService, times(0)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
        @Test
        void testIsGroupManually() {
            accessNodeType = AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name();
            when(connectionDto.getAccessNodeType()).thenReturn(accessNodeType);
            when(taskService.contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message)).thenReturn(false);
            Assertions.assertFalse(taskService.checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(dataParentNode, times(1)).getId();
            verify(taskService, times(1)).contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message);
            verify(agentGroupService, times(0)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionProcessIds, times(0)).removeAll(taskProcessIdList);
            verify(taskDto, times(0)).getAccessNodeType();
            verify(connectionProcessIds, times(0)).isEmpty();
            verify(validateMessage, times(0)).put(anyString(), anyList());
            verify(connectionDto, times(1)).getAccessNodeProcessId();
            verify(taskService, times(1)).contrast(nodeId, parentNodeId, accessNodeProcessId, validateMessage, message);
        }
    }

    @Nested
    class CheckEngineStatusTest {
        List<String> taskProcessIdList;
        List<Worker> availableAgentByAccessNode;
        @BeforeEach
        void init() {
            taskProcessIdList = mock(List.class);
            availableAgentByAccessNode = mock(List.class);

            when(taskProcessIdList.isEmpty()).thenReturn(false);
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(taskProcessIdList);
            when(workerService.findAvailableAgentByAccessNode(user, taskProcessIdList)).thenReturn(availableAgentByAccessNode);
            when(availableAgentByAccessNode.isEmpty()).thenReturn(false);

            doCallRealMethod().when(taskService).checkEngineStatus(taskDto, user);
        }
        @Test
        void testAgentByGroupButListNotEmpty() {
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            Assertions.assertDoesNotThrow(() -> taskService.checkEngineStatus(taskDto, user));
            verify(taskProcessIdList, times(1)).isEmpty();
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(user, taskProcessIdList);
            verify(availableAgentByAccessNode, times(1)).isEmpty();
            verify(taskDto, times(1)).getAccessNodeType();
        }
        @Test
        void testAgentByGroupAndListIsEmpty() {
            when(taskProcessIdList.isEmpty()).thenReturn(true);
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            Assertions.assertThrows(BizException.class, () -> taskService.checkEngineStatus(taskDto, user));
            verify(taskProcessIdList, times(1)).isEmpty();
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
            verify(workerService, times(0)).findAvailableAgentByAccessNode(user, taskProcessIdList);
            verify(availableAgentByAccessNode, times(0)).isEmpty();
            verify(taskDto, times(1)).getAccessNodeType();
        }
        @Test
        void testAgentByUserButListIsEmpty() {
            when(availableAgentByAccessNode.isEmpty()).thenReturn(true);
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            Assertions.assertThrows(BizException.class, () -> taskService.checkEngineStatus(taskDto, user));
            verify(taskProcessIdList, times(0)).isEmpty();
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(user, taskProcessIdList);
            verify(availableAgentByAccessNode, times(1)).isEmpty();
            verify(taskDto, times(1)).getAccessNodeType();
        }
        @Test
        void testAgentByUserButListNotEmpty() {
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            Assertions.assertDoesNotThrow(() -> taskService.checkEngineStatus(taskDto, user));
            verify(taskProcessIdList, times(0)).isEmpty();
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(user, taskProcessIdList);
            verify(availableAgentByAccessNode, times(1)).isEmpty();
            verify(taskDto, times(1)).getAccessNodeType();
        }
    }

    @Nested
    class FindProcessNodeListWithGroupTest {
        List<String> accessNodeProcessIdList;
        List<String> list;

        @BeforeEach
        void init() {
            accessNodeProcessIdList = mock(List.class);
            list = mock(List.class);
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(list);
            when(list.isEmpty()).thenReturn(false);
            when(accessNodeProcessIdList.addAll(list)).thenReturn(true);
            when(taskService.findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertFalse(taskService.findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user));
            verify(accessNodeProcessIdList, times(1)).addAll(list);
        }

        @Test
        void testEmptyList() {
            when(list.isEmpty()).thenReturn(true);
            Assertions.assertFalse(taskService.findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user));
            verify(accessNodeProcessIdList, times(0)).addAll(list);
        }

        @Test
        void testThrow() {
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenAnswer(a -> {
                throw new BizException("Failed");
            });
            Assertions.assertThrows(BizException.class, () -> taskService.findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user));
            verify(accessNodeProcessIdList, times(0)).addAll(list);
        }

        @Test
        void testThrowV2() {
            when(list.isEmpty()).thenReturn(false);
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenAnswer(a -> {
                throw new BizException("group.agent.not.available");
            });
            Assertions.assertTrue(taskService.findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user));
            verify(accessNodeProcessIdList, times(0)).addAll(list);
        }
    }
    @Nested
    class CheckUnwindProcessTest{
        TaskServiceImpl taskService = new TaskServiceImpl(mock(TaskRepository.class));
        @Test
        void testNodesIsNull(){
            DAG dag = mock(DAG.class);
            when(dag.getNodes()).thenReturn(null);
            taskService.checkUnwindProcess(dag);
            Assertions.assertNull(dag.getNodes());
        }
        @Test
        void testHasUnwindProcess(){
            List<Node> nodes = new ArrayList<>();
            UnwindProcessNode unwindProcessNode = new UnwindProcessNode();
            unwindProcessNode.setId("source123");
            nodes.add(unwindProcessNode);
            TableNode tableNode = new TableNode();
            tableNode.setId("target123");
            tableNode.setDmlPolicy(new DmlPolicy());
            nodes.add(tableNode);
            Dag dag = new Dag();
            dag.setNodes(nodes);
            Edge edge=new Edge("source123","target123");
            List<Edge> edges = Arrays.asList(edge);
            dag.setEdges(edges);
            DAG mockDag =  DAG.build(dag);
            taskService.checkUnwindProcess(mockDag);
            assertEquals(tableNode.getDmlPolicy().getInsertPolicy(), DmlPolicyEnum.just_insert);
        }
        @Test
        void testNotHasUnwindProcess(){
            List<Node> nodes = new ArrayList<>();
            TableNode tableNode1 = new TableNode();
            tableNode1.setId("source123");
            nodes.add(tableNode1);
            TableNode tableNode2 = new TableNode();
            tableNode2.setId("target123");
            tableNode2.setDmlPolicy(new DmlPolicy());
            nodes.add(tableNode2);
            Dag dag = new Dag();
            dag.setNodes(nodes);
            Edge edge=new Edge("source123","target123");
            List<Edge> edges = Arrays.asList(edge);
            dag.setEdges(edges);
            DAG mockDag =  DAG.build(dag);
            taskService.checkUnwindProcess(mockDag);
            Assertions.assertNull(tableNode2.getDmlPolicy().getInsertPolicy());
        }
    }
    @Nested
    class CreateTest{
        private CustomSqlService customSqlService;
        private DateNodeService dateNodeService;
        private TransformSchemaService transformSchemaService;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            customSqlService = mock(CustomSqlService.class);
            dateNodeService = mock(DateNodeService.class);
            transformSchemaService = mock(TransformSchemaService.class);
            ReflectionTestUtils.setField(taskService, "customSqlService",customSqlService);
            ReflectionTestUtils.setField(taskService, "dateNodeService",dateNodeService);
            ReflectionTestUtils.setField(taskService, "transformSchemaService",transformSchemaService);
        }
        @Test
        @DisplayName("test create method normal")
        void test1(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            List<Node> nodes = new ArrayList<>();
            nodes.add(mock(DatabaseNode.class));
            JoinProcessorNode joinNode = mock(JoinProcessorNode.class);
            nodes.add(joinNode);
            when(dag.getNodes()).thenReturn(nodes);
            when(taskDto.getName()).thenReturn("test");
            ObjectId taskId = mock(ObjectId.class);
            when(taskDto.getId()).thenReturn(taskId);
            doNothing().when(taskService).checkTaskName("test", user, taskId);
            doNothing().when(customSqlService).checkCustomSqlTask(taskDto, user);
            doNothing().when(dateNodeService).checkTaskDateNode(taskDto, user);
            when(taskService.save(taskDto,user)).thenReturn(taskDto);
            doNothing().when(transformSchemaService).transformSchema(dag, user, taskId);
            when(joinNode.getType()).thenReturn("join_processor");
            doCallRealMethod().when(taskService).create(taskDto,user);
            TaskDto actual = taskService.create(taskDto, user);
            assertEquals(taskDto, actual);
        }
        @Test
        @DisplayName("test create method when cron schedule error")
        void test2(){
            try (MockedStatic<CronScheduleBuilder> mb = Mockito
                    .mockStatic(CronScheduleBuilder.class)) {
                String expression = "0 */1 * * * ?";
                mb.when(()->CronScheduleBuilder.cronSchedule(expression)).thenThrow(new RuntimeException());
                when(taskDto.getCrontabExpression()).thenReturn(expression);
                when(taskDto.getCrontabExpressionFlag()).thenReturn(true);
                doCallRealMethod().when(taskService).create(taskDto,user);
                assertThrows(BizException.class, ()->taskService.create(taskDto, user));
            }
        }
    }
    @Nested
    class BeforeSaveTest{
        @BeforeEach
        void setUp() {
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
        }
        @Test
        @DisplayName("test beforeSave method when dag is null")
        void test1(){
            when(taskDto.getDag()).thenReturn(null);
            doCallRealMethod().when(taskService).beforeSave(taskDto, user);
            taskService.beforeSave(taskDto, user);
        }
        @Test
        @DisplayName("test beforeSave method when nodes is null")
        void test2(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(dag.getNodes()).thenReturn(new ArrayList<>());
            doCallRealMethod().when(taskService).beforeSave(taskDto, user);
            taskService.beforeSave(taskDto, user);
        }
        @Test
        @DisplayName("test beforeSave method for merge table node")
        void test3(){
            try (MockedStatic<MergeTableProperties> mb = Mockito
                    .mockStatic(MergeTableProperties.class)) {
                MergeTableProperties properties = mock(MergeTableProperties.class);
                mb.when(() -> MergeTableProperties.autoFillingArray(properties, false)).thenAnswer(invocations->{return null;});
                DAG dag = mock(DAG.class);
                when(taskDto.getDag()).thenReturn(dag);
                List<Node> nodes = new ArrayList<>();
                Node node = new MergeTableNode();
                List<MergeTableProperties> list = new ArrayList<>();
                list.add(properties);
                ((MergeTableNode) node).setMergeProperties(list);
                nodes.add(node);
                when(dag.getNodes()).thenReturn(nodes);
                doCallRealMethod().when(taskService).beforeSave(taskDto, user);
                taskService.beforeSave(taskDto, user);
            }
        }
        @Test
        @DisplayName("test beforeSave method for cache node")
        void test4(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            List<Node> nodes = new ArrayList<>();
            Node node = new CacheNode();
            nodes.add(node);
            when(dag.getNodes()).thenReturn(nodes);
            List<Node> source = new ArrayList<>();
            source.add(mock(TableNode.class));
            when(dag.getSources()).thenReturn(source);
            doCallRealMethod().when(taskService).beforeSave(taskDto, user);
            taskService.beforeSave(taskDto, user);
        }
    }
    @Nested
    class UpdateByIdTest{
        private MeasurementServiceV2 measurementServiceV2;
        private CustomSqlService customSqlService;
        private DateNodeService dateNodeService;
        private TaskSaveService taskSaveService;
        private TransformSchemaAsyncService transformSchemaAsyncService;
        @BeforeEach
        void setUp() {
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            measurementServiceV2 = mock(MeasurementServiceV2.class);
            customSqlService = mock(CustomSqlService.class);
            dateNodeService = mock(DateNodeService.class);
            taskSaveService = mock(TaskSaveService.class);
            transformSchemaAsyncService = mock(TransformSchemaAsyncService.class);
            ReflectionTestUtils.setField(taskService, "customSqlService",customSqlService);
            ReflectionTestUtils.setField(taskService, "dateNodeService",dateNodeService);
            ReflectionTestUtils.setField(taskService, "measurementServiceV2", measurementServiceV2);
            ReflectionTestUtils.setField(taskService, "taskSaveService", taskSaveService);
            ReflectionTestUtils.setField(taskService, "transformSchemaAsyncService", transformSchemaAsyncService);
        }
        @Test
        @DisplayName("test updateById method when cron expression occur error")
        void test1(){
            try (MockedStatic<CronScheduleBuilder> mb = Mockito
                    .mockStatic(CronScheduleBuilder.class)) {
                String expression = "0 */1 * * * ?";
                mb.when(()->CronScheduleBuilder.cronSchedule(expression)).thenThrow(new RuntimeException());
                doNothing().when(taskService).checkTaskInspectFlag(taskDto);
                when(taskDto.getCrontabExpression()).thenReturn(expression);
                when(taskDto.getCrontabExpressionFlag()).thenReturn(true);
                doCallRealMethod().when(taskService).updateById(taskDto, user);
                assertThrows(BizException.class, ()->taskService.updateById(taskDto, user));
            }
        }
        @Test
        @DisplayName("test updateById method with run tables ")
        void test2(){
            doNothing().when(taskService).checkTaskInspectFlag(taskDto);
            ObjectId taskId = mock(ObjectId.class);
            when(taskDto.getId()).thenReturn(taskId);
            TaskDto oldTask = mock(TaskDto.class);
            when(taskService.findById(taskId)).thenReturn(oldTask);
            when(taskDto.getSyncType()).thenReturn("migrate");
            when(taskDto.getType()).thenReturn("initial_sync");
            DAG newDag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(newDag);
            when(oldTask.getTaskRecordId()).thenReturn("111");
            List<String> runTables = new ArrayList<>();
            runTables.add("test_table");
            when(measurementServiceV2.findRunTable(taskId.toHexString(),"111")).thenReturn(runTables);
            LinkedList<DatabaseNode> newSourceNode = new LinkedList();
            newSourceNode.add(mock(DatabaseNode.class));
            when(newDag.getSourceNode()).thenReturn(newSourceNode);
            doCallRealMethod().when(taskService).updateById(taskDto, user);
            taskService.updateById(taskDto,user);
            verify(taskService, new Times(1)).save(taskDto, user);
        }
        @Test
        @DisplayName("test updateById method with empty run tables")
        void test3(){
            doNothing().when(taskService).checkTaskInspectFlag(taskDto);
            ObjectId taskId = mock(ObjectId.class);
            when(taskDto.getId()).thenReturn(taskId);
            TaskDto oldTask = mock(TaskDto.class);
            when(taskService.findById(taskId)).thenReturn(oldTask);
            when(taskDto.getSyncType()).thenReturn("migrate");
            when(taskDto.getType()).thenReturn("initial_sync");
            DAG newDag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(newDag);
            when(oldTask.getTaskRecordId()).thenReturn("111");
            LinkedList<DatabaseNode> newSourceNode = new LinkedList();
            newSourceNode.add(mock(DatabaseNode.class));
            when(newDag.getSourceNode()).thenReturn(newSourceNode);
            doCallRealMethod().when(taskService).updateById(taskDto, user);
            taskService.updateById(taskDto,user);
            verify(taskService, new Times(1)).save(taskDto, user);
        }
        @Test
        @DisplayName("test updateById method when old task dto is null")
        void test4(){
            when(taskDto.getId()).thenReturn(null);
            doCallRealMethod().when(taskService).updateById(taskDto, user);
            taskService.updateById(taskDto,user);
            verify(taskService, new Times(1)).create(taskDto, user);
        }
    }
    @Nested
    class UpdateShareCacheTaskTest{
        private
        String id;
        private SaveShareCacheParam saveShareCacheParam;
        @BeforeEach
        void setUp() {
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            id = "111";
            saveShareCacheParam = mock(SaveShareCacheParam.class);
        }
        @Test
        void testUpdateShareCacheTask(){
            TaskDto dto = new TaskDto();
            when(taskService.findById(MongoUtils.toObjectId(id))).thenReturn(dto);
            when(saveShareCacheParam.getName()).thenReturn("cache_name");
            doCallRealMethod().when(taskService).updateShareCacheTask(id,saveShareCacheParam,user);
            TaskDto actual = taskService.updateShareCacheTask(id, saveShareCacheParam, user);
            assertEquals("cache_name", actual.getName());
        }
    }
    @Nested
    class CheckTaskNameTest{
        private String newName ;
        private ObjectId id;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            user = mock(UserDetail.class);
        }
        @Test
        @DisplayName("test checkTaskName method for name is null")
        void test1(){
            newName = "";
            doCallRealMethod().when(taskService).checkTaskName(newName,user,id);
            assertThrows(BizException.class, ()->taskService.checkTaskName(newName,user,id));
        }
        @Test
        @DisplayName("test checkTaskName method for repeat name")
        void test2(){
            newName = "test_name";
            id = mock(ObjectId.class);
            when(taskService.checkTaskNameNotError(newName,user,id)).thenReturn(true);
            doCallRealMethod().when(taskService).checkTaskName(newName,user,id);
            assertThrows(BizException.class, ()->taskService.checkTaskName(newName,user,id));
        }
    }
    @Nested
    class CheckTaskNameNotErrorTest{
        @Test
        void testNormal(){
            taskService = mock(TaskServiceImpl.class);
            user = mock(UserDetail.class);
            String newName = "test_name";
            ObjectId id = mock(ObjectId.class);
            when(taskService.count(any(Query.class),any(UserDetail.class))).thenReturn(1L);
            doCallRealMethod().when(taskService).checkTaskNameNotError(newName,user,id);
            boolean actual = taskService.checkTaskNameNotError(newName, user, id);
            assertEquals(true,actual);
        }
    }
    @Nested
    class ConfirmStartTest{
        private boolean confirm;
        private MonitoringLogsService monitoringLogsService;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            user = mock(UserDetail.class);
            taskDto = mock(TaskDto.class);
            monitoringLogsService = mock(MonitoringLogsService.class);
            ReflectionTestUtils.setField(taskService,"monitoringLogsService",monitoringLogsService);
        }
        @Test
        @DisplayName("test confirmStart method with exception")
        void test1(){
            confirm = true;
            when(taskService.confirmById(taskDto, user, confirm)).thenReturn(taskDto);
            doThrow(BizException.class).when(taskService).start(taskDto,user,"11");
            doCallRealMethod().when(taskService).confirmStart(taskDto,user,confirm);
            assertThrows(BizException.class,()->taskService.confirmStart(taskDto,user,confirm));
        }
        @Test
        @DisplayName("test confirmStart method without exception")
        void test2(){
            confirm = true;
            when(taskService.confirmById(taskDto, user, confirm)).thenReturn(taskDto);
            ObjectId taskId = mock(ObjectId.class);
            when(taskDto.getId()).thenReturn(taskId);
            doCallRealMethod().when(taskService).confirmStart(taskDto,user,confirm);
            taskService.confirmStart(taskDto,user,confirm);
            verify(taskService, new Times(1)).findById(taskId,user);
        }
    }
    @Nested
    class CheckDDLConflictTest{
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
        }
        @Test
        @DisplayName("test checkDDLConflict method when source node is not empty")
        void test1(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            LinkedList<DatabaseNode> sourceNode = new LinkedList<>();
            sourceNode.add(mock(DatabaseNode.class));
            when(dag.getSourceNode()).thenReturn(sourceNode);
            doCallRealMethod().when(taskService).checkDDLConflict(taskDto);
            taskService.checkDDLConflict(taskDto);
        }
        @Test
        @DisplayName("test checkDDLConflict method when ddl is unable")
        void test2(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            LinkedList<DatabaseNode> sourceNode = new LinkedList<>();
            when(dag.getSourceNode()).thenReturn(sourceNode);
            doCallRealMethod().when(taskService).checkDDLConflict(taskDto);
            taskService.checkDDLConflict(taskDto);
        }
    }
    @Nested
    class IsInspectPropertyChangedTest{
        private TaskDto existedTask = mock(TaskDto.class);
        private TaskDto newTask = mock(TaskDto.class);
        private DatabaseNode existedSourceDataNode;
        private DatabaseNode existedTargetDataNode;
        private DatabaseNode newSourceDataNode;
        private DatabaseNode newTargetDataNode;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            existedSourceDataNode = mock(DatabaseNode.class);
            existedTargetDataNode = mock(DatabaseNode.class);
            newSourceDataNode = mock(DatabaseNode.class);
            newTargetDataNode = mock(DatabaseNode.class);
            when(taskService.getSourceNode(existedTask)).thenReturn(existedSourceDataNode);
            when(taskService.getTargetNode(existedTask)).thenReturn(existedTargetDataNode);
            when(taskService.getSourceNode(newTask)).thenReturn(newSourceDataNode);
            when(taskService.getTargetNode(newTask)).thenReturn(newTargetDataNode);
        }
        @Test
        @DisplayName("test isInspectPropertyChanged method when existed task name not equals new task name")
        void test1(){
            String existedSourceName = "source_name";
            String newSourceName = "source_name";
            String existedTargetName = "exist_target_name";
            String newTargetName = "new_target_name";
            when(existedSourceDataNode.getName()).thenReturn(existedSourceName);
            when(newSourceDataNode.getName()).thenReturn(newSourceName);
            when(existedTargetDataNode.getName()).thenReturn(existedTargetName);
            when(newTargetDataNode.getName()).thenReturn(newTargetName);
            doCallRealMethod().when(taskService).isInspectPropertyChanged(existedTask,newTask);
            Boolean actual = taskService.isInspectPropertyChanged(existedTask, newTask);
            assertEquals(true,actual);
        }
        @Test
        @DisplayName("test isInspectPropertyChanged method when existed task name equals new task name")
        void test2(){
            String existedSourceName = "source_name";
            String newSourceName = "source_name";
            String existedTargetName = "target_name";
            String newTargetName = "target_name";
            when(existedSourceDataNode.getName()).thenReturn(existedSourceName);
            when(newSourceDataNode.getName()).thenReturn(newSourceName);
            when(existedTargetDataNode.getName()).thenReturn(existedTargetName);
            when(newTargetDataNode.getName()).thenReturn(newTargetName);
            doCallRealMethod().when(taskService).isInspectPropertyChanged(existedTask,newTask);
            Boolean actual = taskService.isInspectPropertyChanged(existedTask, newTask);
            assertEquals(false,actual);
        }
        @Test
        @DisplayName("test isInspectPropertyChanged method when existed source table name equals new source table name")
        void test3(){
            String existedSourceName = "source_name";
            String newSourceName = "source_name";
            String existedTargetName = "target_name";
            String newTargetName = "target_name";
            when(existedSourceDataNode.getName()).thenReturn(existedSourceName);
            when(newSourceDataNode.getName()).thenReturn(newSourceName);
            when(existedTargetDataNode.getName()).thenReturn(existedTargetName);
            when(newTargetDataNode.getName()).thenReturn(newTargetName);
            List<SyncObjects> newSyncObjects = new ArrayList<>();
            SyncObjects syncObjects = mock(SyncObjects.class);
            newSyncObjects.add(syncObjects);
            when(newTargetDataNode.getSyncObjects()).thenReturn(newSyncObjects);
            when(existedTargetDataNode.getSyncObjects()).thenReturn(newSyncObjects);
            when(syncObjects.getType()).thenReturn("table");
            List<String> objectNames1 = new ArrayList<>();
            objectNames1.add("test1");
            List<String> objectNames2 = new ArrayList<>();
            objectNames2.add("test2");
            when(syncObjects.getObjectNames()).thenReturn(objectNames1).thenReturn(objectNames2);
            doCallRealMethod().when(taskService).isInspectPropertyChanged(existedTask,newTask);
            Boolean actual = taskService.isInspectPropertyChanged(existedTask, newTask);
            assertEquals(true,actual);
        }
    }
    @Nested
    class ConfirmByIdWithImportTaskTest{
        private boolean confirm;
        private boolean importTask;
        private StateMachineService stateMachineService;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            confirm = true;
            importTask = false;
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
        }
        @Test
        @DisplayName("test confirmById method when validate message is not empty")
        void test1(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getShareCache()).thenReturn(false);
            Map<String, List<Message>> validateMessage = new HashMap<>();
            validateMessage.put("nodeId",new ArrayList<>());
            when(dag.validate()).thenReturn(validateMessage);
            doCallRealMethod().when(taskService).confirmById(taskDto,user,confirm,importTask);
            assertThrows(BizException.class,()->taskService.confirmById(taskDto,user,confirm,importTask));
        }
        @Test
        @DisplayName("test confirmById method normal")
        void test2(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            when(taskDto.getShareCache()).thenReturn(true);
            doCallRealMethod().when(taskService).confirmById(taskDto,user,confirm,importTask);
            taskService.confirmById(taskDto,user,confirm,importTask);
            verify(taskService,new Times(1)).updateById(taskDto,user);
        }
    }
    @Nested
    class CheckDagAgentConflictTest{
        private boolean showListMsg;
        private DataSourceServiceImpl dataSourceService;
        private AgentGroupService agentGroupService;
        @BeforeEach
        void setUp(){
            TaskRepository repository = mock(TaskRepository.class);
            taskService = new TaskServiceImpl(repository);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            dataSourceService = mock(DataSourceServiceImpl.class);
            agentGroupService = mock(AgentGroupService.class);
            ReflectionTestUtils.setField(taskService,"dataSourceService",dataSourceService);
            ReflectionTestUtils.setField(taskService,"agentGroupService",agentGroupService);
        }
        @Test
        @DisplayName("test checkDagAgentConflict method when task share cache is true")
        void test1(){
            showListMsg = true;
            when(taskDto.getShareCache()).thenReturn(true);
            taskService.checkDagAgentConflict(taskDto,user,showListMsg);
            verify(taskDto, new Times(0)).getDag();
        }
        @Test
        @DisplayName("test checkDagAgentConflict method when validate message is not empty and showListMsg is true")
        void test2(){
            showListMsg = true;
            when(taskDto.getShareCache()).thenReturn(false);
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            nodes.add(mock(DatabaseNode.class));
            when(dag.getNodes()).thenReturn(nodes);
            when(taskDto.getDag()).thenReturn(dag);
            List<DataSourceConnectionDto> dataSourceConnectionList = new ArrayList<>();
            DataSourceConnectionDto connectionDto = mock(DataSourceConnectionDto.class);
            when(connectionDto.getId()).thenReturn(mock(ObjectId.class));
            when(connectionDto.getAccessNodeType()).thenReturn("MANUALLY_SPECIFIED_BY_THE_USER");
            dataSourceConnectionList.add(connectionDto);
            when(dataSourceService.findInfoByConnectionIdList(anyList())).thenReturn(dataSourceConnectionList);
            List<String> connectionProcessIds = new ArrayList<>();
            connectionProcessIds.add("111");
            when(agentGroupService.getProcessNodeListWithGroup(connectionDto,user)).thenReturn(connectionProcessIds);
            assertThrows(BizException.class,()->taskService.checkDagAgentConflict(taskDto,user,showListMsg));
        }
        @Test
        @DisplayName("test checkDagAgentConflict method when validate message is not empty and showListMsg is false")
        void test3(){
            showListMsg = false;
            when(taskDto.getShareCache()).thenReturn(false);
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            nodes.add(mock(DatabaseNode.class));
            when(dag.getNodes()).thenReturn(nodes);
            when(taskDto.getDag()).thenReturn(dag);
            List<DataSourceConnectionDto> dataSourceConnectionList = new ArrayList<>();
            DataSourceConnectionDto connectionDto = mock(DataSourceConnectionDto.class);
            when(connectionDto.getId()).thenReturn(mock(ObjectId.class));
            when(connectionDto.getAccessNodeType()).thenReturn("MANUALLY_SPECIFIED_BY_THE_USER");
            dataSourceConnectionList.add(connectionDto);
            when(dataSourceService.findInfoByConnectionIdList(anyList())).thenReturn(dataSourceConnectionList);
            List<String> connectionProcessIds = new ArrayList<>();
            connectionProcessIds.add("111");
            when(agentGroupService.getProcessNodeListWithGroup(connectionDto,user)).thenReturn(connectionProcessIds);
            assertThrows(BizException.class,()->taskService.checkDagAgentConflict(taskDto,user,showListMsg));
        }
    }
    @Nested
    class RemoveTest{
        private ObjectId id;
        private StateMachineService stateMachineService;
        private TaskResetLogService taskResetLogService;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            user = mock(UserDetail.class);
            id = mock(ObjectId.class);
            stateMachineService = mock(StateMachineService.class);
            taskResetLogService = mock(TaskResetLogService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
            ReflectionTestUtils.setField(taskService,"taskResetLogService",taskResetLogService);
        }
        @Test
        @DisplayName("test remove method when no agent")
        void test1(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user)).thenReturn(dto);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineService.executeAboutTask(dto,DataFlowEvent.DELETE, user)).thenReturn(stateMachineResult);
            when(stateMachineResult.isOk()).thenReturn(true);
            when(taskService.findAgent(dto,user)).thenReturn(true);
            doCallRealMethod().when(taskService).remove(id,user);
            taskService.remove(id,user);
            verify(taskService, new Times(1)).afterRemove(dto,user);
        }
        @Test
        @DisplayName("test remove method when no agent is false")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user)).thenReturn(dto);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineService.executeAboutTask(dto,DataFlowEvent.DELETE, user)).thenReturn(stateMachineResult);
            when(stateMachineResult.isOk()).thenReturn(true);
            when(taskService.findAgent(dto,user)).thenReturn(false);
            doNothing().when(taskService).sendRenewMq(dto,user, DataSyncMq.OP_TYPE_DELETE);
            doCallRealMethod().when(taskService).remove(id,user);
            taskService.remove(id,user);
            verify(taskService, new Times(0)).afterRemove(dto,user);
        }
    }
    @Nested
    class AfterRemove{
        private TaskAutoInspectResultsService taskAutoInspectResultsService;
        @BeforeEach
        void setUp(){
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            taskAutoInspectResultsService = mock(TaskAutoInspectResultsService.class);
            ReflectionTestUtils.setField(taskService,"taskAutoInspectResultsService",taskAutoInspectResultsService);
        }
        @Test
        void test1(){
            when(taskDto.getSyncType()).thenReturn("migrate");
            doCallRealMethod().when(taskService).afterRemove(taskDto,user);
            taskService.afterRemove(taskDto,user);
        }
    }
    @Nested
    class DeleteShareCache{

    }
    @Nested
    class CopyTest{

    }
    @Nested
    class RenewTest{

    }
    @Nested
    class AfterRenewTest{

    }
    @Nested
    class CheckExistByIdTest{

    }
    @Nested
    class CheckExistByIdWithFieldsTest{

    }
    @Nested
    class CheckExistByIdWithoutUser{

    }
    @Nested
    class BatchStopTest{

    }
    @Nested
    class BatchDeleteTest{

    }
    @Nested
    class BatchRenewTest{

    }
    @Nested
    class FindTest{

    }
    @Nested
    class DeleteNotifyEnumDataTest{

    }
    @Nested
    class FindDataCopyListTest{

    }
    @Nested
    class FindDataDevListTest{

    }
    @Nested
    class GetSourceNodeTest{

    }
    @Nested
    class GetTargetNodeTest{

    }
    @Nested
    class SearchLogCollectorTest{

    }
    @Nested
    class CreateShareCacheTaskTest{

    }@Nested
    class FindShareCacheTest{

    }@Nested
    class FindShareCacheByIdTest{

    }@Nested
    class ParseCacheToTaskDtoTest{

    }@Nested
    class InspectChartTest{

    }@Nested
    class GetDataCopyChartTest{

    }@Nested
    class GetDataDevChartTest{

    }@Nested
    class FindByIdsTest{

    }@Nested
    class FindTaskDetailByIdTest{

    }@Nested
    class GetLastHourTest{

    }@Nested
    class GetMillstoneTimeTest{

    }@Nested
    class CheckRunTest{

    }@Nested
    class FindTransformParamTest{

    }@Nested
    class FindTransformAllParam{

    }

    @Test
    void testFindRunningTasksByAgentIdWithoutId() {
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskServiceImpl(repository));
        String processId = "  ";
        assertThrows(IllegalArgumentException.class, () -> taskService.findRunningTasksByAgentId(processId));
    }

    @Test
    void testFindRunningTasksByAgentIdWithId() {
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskServiceImpl(repository));
        String processId = "111";
        Query query = Query.query(Criteria.where("agentId").is(processId).and("status").is("running"));
        when(taskService.findAll(query)).thenReturn(new ArrayList<>());
        int actual = taskService.findRunningTasksByAgentId(processId);
        assertEquals(0, actual);
    }

    @Nested
    class TestCheckIsCronOrPlanTask {
        /**
         * 测试传入的Task任务为null
         */
        @Test
        void testCheckIsCronOrPlanTaskWithNullTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            assertThrows(IllegalArgumentException.class, () -> {
                taskService.checkIsCronOrPlanTask(null);
            });
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为 null 的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithNullCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            TaskDto taskDto = new TaskDto();
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }

        /**
         * 测试传入的Task的planStartDateFlag属性为true的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithTruePlanTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setPlanStartDateFlag(true);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(true, result);
        }

        /**
         * 测试传入的Task的planStartDateFlag属性为false的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithFalsePlanTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setPlanStartDateFlag(false);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为true的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithTrueCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setCrontabExpressionFlag(true);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(true, result);
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为false的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithFalseCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setCrontabExpressionFlag(false);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }
    }

    @Nested
    class TestBatchStart {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        TaskRepository repository;
        ArrayList<TaskEntity> taskEntities = new ArrayList<>();
        SettingsServiceImpl settingsService;
        List<ObjectId> ids;
        Query query;
        TaskEntity taskEntity;
        TaskScheduleService taskScheduleService;


        @BeforeEach
        void beforeEach() {
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            settingsService = mock(SettingsServiceImpl.class);
            when(settingsService.isCloud()).thenReturn(true);
            taskService.setSettingsService(settingsService);
            ids = Arrays.asList("6324562fc5c0a4052d821d90").stream().map(ObjectId::new).collect(Collectors.toList());
            query = new Query(Criteria.where("_id").in(ids));
            taskEntity = new TaskEntity();
            taskEntity.setUserId("6393f084c162f518b18165c3");
            taskEntity.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskEntity.setName("test");
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            taskEntities.add(taskEntity);
            taskScheduleService = mock(TaskScheduleService.class);
            taskService.setTaskScheduleService(taskScheduleService);
            when(repository.findAll(query)).thenReturn(taskEntities);
        }

        @Test
        void testExceedBatchStart() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                Query query = new Query(Criteria.where("_id").is(taskEntity.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                List<TaskDto> taskDtos = CglibUtil.copyList(taskEntities, TaskDto::new);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDtos.get(0), user, true)).thenReturn(calculationEngineVo);
                MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
                taskService.setMonitoringLogsService(monitoringLogsService);
                List<MutiResponseMessage> mutiResponseMessages = taskService.batchStart(ids, user, null, null);
                assertEquals("Task.ScheduleLimit", mutiResponseMessages.get(0).getCode());
            }
        }

        @Test
        void testBatchStart() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                taskEntity.setCrontabExpressionFlag(true);
                Query query = new Query(Criteria.where("_id").is(taskEntity.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                List<TaskDto> taskDtos = CglibUtil.copyList(taskEntities, TaskDto::new);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDtos.get(0), user, true)).thenReturn(calculationEngineVo);
                MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
                taskService.setMonitoringLogsService(monitoringLogsService);
                taskService.batchStart(ids, user, null, null);
                verify(taskService, times(1)).start(taskDtos.get(0), user, "11");
            }
        }
    }

    @Nested
    class TestRun {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        TaskRepository repository;
        SettingsServiceImpl settingsService;
        TaskEntity taskEntity;
        TaskScheduleService taskScheduleService;

        @BeforeEach
        void beforeEach() {
            repository=mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            taskEntity=new TaskEntity();
            taskEntity.setUserId("6393f084c162f518b18165c3");
            taskEntity.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskEntity.setName("test");
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            settingsService=mock(SettingsServiceImpl.class);
            when(settingsService.isCloud()).thenReturn(true);
            taskService.setSettingsService(settingsService);
            taskScheduleService=mock(TaskScheduleService.class);
            taskService.setTaskScheduleService(taskScheduleService);
        }

        @Test
        void testNoExceedRun() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                taskEntity.setCrontabExpressionFlag(true);
                TaskDto taskDto = new TaskDto();
                copyProperties(taskEntity, taskDto, TaskDto.class);
                Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDto, user, true)).thenReturn(calculationEngineVo);
                StateMachineService stateMachineService = mock(StateMachineService.class);
                taskService.setStateMachineService(stateMachineService);
                when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.START, user)).thenReturn(StateMachineResult.ok());
                taskService.run(taskDto, user);
                verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.START, user);
            }
        }

        @Test
        void testExceedRun() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                TaskDto taskDto = new TaskDto();
                copyProperties(taskEntity, taskDto, TaskDto.class);
                Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
                taskService.setTaskScheduleService(taskScheduleService);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDto, user, true)).thenReturn(calculationEngineVo);
                StateMachineService stateMachineService = mock(StateMachineService.class);
                taskService.setStateMachineService(stateMachineService);
                assertThrows(BizException.class, () -> taskService.run(taskDto, user));
            }
        }
    }
    @Nested
    class TestSubCronOrPlanNum{
        TaskRepository taskRepository;
        TaskEntity taskEntity;
        TaskDto taskDto;
        @BeforeEach
        void beforeEach(){
            taskEntity=new TaskEntity();
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));

            taskDto=new TaskDto();
            BeanUtils.copyProperties(taskEntity,taskDto);
            taskRepository = mock(TaskRepository.class);
            taskService=spy(new TaskServiceImpl(taskRepository));
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
            query.fields().include("planStartDateFlag", "crontabExpressionFlag");
            when(taskRepository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
        }
        @DisplayName("test cron task sub 1")
        @Test
        void testSubCronOrPlanNum(){
            taskEntity.setCrontabExpressionFlag(true);
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                int result = taskService.subCronOrPlanNum(taskDto, 3);
                assertEquals(2,result);
            }
        }
        @DisplayName("test not cron task don't sub 1")
        @Test
        void testNoCronOrPlanTask(){
            taskEntity.setCrontabExpressionFlag(null);
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                int result = taskService.subCronOrPlanNum(taskDto, 3);
                assertEquals(3,result);
            }
        }
    }

    @Nested
    class TestCheckCloudTaskLimit {
        TaskRepository taskRepository = mock(TaskRepository.class);

        SettingsServiceImpl settingsService = mock(SettingsServiceImpl.class);

        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
        WorkerService workerService = mock(WorkerService.class);

        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
            taskService = new TaskServiceImpl(taskRepository);
            taskService.setSettingsService(settingsService);
            taskService.setTaskScheduleService(taskScheduleService);
            ReflectionTestUtils.setField(taskService,"workerService",workerService);
        }
        @Test
        void test_isDass(){
            when(settingsService.isCloud()).thenReturn(false);
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
            assertTrue(result);
        }

        @Test
        void test_isCloudLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(false);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertFalse(result);
            }
        }

        @Test
        void test_isCloudLimitNotReached(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(false);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertTrue(result);
            }
        }

        @Test
        void test_isCloudLimitScheduling(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertTrue(result);
            }
        }
    }

    @Nested
    class TestCopy{
        TaskRepository taskRepository = mock(TaskRepository.class);

        SettingsServiceImpl settingsService = mock(SettingsServiceImpl.class);

        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);

        UserLogService serLogService = mock(UserLogService.class);

        WorkerService workerService = mock(WorkerService.class);

        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
            taskService = new TaskServiceImpl(taskRepository);
            taskService.setSettingsService(settingsService);
            taskService.setTaskScheduleService(taskScheduleService);
            taskService.setUserLogService(serLogService);
            ReflectionTestUtils.setField(taskService,"workerService",workerService);
        }

        @Test
        void test_copySchedulingTask(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setCrontabExpression("test");
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskServiceImpl mockTaskService = mock(TaskServiceImpl.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskServiceImpl.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertTrue(result.getCrontabExpressionFlag());
                assertEquals("test",result.getCrontabExpression());
            }
        }

        @Test
        void test_copySchedulingTaskLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskServiceImpl mockTaskService = mock(TaskServiceImpl.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskServiceImpl.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertFalse(result.getCrontabExpressionFlag());
                assertNull(result.getCrontabExpression());
            }
        }

        @Test
        void test_copyNormalTaskLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskServiceImpl mockTaskService = mock(TaskServiceImpl.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskServiceImpl.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertNull(result.getCrontabExpressionFlag());
                assertNull(result.getCrontabExpression());
            }
        }
    }
    @Nested
    class TestRunningTaskNum{
        TaskRepository taskRepository = mock(TaskRepository.class);
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            taskService = new TaskServiceImpl(taskRepository);
        }
        @Test
        void testRunningTaskNum(){
            long except = 5L;
            when(taskRepository.count(Query.query(Criteria.where("is_deleted").ne(true)
                    .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                    .and("status").nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                    .orOperator(Criteria.where("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                            Criteria.where("planStartDateFlag").is(true),
                            Criteria.where("crontabExpressionFlag").is(true)
                    )),user)).thenReturn(except);
            long result = taskService.runningTaskNum(user);
            assertEquals(except,result);
        }
    }
    @Nested
    class ChartTest{
        TaskRepository taskRepository = mock(TaskRepository.class);
        //        @Test
        void testChartNormal(){
            new DataPermissionHelper(mock(DataPermissionHelperImpl.class)); //when repository.find call methods in DataPermissionHelper class this line is need
            try (MockedStatic<DataPermissionService> mb = Mockito
                    .mockStatic(DataPermissionService.class)) {
                mb.when(DataPermissionService::isCloud).thenReturn(true);
                taskService = spy(new TaskServiceImpl(taskRepository));
                UserDetail user = mock(UserDetail.class);
                DataPermissionMenuEnums permission = mock(DataPermissionMenuEnums.class);
                List<TaskDto> taskDtoList = new ArrayList<>();
                TaskDto taskDto1 = new TaskDto();
                taskDto1.setStatus("stop");
                taskDto1.setSyncType("migrate");
                TaskDto taskDto2 = new TaskDto();
                taskDto2.setStatus("wait_start");
                taskDto2.setSyncType("migrate");
                TaskDto taskDto3 = new TaskDto();
                taskDto3.setStatus("edit");
                taskDto3.setSyncType("migrate");
                TaskDto taskDto4 = new TaskDto();
                taskDto4.setStatus("stop");
                taskDto4.setSyncType("sync");
                TaskDto taskDto5 = new TaskDto();
                taskDto5.setStatus("stop");
                taskDto5.setSyncType("sync");
                taskDtoList.add(taskDto1);
                taskDtoList.add(taskDto2);
                taskDtoList.add(taskDto3);
                taskDtoList.add(taskDto4);
                taskDtoList.add(taskDto5);
                doReturn(taskDtoList).when(taskService).findAllDto(any(),any());
                when(permission.MigrateTack.checkAndSetFilter(user, DataPermissionActionEnums.View, () -> taskService.findAllDto(any(),any()))).thenReturn(taskDtoList);
                doReturn(new HashMap()).when(taskService).inspectChart(user);
                Chart6Vo chart6Vo = mock(Chart6Vo.class);
                doReturn(chart6Vo).when(taskService).chart6(user);
                Map<String, Object> actual = taskService.chart(user);
                Map chart1 = (Map) actual.get("chart1");
                assertEquals(3,chart1.get("total"));
                Map chart3 = (Map) actual.get("chart3");
                assertEquals(2,chart3.get("total"));
                Map chart5 = (Map) actual.get("chart5");
                assertEquals(0,chart5.size());
                assertEquals(chart6Vo,actual.get("chart6"));
            }

        }
    }
    @Nested
    class importRmProjectTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        UserDetail userDetail;
        FileInputStream fileInputStream;
        @BeforeEach
        void beforeEach() throws FileNotFoundException {
            userDetail = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                    "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
            URL resource = this.getClass().getClassLoader().getResource("test.relmig");
            fileInputStream=new FileInputStream(resource.getFile());
        }
        @Test
        void importRmProjectTest() throws IOException {
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String rmJson = new String(mockMultipartFile.getBytes());
            HashMap<String, Object> rmProject = new ObjectMapper().readValue(rmJson, HashMap.class);
            HashMap<String, Object> project = (HashMap<String, Object>) rmProject.get("project");
            HashMap<String, Object> content = (HashMap<String, Object>) project.get("content");
            HashMap<String, Object> contentCollections = (HashMap<String, Object>) content.get("collections");
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(rmJson, "123", "123", userDetail);
            TaskDto taskDto=null;
            for(String taskKey:stringStringMap.keySet()){
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(taskKey), TaskDto.class);
            }
            assertEquals(5,stringStringMap.size());
        }
        @Test
        void nullImportRmProjectTest(){
            assertThrows(BizException.class,()->{taskService.parseTaskFromRm(null, "123", "123", userDetail);});
        }
        @Test
        void replaceIdTest() throws IOException {
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, Object> rmProject = new ObjectMapper().readValue(s, HashMap.class);
            Map<String, Object> project = (Map<String, Object>) rmProject.get("project");
            Map<String, Object> content = (Map<String, Object>) project.get("content");
            Map<String, Object> contentMapping = (Map<String, Object>) content.get("mappings");
            Map<String, Object> contentCollections = (Map<String, Object>) content.get("collections");
            Set<String> collectionKeys = contentCollections.keySet();
            String collectionKey=null;
            for(String key:collectionKeys){
                collectionKey=key;
            }
            Set<String> contentMappingKeys = contentMapping.keySet();
            String contentMappingKey=null;
            String contentMappingCollectionId=null;
            for(String key:contentMappingKeys){
                Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
                String collectionId = (String)mapping.get("collectionId");
                contentMappingCollectionId=collectionId;
                contentMappingKey=key;
            }
            taskService.replaceRmProjectId(rmProject);
            Set<String> afterStrings = contentCollections.keySet();
            String afterCollectionKey=null;
            for(String afterKey1:afterStrings){
                afterCollectionKey=afterKey1;
            }
            Set<String> afterContentMappingKeys = contentMapping.keySet();
            String afterContentMappingCollectionId=null;
            String afterContentMappingKey=null;
            for(String key:afterContentMappingKeys){
                Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
                afterContentMappingCollectionId = (String)mapping.get("collectionId");
                afterContentMappingKey=key;
            }
            assertNotEquals(collectionKey,afterCollectionKey);
            assertNotEquals(contentMappingKey,afterContentMappingKey);
            assertNotEquals(contentMappingCollectionId,afterContentMappingCollectionId);
        }
        @Test
        void testReplaceRelationShipsKey() throws IOException {
            Map<String, String> globalIdMap = new HashMap<>();
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, Object> rmProject = new ObjectMapper().readValue(s, HashMap.class);
            Map<String, Object> project = (Map<String, Object>) rmProject.get("project");
            Map<String, Object> content = (Map<String, Object>) project.get("content");
            Map<String, Object> relationships = content.get("relationships") == null ? new HashMap<>() : (Map<String, Object>) content.get("relationships");
            Map<String, Object> collectionMap = (Map<String, Object>) relationships.get("collections");
            Map<String, Object> mappingsMap = (Map<String, Object>) relationships.get("mappings");
            String collectionKey=null;
            for(String key:collectionMap.keySet()){
                collectionKey=key;
            }
            String mappingKey=null;
            for(String key:mappingsMap.keySet()){
                mappingKey=key;
            }
            String relationShipMappingsKey=null;
            for(String key:mappingsMap.keySet()){
                relationShipMappingsKey=key;
            }
            taskService.replaceRelationShipsKey(globalIdMap,content);
            String afterCollectionKey=null;
            for(String key:collectionMap.keySet()){
                afterCollectionKey=key;
            }
            String afterMappingKey=null;
            for(String key:mappingsMap.keySet()){
                afterMappingKey=key;
            }
            String afterRelationShipMappingsKey=null;
            for(String key:mappingsMap.keySet()){
                afterRelationShipMappingsKey=key;
            }
            assertNotEquals(collectionKey,afterCollectionKey);
            assertNotEquals(afterMappingKey,mappingKey);
            assertNotEquals(afterRelationShipMappingsKey,relationShipMappingsKey);
        }
        @Test
        void testImportRmProject() throws IOException {
            CustomSqlService customSqlService = mock(CustomSqlService.class);
            taskService.setCustomSqlService(customSqlService);
            DateNodeService dataNodeService = mock(DateNodeService.class);
            taskService.setDateNodeService(dataNodeService);
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(s, "123", "123", userDetail);
            TaskDto taskDto=null;
            for(String s1: stringStringMap.keySet()){
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(s1), TaskDto.class);
            }
            try (MockedStatic<BeanUtils> beanUtilsMockedStatic = mockStatic(BeanUtils.class);MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                BeanUtils.copyProperties(any(),any());
                TaskEntity taskEntity = taskService.convertToEntity(TaskEntity.class, taskDto);
                when(taskRepository.importEntity(any(),any())).thenReturn(taskEntity);
                MongoTemplate mongoTemplate = mock(MongoTemplate.class);
                when(taskRepository.getMongoOperations()).thenReturn(mongoTemplate);
                assertThrows(BizException.class,()->{taskService.importRmProject(mockMultipartFile,userDetail,false,new ArrayList<>(),"123","123");});
            }
        }
        @Test
        void testGenProperties() throws IOException {
            URL resource = this.getClass().getClassLoader().getResource("EmployeeSchema.relmig");
            FileInputStream fileInputStream = new FileInputStream(resource.getFile());
            MockMultipartFile mockMultipartFile = new MockMultipartFile("EmployeeSchema.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(s, "123", "123", userDetail);
            TaskDto taskDto = null;
            for (String key : stringStringMap.keySet()) {
                System.out.println();
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(key), TaskDto.class);
            }
            ;
            List<Node> nodes = taskDto.getDag().getNodes();
            boolean flag = false;
            for (Node node : nodes) {
                if (node.getType().equals("merge_table_processor")) {
                    flag = true;
                }
            }
            assertTrue(flag);
        }
    }
    @Nested
    class ParentColumnsFindJoinKeysClass{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        Map<String, Object> parent;
        Map<String, Map<String, Map<String, Object>>> renameFields;


        @BeforeEach
        void beforeSetUp(){
            parent = new HashMap<>();
            parent.put("rm_id", "rm_id -> eb1243b6-e7dc-4b84-b094-e719f9275512");
            parent.put("tableName", "Orders");
            renameFields = new HashMap<>();

            Map<String,Map<String,Object>> orderFieldMap=new HashMap<>();
            Map<String,Object> idAttrs=new HashMap<>();
            idAttrs.put("isPrimaryKey", false);
            idAttrs.put("target","_id");
            Map<String,Object> orderIdAttrs=new HashMap<>();
            orderIdAttrs.put("isPrimaryKey",true);
            orderIdAttrs.put("target","orderId");
            Map<String,Object> shipViaAttrs=new HashMap<>();
            shipViaAttrs.put("target","shipVia");
            shipViaAttrs.put("isPrimaryKey",false);
            orderFieldMap.put("ShipVia",shipViaAttrs);
            orderFieldMap.put("_id",idAttrs);
            orderFieldMap.put("OrderID",orderIdAttrs);

            Map<String,Map<String,Object>> shipperFieldMap=new HashMap<>();
            Map<String,Object> shipperIdAttrs=new HashMap<>();
            shipperIdAttrs.put("target","shipperId");
            shipperIdAttrs.put("isPrimaryKey",true);
            shipperFieldMap.put("ShipperID",shipperIdAttrs);

            renameFields.put("Shippers",shipperFieldMap);
            renameFields.put("Orders",orderFieldMap);
        }
        @DisplayName("test parent column have foreignKey,and foreignKey table is child table")
        @Test
        void test1(){
            parent.put("targetPath", "");
            Map<String, Map<String,String>> souceJoinKeyMapping=new HashMap<>();
            Map<String, Map<String,String>> targetJoinKeyMapping=new HashMap<>();
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","Shippers");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys, souceJoinKeyMapping, targetJoinKeyMapping);
            assertEquals(1,joinKeys.size());
            Map<String, String> stringStringMap = joinKeys.get(0);
            String sourceJoinKey = stringStringMap.get("source");
            String targetJoinKey = stringStringMap.get("target");
            assertEquals("shipperId",sourceJoinKey);
            assertEquals("shipVia",targetJoinKey);
        }
        @DisplayName("test parent column have foreignKey,foreignKey table is child table and have targetPath")
        @Test
        void test2(){
            parent.put("targetPath", "orders");
            Map<String, Map<String,String>> souceJoinKeyMapping=new HashMap<>();
            Map<String, Map<String,String>> targetJoinKeyMapping=new HashMap<>();
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","Shippers");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys, souceJoinKeyMapping, targetJoinKeyMapping);
            assertEquals(1,joinKeys.size());
            Map<String, String> stringStringMap = joinKeys.get(0);
            String sourceJoinKey = stringStringMap.get("source");
            String targetJoinKey = stringStringMap.get("target");
            assertEquals("shipperId",sourceJoinKey);
            assertEquals("orders.shipVia",targetJoinKey);
        }
        @DisplayName("test parent column no have foreignKey")
        @Test
        void test3(){
            Map<String, Map<String,String>> souceJoinKeyMapping=new HashMap<>();
            Map<String, Map<String,String>> targetJoinKeyMapping=new HashMap<>();
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys, souceJoinKeyMapping, targetJoinKeyMapping);
            assertEquals(0,joinKeys.size());
        }
        @DisplayName("test parnet column table is not child table")
        @Test
        void test4(){
            parent.put("targetPath", "");
            Map<String, Map<String,String>> souceJoinKeyMapping=new HashMap<>();
            Map<String, Map<String,String>> targetJoinKeyMapping=new HashMap<>();
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","testTable");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys, souceJoinKeyMapping, targetJoinKeyMapping);
            assertEquals(0,joinKeys.size());
        }
    }
    @Nested
    class GetEmbeddedDocumentPathTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        @DisplayName("test parent path is empty string,use embeddedPath")
        @Test
        void test1(){
            Map<String,String> setting=new HashMap<>();
            setting.put("embeddedPath","abc");
            String targetPath = taskService.getEmbeddedDocumentPath("", setting);
            assertEquals("abc",targetPath);
        }
        @DisplayName("test parent path is not empty string,embeddedPath is null")
        @Test
        void test2(){
            Map<String,String> setting=new HashMap<>();
            String targetPath = taskService.getEmbeddedDocumentPath("parentPath", setting);
            assertEquals("parentPath",targetPath);
        }
        @DisplayName("test parent path is not empty string,embeddedPath is not null")
        @Test
        void test3(){
            Map<String,String> setting=new HashMap<>();
            setting.put("embeddedPath","abc");
            String targetPath = taskService.getEmbeddedDocumentPath("parentPath", setting);
            assertEquals("parentPath.abc",targetPath);
        }
        @DisplayName("test parent path is not empty string,embeddedPaht is empty str")
        @Test
        void test4(){
            Map<String,String> setting=new HashMap<>();
            setting.put("embeddedPath","");
            String targetPath = taskService.getEmbeddedDocumentPath("parentPath", setting);
            assertEquals("parentPath",targetPath);
        }
    }
    @Nested
    class RunningTaskNumWithProcessIdTest{
        @Test
        void testRunningTaskNumWithProcessId(){
            TaskRepository taskRepository = mock(TaskRepository.class);
            taskService = new TaskServiceImpl(taskRepository);
            long except = 5L;
            UserDetail userDetail = mock(UserDetail.class);
            when(taskRepository.count(Query.query(Criteria.where("agentId").is("111")
                    .and("is_deleted").ne(true).and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                    .and("status").nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                    .orOperator(Criteria.where("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                            Criteria.where("planStartDateFlag").is(true),
                            Criteria.where("crontabExpressionFlag").is(true)
                    )), userDetail)).thenReturn(except);
            long result = taskService.runningTaskNum("111", userDetail);
            assertEquals(except,result);
        }
    }
    @Nested
    class GetNewNameMapTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        @DisplayName("test get newname map is pk")
        @Test
        void test1(){
            Map<String,Object> target=new HashMap<>();
            target.put("name","employeeId");
            target.put("included",true);
            Map<String,Object> source=new HashMap<>();
            source.put("name","EmployeeId");
            source.put("isPrimaryKey",true);
            Map<String, Object> newNameMap = taskService.getNewNameMap(target, source);
            assertEquals("employeeId",newNameMap.get("target"));
            assertEquals(true,newNameMap.get("isPrimaryKey"));
        }
        @DisplayName("test get newname map is not pk")
        @Test
        void test2(){
            Map<String,Object> target=new HashMap<>();
            target.put("name","employeeId");
            target.put("included",true);
            Map<String,Object> source=new HashMap<>();
            source.put("name","EmployeeId");
            source.put("isPrimaryKey",false);
            Map<String, Object> newNameMap = taskService.getNewNameMap(target, source);
            assertEquals("employeeId",newNameMap.get("target"));
            assertEquals(false,newNameMap.get("isPrimaryKey"));
        }
    }
    @Nested
    class GetOperationTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        @DisplayName("test get deleteOperation")
        @Test
        void test1(){
            Map<String,Object> source=new HashMap<>();
            source.put("name","EmployeeId");
            source.put("isPrimaryKey",false);
            Map<String, Object> deleteOperation = taskService.getDeleteOperation(source.get("name"), source.get("isPrimaryKey"));
            assertEquals("EmployeeId",deleteOperation.get("field"));
            assertEquals("REMOVE",deleteOperation.get("op"));
            assertEquals("true",deleteOperation.get("operand"));
            assertEquals("EmployeeId",deleteOperation.get("label"));
        }
        @DisplayName("test get renameOperation")
        @Test
        void test2(){
            Map<String,Object> target=new HashMap<>();
            target.put("name","employeeId");
            target.put("included",true);
            Map<String,Object> source=new HashMap<>();
            source.put("name","EmployeeId");
            source.put("isPrimaryKey",false);
            Map<String, Object> renameOperation = taskService.getRenameOperation(source.get("name"), target.get("name"));
            assertEquals("EmployeeId",renameOperation.get("field"));
            assertEquals("RENAME",renameOperation.get("op"));
            assertEquals("employeeId",renameOperation.get("operand"));
        }
    }
    @Nested
    class AddProcessorNodeTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        @DisplayName("test add delete node")
        @Test
        void test1(){
            List<Map<String, Object>> nodes=new ArrayList<>();
            List<Map<String, Object>> edges =new ArrayList<>();
            Map<String, Object> deleteOperation = new HashMap<>();
            List<Map<String, Object>> deleteOperationList=new ArrayList<>();
            deleteOperation.put("id", UUID.randomUUID().toString().toLowerCase());
            deleteOperation.put("field", "CustomerId");
            deleteOperation.put("op", "REMOVE");
            deleteOperation.put("operand", "true");
            deleteOperation.put("label", "CustomerId");
            deleteOperationList.add(deleteOperation);
            String sourceId = taskService.addDeleteNode("customer", deleteOperationList,  "souceId",nodes, edges);
            assertNotEquals("souceId",sourceId);
            assertEquals(1,nodes.size());
            Map<String, Object> nodeMap = nodes.get(0);
            assertEquals("Delete customer",nodeMap.get("name"));
            assertEquals("field_add_del_processor",nodeMap.get("type"));
        }
        @DisplayName("test add rename node")
        @Test
        void test2(){
            List<Map<String, Object>> nodes=new ArrayList<>();
            List<Map<String, Object>> edges =new ArrayList<>();
            List<Map<String, Object>> fieldRenameOperationList=new ArrayList<>();
            Map<String, Object> fieldRenameOperation = new HashMap<>();
            fieldRenameOperation.put("field", "CustomerId");
            fieldRenameOperation.put("op", "RENAME");
            fieldRenameOperation.put("operand", "customerId");
            fieldRenameOperationList.add(fieldRenameOperation);
            String sourceId = taskService.addRenameNode("customer", fieldRenameOperationList, "souceId",nodes, edges);
            assertNotEquals("souceId",sourceId);
            assertEquals(1,nodes.size());
            Map<String, Object> nodeMap = nodes.get(0);
            assertEquals("Rename customer",nodeMap.get("name"));
            assertEquals("field_rename_processor",nodeMap.get("type"));
        }
        @Test
        void test3(){
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> edges = new ArrayList<>();
            String script = "function process(){}";
            String declareScript = "retrun record";
            String sourceId = taskService.addJSNode("customer", script, declareScript, nodes, "sourceId", edges);
            assertNotEquals("souceId",sourceId);
            assertEquals(1,nodes.size());
            Map<String, Object> nodeMap = nodes.get(0);
            assertEquals("customer",nodeMap.get("name"));
            assertEquals("js_processor",nodeMap.get("type"));
        }
    }
    @Nested
    class RemoveDeleteOperationIfJoinKeyIsDeletedTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        List<Map<String, Object>> childDeleteOperationsList=new ArrayList<>();
        List<Map<String, Object>> childRenameOperationsList =new ArrayList();
        Map<String, List<Map<String, Object>>> contentDeleteOperations = new HashMap<>();
        Map<String, List<Map<String, Object>>> contentRenameOperations =new HashMap<>();
        @BeforeEach
        void beforeSetUp(){
            Map<String, Object> deleteOperations =new HashMap<>();
            deleteOperations.put("op","REMOVE");
            deleteOperations.put("field", "OrderID");
            deleteOperations.put("label" ,"OrderID");
            deleteOperations.put("operand","true");
            deleteOperations.put("id", UUID.randomUUID().toString().toLowerCase());
            childDeleteOperationsList.add(deleteOperations);
            Map<String,Object> renameOperations =new HashMap<>();
            renameOperations.put("op","RENAME");
            renameOperations.put("field" , "UnitPrice");
            renameOperations.put("id", UUID.randomUUID().toString().toLowerCase());
            renameOperations.put("operand","unitPrice");
            childRenameOperationsList.add(renameOperations);
            contentDeleteOperations.put("childId",childDeleteOperationsList);
            contentRenameOperations.put("childId",childRenameOperationsList);
        }
        @Test
        void test1(){
            Map<String,Map<String, String>> sourceJoinKeyMapping =new HashMap<>();
            Map<String,String> newFieldMap=new HashMap<>();
            newFieldMap.put("source","OrderID");
            newFieldMap.put("target","orderId");
            sourceJoinKeyMapping.put("orderId",newFieldMap);
            taskService.addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations,contentRenameOperations,"childId",sourceJoinKeyMapping,"orderId");
            assertEquals(0,childDeleteOperationsList.size());
            assertEquals(2,childRenameOperationsList.size());
        }
        @Test
        void test2(){
            Map<String,Map<String, String>> sourceJoinKeyMapping =new HashMap<>();
            Map<String,String> newFieldMap=new HashMap<>();
            newFieldMap.put("source","OrderId");
            newFieldMap.put("target","orderId");
            sourceJoinKeyMapping.put("productId",newFieldMap);
            taskService.addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations,contentRenameOperations,"childId",sourceJoinKeyMapping,"productId");
            assertEquals(1,childDeleteOperationsList.size());
            assertEquals(1,childRenameOperationsList.size());
        }
    }
    @Nested
    class RemoveDeleteOperationTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        List<Map<String, Object>> deleteOperationsList=new ArrayList<>();
        @BeforeEach
        void beforeSetUp(){
            Map<String, Object> deleteOperations =new HashMap<>();
            deleteOperations.put("op","REMOVE");
            deleteOperations.put("field", "OrderID");
            deleteOperations.put("label" ,"OrderID");
            deleteOperations.put("operand","true");
            deleteOperations.put("id", UUID.randomUUID().toString().toLowerCase());
            deleteOperationsList.add(deleteOperations);
        }
        @DisplayName("test removeDeleteOperation when joinkey in deleteOperation")
        @Test
        void test1(){
            Map<String,Map<String, String>> sourceJoinKeyMapping =new HashMap<>();
            Map<String,String> newFieldMap=new HashMap<>();
            newFieldMap.put("source","OrderID");
            newFieldMap.put("target","orderId");
            sourceJoinKeyMapping.put("orderId",newFieldMap);
            boolean flag = taskService.removeDeleteOperation(deleteOperationsList, sourceJoinKeyMapping, "orderId");
            assertEquals(true,flag);
        }
        @DisplayName("test removeDeleteOperation when joinkey not in deleteOperation")
        @Test
        void test2(){
            Map<String,Map<String, String>> sourceJoinKeyMapping =new HashMap<>();
            Map<String,String> newFieldMap=new HashMap<>();
            newFieldMap.put("source","OrderId");
            newFieldMap.put("target","orderId");
            sourceJoinKeyMapping.put("productId",newFieldMap);
            boolean flag = taskService.removeDeleteOperation(deleteOperationsList, sourceJoinKeyMapping, "productId");
            assertEquals(false,flag);
        }
    }

    @Nested
    class ReNewNotSendMqTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskServiceImpl taskService=spy(new TaskServiceImpl(taskRepository));
        @DisplayName("test errorEvents is null")
        @Test
        void test(){
            TaskDto taskDto = new TaskDto();
            taskDto.setDag(mock(DAG.class));
            UpdateResult updateResult = new UpdateResult() {
                @Override
                public boolean wasAcknowledged() {
                    return false;
                }

                @Override
                public long getMatchedCount() {
                    return 0;
                }

                @Override
                public long getModifiedCount() {
                    return 0;
                }

                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            };
            when(taskRepository.update(any(Query.class),any(Update.class),any())).thenAnswer(invocationOnMock -> {
                Update update = invocationOnMock.getArgument(1);
                Assertions.assertNull(update.getUpdateObject().get("errorEvents"));
                return updateResult;
            });
            taskService.renewNotSendMq(taskDto,mock(UserDetail.class));
        }

    }
}