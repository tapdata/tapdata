package com.tapdata.tm.task.service;

import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.disruptor.service.DisruptorService;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.lock.service.LockControlService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageServiceImpl;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.DataPermissionHelperImpl;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.schedule.service.ScheduleService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.dto.CheckEchoOneNodeParam;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.utils.TaskServiceUtil;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.task.vo.TaskDetailVo;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.tapdata.exception.TapCodeException;
import lombok.SneakyThrows;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.quartz.CronScheduleBuilder;
import org.slf4j.Logger;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
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
    class dataPermissionFindByIdTest{
        private ObjectId taskId;
        private Field fields;
        @Test
        void test1(){
            taskId = new ObjectId("6613954dc8a36646da142da3");
            fields = new Field();
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findById(taskId,fields)).thenReturn(dto);
            doCallRealMethod().when(taskService).dataPermissionFindById(taskId,fields);
            Supplier<TaskDto> actual = taskService.dataPermissionFindById(taskId, fields);
            assertEquals(dto,actual.get());
        }
    }
    @Nested
    class CreateTest{
        private CustomSqlService customSqlService;
        private DateNodeService dateNodeService;
        private TransformSchemaService transformSchemaService;
        @BeforeEach
        void setUp(){
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
            user = mock(UserDetail.class);
            id = "65bc933e6129fe73d7858cbd";
            saveShareCacheParam = mock(SaveShareCacheParam.class);
        }
        @Test
        void testUpdateShareCacheTask(){
            TaskDto dto = new TaskDto();
            when(taskService.findById(MongoUtils.toObjectId(id))).thenReturn(dto);
            when(saveShareCacheParam.getName()).thenReturn("cache_name");
            doCallRealMethod().when(taskService).updateShareCacheTask(id,saveShareCacheParam,user);
            TaskDto actual = taskService.updateShareCacheTask(id, saveShareCacheParam, user);
            verify(taskService,new Times(1)).parseCacheToTaskDto(saveShareCacheParam,dto);
            verify(taskService,new Times(1)).updateById(any(TaskDto.class),any(UserDetail.class));
        }
    }
    @Nested
    class CheckTaskNameTest{
        private String newName ;
        private ObjectId id;
        @BeforeEach
        void setUp(){
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
        private MessageServiceImpl messageService;
        private MetadataInstancesServiceImpl metadataInstancesService;
        private MetaDataHistoryService historyService;
        private TaskCollectionObjService taskCollectionObjService;
        @BeforeEach
        void setUp(){
            taskAutoInspectResultsService = mock(TaskAutoInspectResultsService.class);
            messageService = mock(MessageServiceImpl.class);
            metadataInstancesService = mock(MetadataInstancesServiceImpl.class);
            historyService = mock(MetaDataHistoryService.class);
            taskCollectionObjService = mock(TaskCollectionObjService.class);
            ReflectionTestUtils.setField(taskService,"taskAutoInspectResultsService",taskAutoInspectResultsService);
            ReflectionTestUtils.setField(taskService,"messageService",messageService);
            ReflectionTestUtils.setField(taskService,"metadataInstancesService",metadataInstancesService);
            ReflectionTestUtils.setField(taskService,"historyService",historyService);
            ReflectionTestUtils.setField(taskService,"taskCollectionObjService",taskCollectionObjService);
        }
        @Test
        @DisplayName("test afterRemove method for migrate")
        void test1(){
            when(taskDto.getSyncType()).thenReturn("migrate");
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));
            doCallRealMethod().when(taskService).afterRemove(taskDto,user);
            taskService.afterRemove(taskDto,user);
            verify(messageService, new Times(1)).addMigration(taskDto.getDeleteName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, Level.WARN, user);
        }
        @Test
        @DisplayName("test afterRemove method for sync")
        void test2(){
            when(taskDto.getSyncType()).thenReturn("sync");
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));
            doCallRealMethod().when(taskService).afterRemove(taskDto,user);
            taskService.afterRemove(taskDto,user);
            verify(messageService, new Times(1)).addSync(taskDto.getDeleteName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, "", Level.WARN, user);
        }
    }
    @Nested
    class DeleteShareCache{
        private ObjectId id;
        @Test
        void test1(){
            id = mock(ObjectId.class);
            doCallRealMethod().when(taskService).deleteShareCache(id,user);
            taskService.deleteShareCache(id,user);
            verify(taskService).update(any(Query.class),any(Update.class));
        }
    }
    @Nested
    class CopyTest{
        private ObjectId id;
        private UserLogService userLogService;
        @BeforeEach
        void setUp(){
            id = mock(ObjectId.class);
            taskDto = mock(TaskDto.class);
            userLogService = mock(UserLogService.class);
            ReflectionTestUtils.setField(taskService,"userLogService",userLogService);
        }
        @Test
        void test1(){
            try (MockedStatic<SpringContextHelper> mb = Mockito
                    .mockStatic(SpringContextHelper.class)) {
                TaskServiceImpl service = mock(TaskServiceImpl.class);
                mb.when(()->SpringContextHelper.getBean(TaskServiceImpl.class)).thenReturn(service);
                id = mock(ObjectId.class);
                when(taskService.checkExistById(id,user)).thenReturn(taskDto);
                DAG dag = mock(DAG.class);
                List<Node> nodes = new ArrayList<>();
                DatabaseNode databaseNode = mock(DatabaseNode.class);
                nodes.add(databaseNode);
                when(databaseNode.getId()).thenReturn("111");
                MergeTableNode mergeTableNode = mock(MergeTableNode.class);
                nodes.add(mergeTableNode);
                when(mergeTableNode.getId()).thenReturn("222");
                MigrateFieldRenameProcessorNode renameProcessorNode = mock(MigrateFieldRenameProcessorNode.class);
                nodes.add(renameProcessorNode);
                when(renameProcessorNode.getId()).thenReturn("333");
                when(dag.getNodes()).thenReturn(nodes);
                when(taskDto.getDag()).thenReturn(dag);
                List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
                syncPoints.add(mock(TaskDto.SyncPoint.class));
                when(taskDto.getSyncPoints()).thenReturn(syncPoints);
                LinkedList<Edge> edges = new LinkedList<>();
                Edge edge = mock(Edge.class);
                edges.add(edge);
                when(edge.getSource()).thenReturn("source");
                when(edge.getTarget()).thenReturn("target");
                when(dag.getEdges()).thenReturn(edges);
                when(service.confirmById(any(TaskDto.class),any(UserDetail.class),anyBoolean())).thenReturn(taskDto);
                doCallRealMethod().when(taskService).copy(id,user);
                TaskDto actual = taskService.copy(id, user);
                assertEquals(taskDto,actual);
            }
        }
    }
    @Nested
    class RenewTest{
        private ObjectId id;
        private boolean system;
        private StateMachineService stateMachineService;
        private TaskResetLogService taskResetLogService;
        private DisruptorService disruptorService;
        @BeforeEach
        void setUp(){
            id = mock(ObjectId.class);
            stateMachineService = mock(StateMachineService.class);
            taskResetLogService = mock(TaskResetLogService.class);
            disruptorService = mock(DisruptorService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
            ReflectionTestUtils.setField(taskService,"taskResetLogService",taskResetLogService);
            ReflectionTestUtils.setField(taskService,"disruptorService",disruptorService);
        }
        @Test
        @DisplayName("test renew method when no agent")
        void test1(){
            system = true;
            when(taskService.checkExistById(id,user)).thenReturn(taskDto);
            when(taskService.findAgent(taskDto,user)).thenReturn(true);
            doCallRealMethod().when(taskService).renew(id,user,system);
            assertThrows(BizException.class,()->taskService.renew(id,user,system));
        }
        @Test
        @DisplayName("test renew method normal")
        void test2(){
            system = true;
            when(taskService.checkExistById(id,user)).thenReturn(taskDto);
            when(taskService.findAgent(taskDto,user)).thenReturn(false);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW, user)).thenReturn(stateMachineResult);
            when(stateMachineResult.isOk()).thenReturn(true);
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));
            doCallRealMethod().when(taskService).renew(id,user,system);
            taskService.renew(id,user,system);
            verify(disruptorService,new Times(1)).sendMessage(any(DisruptorTopicEnum.class),any(TaskRecord.class));
        }
        @Test
        @DisplayName("test renew method when state machine result is not ok")
        void test3(){
            system = true;
            when(taskService.checkExistById(id,user)).thenReturn(taskDto);
            when(taskService.findAgent(taskDto,user)).thenReturn(false);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW, user)).thenReturn(stateMachineResult);
            when(stateMachineResult.isOk()).thenReturn(false);
            doCallRealMethod().when(taskService).renew(id,user,system);
            assertThrows(BizException.class,()->taskService.renew(id,user,system));
        }
    }
    @Nested
    class AfterRenewTest{
        private TaskAutoInspectResultsService taskAutoInspectResultsService;
        private StateMachineService stateMachineService;
        @BeforeEach
        void setUp(){
            taskAutoInspectResultsService = mock(TaskAutoInspectResultsService.class);
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"taskAutoInspectResultsService",taskAutoInspectResultsService);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);

        }
        @Test
        void test1(){
            UpdateResult updateResult = mock(UpdateResult.class);
            when(taskService.renewNotSendMq(taskDto,user)).thenReturn(updateResult);
            when(updateResult.getMatchedCount()).thenReturn(1L);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_SUCCESS, user)).thenReturn(stateMachineResult);
            when(stateMachineResult.isFail()).thenReturn(true);
            doCallRealMethod().when(taskService).afterRenew(taskDto,user);
            taskService.afterRenew(taskDto,user);
            verify(taskAutoInspectResultsService,new Times(1)).cleanResultsByTask(taskDto);
        }
    }
    @Nested
    class CheckExistByIdTest{
        private ObjectId id;
        @BeforeEach
        void setUp(){
            id = mock(ObjectId.class);
        }
        @Test
        @DisplayName("test checkExistById method when taskDto is null")
        void test1(){
            when(taskService.findById(id,user)).thenReturn(null);
            doCallRealMethod().when(taskService).checkExistById(id,user);
            assertThrows(BizException.class,()->taskService.checkExistById(id,user));
        }
        @Test
        @DisplayName("test checkExistById method normal")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findById(id,user)).thenReturn(dto);
            doCallRealMethod().when(taskService).checkExistById(id,user);
            TaskDto actual = taskService.checkExistById(id, user);
            assertEquals(dto,actual);
        }
    }
    @Nested
    class CheckExistByIdWithFieldsTest{
        private ObjectId id;
        private String fields;
        @BeforeEach
        void setUp(){
            id = mock(ObjectId.class);
            fields = "test_field";
        }
        @Test
        @DisplayName("test checkExistById with fields method when taskDto is null")
        void test1(){
            when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(null);
            doCallRealMethod().when(taskService).checkExistById(id,user,fields);
            assertThrows(BizException.class,()->taskService.checkExistById(id,user,fields));
        }
        @Test
        @DisplayName("test checkExistById with fields method normal")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(dto);
            doCallRealMethod().when(taskService).checkExistById(id,user,fields);
            TaskDto actual = taskService.checkExistById(id, user, fields);
            assertEquals(dto,actual);
        }
    }
    @Nested
    class CheckExistByIdWithoutUser{
        private ObjectId id;
        private String fields;
        @BeforeEach
        void setUp(){
            id = mock(ObjectId.class);
            fields = "test_field";
        }
        @Test
        @DisplayName("test checkExistById without user method when taskDto is null")
        void test1(){
            when(taskService.findOne(any(Query.class))).thenReturn(null);
            doCallRealMethod().when(taskService).checkExistById(id,fields);
            assertThrows(BizException.class,()->taskService.checkExistById(id,fields));
        }
        @Test
        @DisplayName("test checkExistById without user method normal")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findOne(any(Query.class))).thenReturn(dto);
            doCallRealMethod().when(taskService).checkExistById(id,fields);
            TaskDto actual = taskService.checkExistById(id,fields);
            assertEquals(dto,actual);
        }
    }
    @Nested
    class BatchStopTest{
        private List<ObjectId> taskIds;
        private UserDetail user;
        private HttpServletRequest request;
        private HttpServletResponse response;
        private ObjectId id;
        private ExceptionHandler exceptionHandler;
        @BeforeEach
        void setUp(){
            taskIds = new ArrayList<>();
            id = mock(ObjectId.class);
            taskIds.add(id);
            request = mock(HttpServletRequest.class);
            response = mock(HttpServletResponse.class);
            exceptionHandler = mock(ExceptionHandler.class);
            ReflectionTestUtils.setField(taskService,"exceptionHandler",exceptionHandler);
        }
        @Test
        @DisplayName("test batchStop method with BizException")
        void test1(){
            BizException exception = new BizException("SystemError");
            doThrow(exception).when(taskService).pause(id,user,false);
            doCallRealMethod().when(taskService).batchStop(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchStop(taskIds, user, request, response);
            assertEquals("SystemError",responseMessages.get(0).getCode());
        }
        @Test
        @DisplayName("test batchStop method with exception")
        @SneakyThrows
        void test2(){
            Exception e = mock(RuntimeException.class);
            doThrow(e).when(taskService).pause(id,user,false);
            ResponseMessage responseMessage = mock(ResponseMessage.class);
            when(responseMessage.getCode()).thenReturn("error_code");
            when(exceptionHandler.handlerException(e,request,response)).thenReturn(responseMessage);
            doCallRealMethod().when(taskService).batchStop(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchStop(taskIds, user, request, response);
            assertEquals("error_code",responseMessages.get(0).getCode());
        }
    }
    @Nested
    class BatchDeleteTest{
        private List<ObjectId> taskIds;
        private UserDetail user;
        private HttpServletRequest request;
        private HttpServletResponse response;
        private ObjectId id;
        private InspectService inspectService;
        private ExceptionHandler exceptionHandler;
        @BeforeEach
        void setUp(){
            taskIds = new ArrayList<>();
            id = mock(ObjectId.class);
            taskIds.add(id);
            request = mock(HttpServletRequest.class);
            response = mock(HttpServletResponse.class);
            inspectService = mock(InspectService.class);
            ReflectionTestUtils.setField(taskService,"inspectService",inspectService);
            exceptionHandler = mock(ExceptionHandler.class);
            ReflectionTestUtils.setField(taskService,"exceptionHandler",exceptionHandler);

        }
        @Test
        @DisplayName("test batchDelete method normal")
        void test1(){
            doCallRealMethod().when(taskService).batchDelete(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchDelete(taskIds, user, request, response);
            assertEquals("ok",responseMessages.get(0).getCode());
        }
        @Test
        @DisplayName("test batchDelete method with BizExcception")
        void test2(){
            BizException exception = new BizException("Clear.Slot");
            doThrow(exception).when(taskService).remove(id,user);
            doCallRealMethod().when(taskService).batchDelete(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchDelete(taskIds, user, request, response);
            assertEquals("Clear.Slot",responseMessages.get(0).getCode());
        }
        @Test
        @DisplayName("test batchDelete method with exception")
        @SneakyThrows
        void test3(){
            Exception e = mock(RuntimeException.class);
            doThrow(e).when(taskService).remove(id,user);
            ResponseMessage responseMessage = mock(ResponseMessage.class);
            when(responseMessage.getCode()).thenReturn("error_code");
            when(exceptionHandler.handlerException(e,request,response)).thenReturn(responseMessage);
            doCallRealMethod().when(taskService).batchDelete(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchDelete(taskIds, user, request, response);
            assertEquals("error_code",responseMessages.get(0).getCode());
        }
    }
    @Nested
    class BatchRenewTest{
        private List<ObjectId> taskIds;
        private UserDetail user;
        private HttpServletRequest request;
        private HttpServletResponse response;
        private ObjectId id;
        private ExceptionHandler exceptionHandler;
        @BeforeEach
        void setUp(){
            taskIds = new ArrayList<>();
            id = mock(ObjectId.class);
            taskIds.add(id);
            request = mock(HttpServletRequest.class);
            response = mock(HttpServletResponse.class);
            exceptionHandler = mock(ExceptionHandler.class);
            ReflectionTestUtils.setField(taskService,"exceptionHandler",exceptionHandler);

        }
        @Test
        @DisplayName("test batchRenew method normal")
        void test1(){
            doCallRealMethod().when(taskService).batchRenew(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchRenew(taskIds, user, request, response);
            assertEquals("ok",responseMessages.get(0).getCode());
        }
        @Test
        @DisplayName("test batchRenew method with BizExcception")
        void test2(){
            BizException exception = new BizException("Task.ResetAgentNotFound");
            doThrow(exception).when(taskService).renew(id,user);
            doCallRealMethod().when(taskService).batchRenew(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchRenew(taskIds, user, request, response);
            assertEquals("Task.ResetAgentNotFound",responseMessages.get(0).getCode());
        }
        @Test
        @DisplayName("test batchRenew method with exception")
        @SneakyThrows
        void test3(){
            Exception e = mock(RuntimeException.class);
            doThrow(e).when(taskService).renew(id,user);
            ResponseMessage responseMessage = mock(ResponseMessage.class);
            when(responseMessage.getCode()).thenReturn("error_code");
            when(exceptionHandler.handlerException(e,request,response)).thenReturn(responseMessage);
            doCallRealMethod().when(taskService).batchRenew(taskIds,user,request,response);
            List<MutiResponseMessage> responseMessages = taskService.batchRenew(taskIds, user, request, response);
            assertEquals("error_code",responseMessages.get(0).getCode());
        }
    }
    @Nested
    class FindTest{
        private Filter filter;
        private TaskRepository repository;
        private MetadataTransformerService transformerService;
        @BeforeEach
        void setUp(){
            filter = new Filter();
            user = mock(UserDetail.class);
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            when(repository.getMongoOperations()).thenReturn(mock(MongoTemplate.class));
            new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
            transformerService = mock(MetadataTransformerService.class);
            ReflectionTestUtils.setField(taskService,"transformerService",transformerService);
        }
        @Test
        @DisplayName("test find method when is agent request")
        void test1(){
            taskService = spy(new TaskServiceImpl(mock(TaskRepository.class)));
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("Java");
                taskService.find(filter, user);
                verify(taskService,new Times(1)).deleteNotifyEnumData(anyList());
            }
        }
        @Test
        @DisplayName("test find method when sync type not instanceof String")
        void test2(){
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("");
                filter.setWhere(null);
                taskService.find(filter, user);
                verify(taskService,new Times(0)).findDataCopyList(filter,user);
                verify(taskService,new Times(0)).findDataDevList(filter,user);
            }
        }
        @Test
        @DisplayName("test find method when sync type is migrate")
        void test3(){
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("");
                Where where = mock(Where.class);
                when(where.get("syncType")).thenReturn("migrate");
                filter.setWhere(where);
                taskService.find(filter, user);
                verify(taskService,new Times(1)).findDataCopyList(filter,user);
            }
        }
        @Test
        @DisplayName("test find method when sync type is sync")
        void test4(){
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("");
                Where where = mock(Where.class);
                when(where.get("syncType")).thenReturn("sync");
                filter.setWhere(where);
                when(repository.filterToQuery(filter)).thenReturn(mock(Query.class));
                taskService.find(filter, user);
                verify(taskService,new Times(1)).findDataDevList(filter,user);
            }
        }
        @Test
        @DisplayName("test find method when sync type is heartbeat")
        void test5(){
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("");
                Where where = mock(Where.class);
                when(where.get("syncType")).thenReturn("connHeartbeat");
                filter.setWhere(where);
                when(repository.filterToQuery(filter)).thenReturn(mock(Query.class));
                taskService.find(filter, user);
                verify(taskService,new Times(1)).findDataDevList(filter,user);
            }
        }
        @Test
        @DisplayName("test find method")
        void test6(){
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                MetadataTransformerService transformerService = mock(MetadataTransformerService.class);
                ReflectionTestUtils.setField(taskService,"transformerService",transformerService);
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("");
                Where where = mock(Where.class);
                when(where.get("syncType")).thenReturn("connHeartbeat");
                filter.setWhere(where);
                when(repository.filterToQuery(filter)).thenReturn(mock(Query.class));
                Page taskDtoPage = new Page<>();
                List<TaskDto> taskDtos = new ArrayList<>();
                TaskDto dto = new TaskDto();
                taskDtos.add(dto);
                taskDtoPage.setItems(taskDtos);
                dto.setId(new ObjectId("65bc933c6129fe73d7858a8e"));
                dto.setShareCdcStopMessage("share cdc message");
                doReturn(taskDtoPage).when(taskService).findDataDevList(filter,user);
                List<MetadataTransformerDto> transformerDtos = new ArrayList<>();
                MetadataTransformerDto metadataTransformerDto = mock(MetadataTransformerDto.class);
                when(metadataTransformerDto.getDataFlowId()).thenReturn("65bc933c6129fe73d7858a8e");
                when(metadataTransformerDto.getStatus()).thenReturn("running");
                transformerDtos.add(metadataTransformerDto);
                when(transformerService.findAll(any(Query.class))).thenReturn(transformerDtos);
                Page<TaskDto> actual = taskService.find(filter, user);
                assertEquals("running",actual.getItems().get(0).getTransformStatus());
            }
        }
    }
    @Nested
    class DeleteNotifyEnumDataTest{
        private List<TaskDto> taskDtoList;
        @BeforeEach
        void setUp(){
            taskDtoList = new ArrayList<>();
        }
        @Test
        @DisplayName("test deleteNotifyEnumData method for empty taskDtoList")
        void test1(){
            doCallRealMethod().when(taskService).deleteNotifyEnumData(taskDtoList);
            taskService.deleteNotifyEnumData(taskDtoList);
        }
        @Test
        @DisplayName("test deleteNotifyEnumData method normal")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            List<AlarmSettingVO> alarmSettings = new ArrayList<>();
            AlarmSettingVO alarmSettingVO = mock(AlarmSettingVO.class);
            alarmSettings.add(alarmSettingVO);
            when(dto.getAlarmSettings()).thenReturn(alarmSettings);
            taskDtoList.add(dto);
            DAG dag = mock(DAG.class);
            when(dto.getDag()).thenReturn(dag);
            List<Node> nodes = new ArrayList<>();
            Node node = mock(Node.class);
            when(node.getAlarmSettings()).thenReturn(alarmSettings);
            nodes.add(node);
            when(dag.getNodes()).thenReturn(nodes);
            List<NotifyEnum> notifyEnums = new ArrayList<>();
            notifyEnums.add(NotifyEnum.SMS);
            when(alarmSettingVO.getNotify()).thenReturn(notifyEnums);
            doCallRealMethod().when(taskService).deleteNotifyEnumData(taskDtoList);
            taskService.deleteNotifyEnumData(taskDtoList);
        }
    }
    @Nested
    class FindDataCopyListTest{
        private Filter filter;
        private InspectService inspectService;
        private TaskRepository repository;
        @BeforeEach
        void setUp(){
            filter = new Filter();
            inspectService = mock(InspectService.class);
            repository = mock(TaskRepository.class);
            ReflectionTestUtils.setField(taskService,"inspectService",inspectService);
            ReflectionTestUtils.setField(taskService,"repository",repository);
            new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
        }
        @Test
        void test1(){
            Where where = new Where();
            where.put("inspectResult","agreement");
            filter.setWhere(where);
            when(taskService.parseOrToCriteria(where)).thenReturn(mock(Criteria.class));
            MongoTemplate template = mock(MongoTemplate.class);
            when(repository.getMongoOperations()).thenReturn(template);
            List<TaskEntity> taskEntityList = new ArrayList<>();
            TaskEntity task = new TaskEntity();
            ObjectId id = mock(ObjectId.class);
            task.setId(id);
            taskEntityList.add(task);
            when(template.find(any(Query.class),any(Class.class))).thenReturn(taskEntityList);
            List<InspectDto> inspectDtoList = new ArrayList<>();
            InspectDto inspectDto = new InspectDto();
            String flowId = "111";
            when(id.toHexString()).thenReturn(flowId);
            inspectDto.setFlowId(flowId);
            inspectDto.setId(mock(ObjectId.class));
            inspectDtoList.add(inspectDto);
            when(inspectService.findByTaskIdList(anyList())).thenReturn(inspectDtoList);
            doCallRealMethod().when(taskService).findDataCopyList(filter,user);
            Page<TaskDto> actual = taskService.findDataCopyList(filter, user);
            assertEquals(id,actual.getItems().get(0).getId());
        }
    }
    @Nested
    class GetSourceNodeTest{
        @Test
        @DisplayName("test getSourceNode method when dag is null")
        void test1(){
            when(taskDto.getDag()).thenReturn(null);
            doCallRealMethod().when(taskService).getSourceNode(taskDto);
            Node actual = taskService.getSourceNode(taskDto);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test getSourceNode method normal")
        void test2(){
            DAG dag = mock(DAG.class);
            LinkedList<Edge> edges = new LinkedList();
            Edge edge = mock(Edge.class);
            edges.add(edge);
            when(edge.getSource()).thenReturn("111");
            when(dag.getEdges()).thenReturn(edges);
            List<Node> nodeList = new ArrayList<>();
            Node node = mock(Node.class);
            nodeList.add(node);
            when(node.getId()).thenReturn("111");
            when(dag.getNodes()).thenReturn(nodeList);
            when(taskDto.getDag()).thenReturn(dag);
            doCallRealMethod().when(taskService).getSourceNode(taskDto);
            Node actual = taskService.getSourceNode(taskDto);
            assertEquals(nodeList.get(0),actual);
        }
        @Test
        @DisplayName("test getSourceNode method when edges is empty")
        void test3(){
            DAG dag = mock(DAG.class);
            LinkedList<Edge> edges = new LinkedList();
            when(dag.getEdges()).thenReturn(edges);
            when(taskDto.getDag()).thenReturn(dag);
            doCallRealMethod().when(taskService).getSourceNode(taskDto);
            Node actual = taskService.getSourceNode(taskDto);
            assertEquals(null,actual);
        }
    }
    @Nested
    class GetTargetNodeTest{
        @BeforeEach
        void beforeEach(){
            taskDto = mock(TaskDto.class);
            taskService = mock(TaskServiceImpl.class);
        }
        @Test
        @DisplayName("test getTargetNode method normal")
        void test1(){
            DAG dag = mock(DAG.class);
            LinkedList<Edge> edges = new LinkedList();
            Edge edge = mock(Edge.class);
            edges.add(edge);
            when(edge.getTarget()).thenReturn("111");
            when(dag.getEdges()).thenReturn(edges);
            List<Node> nodeList = new ArrayList<>();
            Node node = mock(Node.class);
            nodeList.add(node);
            when(node.getId()).thenReturn("111");
            when(dag.getNodes()).thenReturn(nodeList);
            when(taskDto.getDag()).thenReturn(dag);
            doCallRealMethod().when(taskService).getTargetNode(taskDto);
            Node actual = taskService.getTargetNode(taskDto);
            assertEquals(nodeList.get(0),actual);
        }
        @Test
        @DisplayName("test getTargetNode method when edges is empty")
        void test2(){
            DAG dag = mock(DAG.class);
            LinkedList<Edge> edges = new LinkedList();
            when(dag.getEdges()).thenReturn(edges);
            when(taskDto.getDag()).thenReturn(dag);
            doCallRealMethod().when(taskService).getTargetNode(taskDto);
            Node actual = taskService.getTargetNode(taskDto);
            assertEquals(null,actual);
        }

    }
    @Nested
    class SearchLogCollectorTest{
        @Test
        void test1(){
            String key = "111";
            doCallRealMethod().when(taskService).searchLogCollector(key);
            LogCollectorResult actual = taskService.searchLogCollector(key);
            assertEquals(LogCollectorResult.class,actual.getClass());

        }
    }
    @Nested
    class CreateShareCacheTaskTest{
        private SaveShareCacheParam saveShareCacheParam;
        private UserDetail user;
        private HttpServletRequest request;
        private HttpServletResponse response;
        @BeforeEach
        void beforeEach(){
            saveShareCacheParam = mock(SaveShareCacheParam.class);
            user = mock(UserDetail.class);
            request = mock(HttpServletRequest.class);
            response = mock(HttpServletResponse.class);
        }
        @Test
        void test(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.confirmById(any(TaskDto.class),any(UserDetail.class),anyBoolean())).thenReturn(dto);
            doCallRealMethod().when(taskService).createShareCacheTask(saveShareCacheParam,user,request,response);
            TaskDto actual = taskService.createShareCacheTask(saveShareCacheParam, user, request, response);
            assertEquals(dto,actual);
            verify(taskService,new Times(1)).batchStart(anyList(),any(UserDetail.class),any(HttpServletRequest.class),any(HttpServletResponse.class));
        }
    }
    @Nested
    class FindShareCacheTest{
        private Filter filter;
        private TaskRepository repository;
        private DataSourceServiceImpl dataSourceService;
        private ExternalStorageService externalStorageService;
        @BeforeEach
        void beforeEach(){
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            user = mock(UserDetail.class);
            filter = new Filter();
            dataSourceService = mock(DataSourceServiceImpl.class);
            ReflectionTestUtils.setField(taskService,"dataSourceService",dataSourceService);
            new DataPermissionHelper(mock(IDataPermissionHelper.class));
            externalStorageService = mock(ExternalStorageService.class);
            ReflectionTestUtils.setField(taskService,"externalStorageService",externalStorageService);
        }
        @Test
        void test(){
            Where where = new Where();
            where.put("connectionName",new HashMap<>());
            filter.setWhere(where);
            List<TaskEntity> tasks = new ArrayList<>();
            TaskEntity taskEntity = new TaskEntity();
            ObjectId taskId = mock(ObjectId.class);
            taskEntity.setId(taskId);
            taskEntity.setName("test");
            tasks.add(taskEntity);
            when(repository.findAll(filter,user)).thenReturn(tasks);
            TableNode sourceNode = mock(TableNode.class);
            doReturn(sourceNode).when(taskService).getSourceNode(any(TaskDto.class));
            when(dataSourceService.findOne(any(Query.class))).thenReturn(mock(DataSourceConnectionDto.class));
            when(sourceNode.getAttrs()).thenReturn(new HashMap<>());
            CacheNode cacheNode = mock(CacheNode.class);
            doReturn(cacheNode).when(taskService).getTargetNode(any(TaskDto.class));
            ObjectId id = mock(ObjectId.class);
            when(id.toHexString()).thenReturn("111");
            String externalStorageId = id.toHexString();
            when(cacheNode.getExternalStorageId()).thenReturn(externalStorageId);
            when(externalStorageService.findById(any())).thenReturn(mock(ExternalStorageDto.class));
            doCallRealMethod().when(taskService).findShareCache(filter,user);
            Page<ShareCacheVo> actual = taskService.findShareCache(filter, user);
            assertEquals("test",actual.getItems().get(0).getName());
        }
    }
    @Nested
    class FindShareCacheByIdTest{
        private String id;
        private DataSourceServiceImpl dataSourceService;
        @BeforeEach
        void beforeEach(){
            id = "111";
            dataSourceService = mock(DataSourceServiceImpl.class);
            ReflectionTestUtils.setField(taskService,"dataSourceService",dataSourceService);
        }
        @Test
        @DisplayName("test findShareCacheById method when source node is null")
        void test1(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findById(MongoUtils.toObjectId(id))).thenReturn(dto);
            when(taskService.getSourceNode(dto)).thenReturn(null);
            doCallRealMethod().when(taskService).findShareCacheById(id);
            assertThrows(BizException.class,()->taskService.findShareCacheById(id));
        }
        @Test
        @DisplayName("test findShareCacheById method normal")
        void test2(){
            TaskDto dto = mock(TaskDto.class);
            when(taskService.findById(MongoUtils.toObjectId(id))).thenReturn(dto);
            TableNode dataNode = mock(TableNode.class);
            when(taskService.getSourceNode(dto)).thenReturn(dataNode);
            CacheNode cacheNode = mock(CacheNode.class);
            when(taskService.getTargetNode(dto)).thenReturn(cacheNode);
            when(dataNode.getConnectionId()).thenReturn("222");
            DataSourceConnectionDto connectionDto = mock(DataSourceConnectionDto.class);
            when(dataSourceService.findOne(any(Query.class))).thenReturn(connectionDto);
            ObjectId objectId = mock(ObjectId.class);
            when(connectionDto.getId()).thenReturn(objectId);
            when(dataNode.getAttrs()).thenReturn(new HashMap<>());
            when(dto.getCurrentEventTimestamp()).thenReturn(1713846744L);
            doCallRealMethod().when(taskService).findShareCacheById(id);
            ShareCacheDetailVo actual = taskService.findShareCacheById(id);
            assertEquals(objectId.toString(),actual.getConnectionId());
        }
    }
    @Nested
    class ParseCacheToTaskDtoTest{
        private SaveShareCacheParam saveShareCacheParam;
        private TaskDto taskDto;
        private HashMap sourceNodeMap;
        private DataSourceServiceImpl dataSourceService;
        @BeforeEach
        void beforeEach(){
            saveShareCacheParam = mock(SaveShareCacheParam.class);
            taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            List<Node> nodes = new ArrayList<>();
            nodes.add(mock(Node.class));
            when(dag.getSources()).thenReturn(nodes);
            when(dag.getTargets()).thenReturn(nodes);
            Map map = new HashMap();
            ArrayList<Map> list = new ArrayList<>();
            sourceNodeMap = mock(HashMap.class);
            list.add(sourceNodeMap);
            HashMap sourceNode1 = new HashMap();
            list.add(sourceNode1);
            map.put("nodes",list);
            when(saveShareCacheParam.getDag()).thenReturn(map);
            dataSourceService = mock(DataSourceServiceImpl.class);
            ReflectionTestUtils.setField(taskService,"dataSourceService",dataSourceService);
        }
        @Test
        @DisplayName("test parseCacheToTaskDto method when connectionName is null")
        void test1(){
            doCallRealMethod().when(taskService).parseCacheToTaskDto(saveShareCacheParam,taskDto);
            assertThrows(BizException.class,()->taskService.parseCacheToTaskDto(saveShareCacheParam,taskDto));
        }
        @Test
        @DisplayName("test parseCacheToTaskDto method normal")
        void test2(){
            try (MockedStatic<DAG> mb = Mockito
                    .mockStatic(DAG.class)) {
                DAG dag = mock(DAG.class);
                mb.when(()->DAG.build(any(Dag.class))).thenReturn(dag);
                when(sourceNodeMap.get("connectionId")).thenReturn("66225a960f79853715856105");
                DataSourceConnectionDto dto = mock(DataSourceConnectionDto.class);
                when(dto.getName()).thenReturn("test");
                when(dataSourceService.findById(any(ObjectId.class),any(Field.class))).thenReturn(dto);
                when(sourceNodeMap.get("attrs")).thenReturn(new HashMap<>());
                doCallRealMethod().when(taskService).parseCacheToTaskDto(saveShareCacheParam,taskDto);
                TaskDto actual = taskService.parseCacheToTaskDto(saveShareCacheParam, taskDto);
                assertEquals(dag,actual.getDag());
            }
        }
    }
    @Nested
    class InspectChartTest{
        private TaskAutoInspectResultsService taskAutoInspectResultsService;
        @BeforeEach
        void beforeEach(){
            taskAutoInspectResultsService = mock(TaskAutoInspectResultsService.class);
            ReflectionTestUtils.setField(taskService,"taskAutoInspectResultsService",taskAutoInspectResultsService);
        }
        @Test
        void test1(){
            List<TaskDto> taskDtos = new ArrayList<>();
            TaskDto dto = mock(TaskDto.class);
            taskDtos.add(dto);
            when(taskService.findAllDto(any(Query.class),any(UserDetail.class))).thenReturn(taskDtos);
            when(dto.getCanOpenInspect()).thenReturn(true);
            when(dto.getStatus()).thenReturn("error");
            ObjectId id = mock(ObjectId.class);
            when(dto.getId()).thenReturn(id);
            Set<String> taskSet = new HashSet<>();
            taskSet.add(id.toHexString());
            when(taskAutoInspectResultsService.groupByTask(user)).thenReturn(taskSet);
            doCallRealMethod().when(taskService).inspectChart(user);
            Map<String, Integer> actual = taskService.inspectChart(user);
            assertEquals(1,actual.get("error"));
            assertEquals(1,actual.get("can"));
            assertEquals(1,actual.get("diff"));
        }
    }
    @Nested
    class GetDataCopyChartTest{
        private List<TaskDto> migrateList;
        @Test
        void test1(){
            migrateList = new ArrayList<>();
            TaskDto dto1 = mock(TaskDto.class);
            TaskDto dto2 = mock(TaskDto.class);
            migrateList.add(dto1);
            migrateList.add(dto2);
            when(dto1.getStatus()).thenReturn("paused");
            when(dto1.getStatus()).thenReturn("scheduling");
            doCallRealMethod().when(taskService).getDataCopyChart(migrateList);
            Map<String, Object> actual = taskService.getDataCopyChart(migrateList);
            assertEquals(2,actual.get("total"));
        }
    }
    @Nested
    class GetDataDevChartTest{
        private List<TaskDto> synList;
        @Test
        void test(){
            synList = new ArrayList<>();
            TaskDto dto1 = mock(TaskDto.class);
            TaskDto dto2 = mock(TaskDto.class);
            synList.add(dto1);
            synList.add(dto2);
            doCallRealMethod().when(taskService).getDataDevChart(synList);
            Map<String, Object> actual = taskService.getDataDevChart(synList);
            assertEquals(2,actual.size());
        }
    }
    @Nested
    class FindByIdsTest{
        @Test
        void test(){
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            List<ObjectId> idList = new ArrayList<>();
            ObjectId id = mock(ObjectId.class);
            idList.add(id);
            List<TaskEntity> taskEntityList = new ArrayList<>();
            taskEntityList.add(mock(TaskEntity.class));
            MongoTemplate template = mock(MongoTemplate.class);
            when(repository.getMongoOperations()).thenReturn(template);
            when(template.find(any(Query.class),any(Class.class))).thenReturn(taskEntityList);
            List<TaskEntity> actual = taskService.findByIds(idList);
            assertEquals(taskEntityList,actual);
        }
    }
    @Nested
    class FindTaskDetailByIdTest{
        private String id;
        private Field field;
        TaskRepository repository;
        @BeforeEach
        void setUp(){
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
            user = mock(UserDetail.class);
            new DataPermissionHelper(mock(IDataPermissionHelper.class));
            field = mock(Field.class);
            ObjectId objectId = new ObjectId("6613954dc8a36646da142da3");
            id = objectId.toHexString();
        }
        @Test
        @DisplayName("test findTaskDetailById method for initial sync")
        void test1(){
            TaskEntity task = new TaskEntity();
            task.setId(mock(ObjectId.class));
            task.setType("initial_sync");
            Optional<TaskEntity> entity = Optional.ofNullable(task);
            when(repository.findById(any(ObjectId.class),any(Field.class),any(UserDetail.class))).thenReturn(entity);
            Date date = new Date();
            doReturn(date).when(taskService).getMillstoneTime(any(TaskDto.class),anyString(),anyString());
            doCallRealMethod().when(taskService).findTaskDetailById(id,field,user);
            TaskDetailVo actual = taskService.findTaskDetailById(id, field, user);
            assertEquals(date,actual.getInitStartTime());
            assertEquals("initial_sync",actual.getType());
        }
        @Test
        @DisplayName("test findTaskDetailById method for cdc")
        void test2(){
            TaskEntity task = new TaskEntity();
            task.setId(mock(ObjectId.class));
            task.setType("cdc");
            Optional<TaskEntity> entity = Optional.ofNullable(task);
            when(repository.findById(any(ObjectId.class),any(Field.class),any(UserDetail.class))).thenReturn(entity);
            Date date = new Date();
            doReturn(date).when(taskService).getMillstoneTime(any(TaskDto.class),anyString(),anyString());
            doCallRealMethod().when(taskService).findTaskDetailById(id,field,user);
            TaskDetailVo actual = taskService.findTaskDetailById(id, field, user);
            assertEquals(date,actual.getCdcStartTime());
            assertEquals("cdc",actual.getType());
        }
        @Test
        @DisplayName("test findTaskDetailById method for initial sync")
        void test3(){
            TaskEntity task = new TaskEntity();
            task.setId(mock(ObjectId.class));
            task.setType("initial_sync+cdc");
            Optional<TaskEntity> entity = Optional.ofNullable(task);
            when(repository.findById(any(ObjectId.class),any(Field.class),any(UserDetail.class))).thenReturn(entity);
            Date date = new Date();
            doReturn(date).when(taskService).getMillstoneTime(any(TaskDto.class),anyString(),anyString());
            doCallRealMethod().when(taskService).findTaskDetailById(id,field,user);
            TaskDetailVo actual = taskService.findTaskDetailById(id, field, user);
            assertEquals(date,actual.getInitStartTime());
            assertEquals("initial_sync+cdc",actual.getType());
        }
    }
    @Nested
    class GetLastHourTest{
        private SnapshotEdgeProgressService snapshotEdgeProgressService;
        @Test
        void test1(){
            try (MockedStatic<DateUtil> mb = Mockito
                    .mockStatic(DateUtil.class)) {
                Date start = new Date();
                Date end = new Date();
                mb.when(()->DateUtil.between(start,end,DateUnit.MS)).thenThrow(new RuntimeException());
                String taskId = "111";
                snapshotEdgeProgressService = mock(SnapshotEdgeProgressService.class);
                taskService = mock(TaskServiceImpl.class);
                ReflectionTestUtils.setField(taskService,"snapshotEdgeProgressService",snapshotEdgeProgressService);
                FullSyncVO fullSyncVO = mock(FullSyncVO.class);
                when(snapshotEdgeProgressService.syncOverview(taskId)).thenReturn(fullSyncVO);
                when(fullSyncVO.getStartTs()).thenReturn(start);
                when(fullSyncVO.getEndTs()).thenReturn(end);
                doCallRealMethod().when(taskService).getLastHour(taskId);
                Long actual = taskService.getLastHour(taskId);
                assertEquals(null,actual);
            }
        }
    }
    @Nested
    class GetMillstoneTimeTest{
        private String code;
        private String group;
        @Test
        void test1(){
            code = "111";
            group = "222";
            taskService = mock(TaskServiceImpl.class);
            taskDto = mock(TaskDto.class);
            doCallRealMethod().when(taskService).getMillstoneTime(taskDto,code,group);
            Date actual = taskService.getMillstoneTime(taskDto, code, group);
            assertEquals(null,actual);
        }
    }
    @Nested
    class CheckRunTest{
        private String taskId = "111";
        private TaskDto dto;
        @BeforeEach
        void setUp(){
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(MongoUtils.toObjectId(taskId), user, "status")).thenReturn(dto);
        }
        @Test
        @DisplayName("test checkRun method for edit status")
        void test1(){
            when(dto.getStatus()).thenReturn("edit");
            doCallRealMethod().when(taskService).checkRun(taskId,user);
            Boolean actual = taskService.checkRun(taskId, user);
            assertEquals(true,actual);
        }
        @Test
        @DisplayName("test checkRun method for running status")
        void test2(){
            when(dto.getStatus()).thenReturn("running");
            doCallRealMethod().when(taskService).checkRun(taskId,user);
            Boolean actual = taskService.checkRun(taskId, user);
            assertEquals(false,actual);
        }

    }
    @Nested
    class FindTransformParamTest{
        private String taskId = "111";
        private TransformSchemaService transformSchemaService;
        @Test
        void test1(){
            taskService = spy(new TaskServiceImpl(mock(TaskRepository.class)));
            transformSchemaService = mock(TransformSchemaService.class);
            ReflectionTestUtils.setField(taskService,"transformSchemaService",transformSchemaService);
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                TaskDto dto = mock(TaskDto.class);
                doReturn(dto).when(taskService).checkExistById(MongoUtils.toObjectId(taskId), user);
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("Java");
                doNothing().when(taskService).deleteNotifyEnumData(anyList());
                taskService.findTransformParam(taskId,user);
                verify(transformSchemaService,new Times(1)).getTransformParam(dto,user);
            }
        }
    }
    @Nested
    class FindTransformAllParam{
        private String taskId = "111";
        private TransformSchemaService transformSchemaService;
        @Test
        void test1(){
            taskService = spy(new TaskServiceImpl(mock(TaskRepository.class)));
            transformSchemaService = mock(TransformSchemaService.class);
            ReflectionTestUtils.setField(taskService,"transformSchemaService",transformSchemaService);
            try (MockedStatic<RequestContextHolder> mb = Mockito
                    .mockStatic(RequestContextHolder.class)) {
                doReturn(mock(TaskDto.class)).when(taskService).checkExistById(MongoUtils.toObjectId(taskId), user);
                ServletRequestAttributes attributes = mock(ServletRequestAttributes.class);
                mb.when(RequestContextHolder::currentRequestAttributes).thenReturn(attributes);
                HttpServletRequest mockReq = mock(HttpServletRequest.class);
                when(attributes.getRequest()).thenReturn(mockReq);
                when(mockReq.getHeader("user-agent")).thenReturn("Java");
                doNothing().when(taskService).deleteNotifyEnumData(anyList());
                taskService.findTransformAllParam(taskId,user);
                verify(taskService,new Times(1)).deleteNotifyEnumData(anyList());
            }
        }
    }
    @Nested
    class SendRenewMqTest{
        private String opType;
        private MessageQueueServiceImpl messageQueueService;
        @Test
        @DisplayName("test sendRenewMq method normal")
        void test1(){
            messageQueueService = mock(MessageQueueServiceImpl.class);
            ReflectionTestUtils.setField(taskService,"messageQueueService",messageQueueService);
            taskDto = mock(TaskDto.class);
            opType = "test";
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));
            doCallRealMethod().when(taskService).sendRenewMq(taskDto,user,opType);
            taskService.sendRenewMq(taskDto,user,opType);
            verify(messageQueueService,new Times(1)).sendMessage(any(MessageQueueDto.class));
        }

    }
    @Nested
    class FindAgentTest{
        private AgentGroupService agentGroupService;
        private Worker worker;
        @BeforeEach
        void beforeEach(){
            workerService = mock(WorkerService.class);
            ReflectionTestUtils.setField(taskService,"workerService",workerService);
            taskDto = new TaskDto();
            user = mock(UserDetail.class);
            agentGroupService = mock(AgentGroupService.class);
            ReflectionTestUtils.setField(taskService,"agentGroupService",agentGroupService);
            List<Worker> list = new ArrayList<>();
            worker = mock(Worker.class);
            list.add(worker);
            when(workerService.findAvailableAgent(user)).thenReturn(list);
        }
        @Test
        @DisplayName("test findAgent method when findProcessNodeListWithGroup return true")
        void test1(){
            List<Worker> list = new ArrayList<>();
            list.add(mock(Worker.class));
            when(workerService.findAvailableAgent(user)).thenReturn(list);
            when(taskService.findProcessNodeListWithGroup(any(TaskDto.class),anyList(),any(UserDetail.class))).thenReturn(true);
            doCallRealMethod().when(taskService).findAgent(taskDto,user);
            boolean actual = taskService.findAgent(taskDto, user);
            assertEquals(true,actual);
        }
        @Test
        @DisplayName("test findAgent method when taskDto getAccessNodeType is manually")
        void test2(){
            doCallRealMethod().when(taskService).findAgent(taskDto,user);
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER");
            List<String> idList = new ArrayList<>();
            idList.add("111");
            doCallRealMethod().when(taskService).findProcessNodeListWithGroup(any(TaskDto.class),anyList(),any(UserDetail.class));
            when(agentGroupService.getProcessNodeListWithGroup(taskDto,user)).thenReturn(idList);
            when(worker.getProcessId()).thenReturn("111");
            boolean actual = taskService.findAgent(taskDto, user);
            assertEquals(false,actual);
            assertEquals("111",taskDto.getAgentId());
        }
        @Test
        @DisplayName("test findAgent method when agentId is null")
        void test3(){
            doCallRealMethod().when(taskService).findAgent(taskDto,user);
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER");
            List<String> idList = new ArrayList<>();
            idList.add("111");
            doCallRealMethod().when(taskService).findProcessNodeListWithGroup(any(TaskDto.class),anyList(),any(UserDetail.class));
            when(agentGroupService.getProcessNodeListWithGroup(taskDto,user)).thenReturn(idList);
            when(worker.getProcessId()).thenReturn("222");
            boolean actual = taskService.findAgent(taskDto, user);
            assertEquals(true,actual);
            assertEquals(null,taskDto.getAgentId());
        }
        @Test
        @DisplayName("test findAgent method when taskDto getAccessNodeType is not manually")
        void test4(){
            doCallRealMethod().when(taskService).findAgent(taskDto,user);
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER");
            when(worker.getProcessId()).thenReturn("111");
            boolean actual = taskService.findAgent(taskDto, user);
            assertEquals(false,actual);
            assertEquals("111",taskDto.getAgentId());
        }
        @Test
        @DisplayName("test findAgent method without availableAgent")
        void test45(){
            List<Worker> list = new ArrayList<>();
            when(workerService.findAvailableAgent(user)).thenReturn(list);
            doCallRealMethod().when(taskService).findAgent(taskDto,user);
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER");
            boolean actual = taskService.findAgent(taskDto, user);
            assertEquals(true,actual);
            assertEquals(null,taskDto.getAgentId());
        }
    }
    @Nested
    class StartTest{
        private ObjectId id;
        private TaskDto dto;
        private MonitoringLogsService monitoringLogsService;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            dto = mock(TaskDto.class);
            monitoringLogsService = mock(MonitoringLogsService.class);
            ReflectionTestUtils.setField(taskService,"monitoringLogsService",monitoringLogsService);
        }
        @Test
        @DisplayName("test start method normal")
        void test1(){
            when(taskService.checkExistById(id,user)).thenReturn(dto);
            doCallRealMethod().when(taskService).start(id,user);
            taskService.start(id,user);
            verify(taskService,new Times(1)).start(dto,user,"11");
        }
        @Test
        @DisplayName("test start method with exception")
        void test2(){
            when(taskService.checkExistById(id,user)).thenReturn(dto);
            BizException exception = mock(BizException.class);
            doThrow(exception).when(taskService).start(dto,user,"11");
            doCallRealMethod().when(taskService).start(id,user);
            assertThrows(Exception.class,()->taskService.start(id,user));
            verify(monitoringLogsService,new Times(1)).startTaskErrorLog(dto,user,exception,Level.ERROR);
        }
    }
    @Nested
    class StartWithFlagTest{
        private String startFlag;
        private LockControlService lockControlService;
        private DisruptorService disruptorService;
        private LogCollectorService logCollectorService;
        @BeforeEach
        void beforeEach(){
            startFlag = "11";
            lockControlService = mock(LockControlService.class);
            disruptorService = mock(DisruptorService.class);
            logCollectorService = mock(LogCollectorService.class);
            ReflectionTestUtils.setField(taskService,"lockControlService",lockControlService);
            ReflectionTestUtils.setField(taskService,"disruptorService",disruptorService);
            ReflectionTestUtils.setField(taskService,"logCollectorService",logCollectorService);
            when(taskDto.getShareCdcEnable()).thenReturn(true);
            when(taskDto.getSyncType()).thenReturn("sync");
            when(taskDto.getTaskRecordId()).thenReturn("111");
            when(taskDto.getStartTime()).thenReturn(null);
            when(taskDto.getShareCache()).thenReturn(false);
            when(taskDto.getId()).thenReturn(mock(ObjectId.class));
        }
        @Test
        @DisplayName("test start method when dag is invalid")
        void test1(){
            DAG dag = mock(DAG.class);
            when(taskDto.getDag()).thenReturn(dag);
            Map<String, List<Message>> validateMessage = new HashMap<>();
            validateMessage.put("test",new ArrayList<>());
            when(dag.validate()).thenReturn(validateMessage);
            doCallRealMethod().when(taskService).start(taskDto,user,startFlag);
            assertThrows(BizException.class,()->taskService.start(taskDto,user,startFlag));
        }
        @Test
        @DisplayName("test start method when status is invalid")
        void test2(){
            when(taskDto.getDag()).thenReturn(mock(DAG.class));
            doCallRealMethod().when(taskService).start(taskDto,user,startFlag);
            assertThrows(BizException.class,()->taskService.start(taskDto,user,startFlag));
        }
        @Test
        @DisplayName("test start method normal")
        void test3(){
            when(taskDto.getDag()).thenReturn(mock(DAG.class));
            when(taskDto.getStatus()).thenReturn("edit");
            when(taskDto.getSyncType()).thenReturn("migrate");
            TaskDto transformedCheck = mock(TaskDto.class);
            when(taskService.findByTaskId(any(ObjectId.class),anyString())).thenReturn(transformedCheck);
            when(transformedCheck.getTransformed()).thenReturn(true);
            doCallRealMethod().when(taskService).start(taskDto,user,startFlag);
            taskService.start(taskDto,user,startFlag);
            verify(taskService,new Times(1)).run(taskDto,user);
        }
//        @Test
        @DisplayName("test start method when transformedCheck is false")
        void test4(){
            try (MockedStatic<Thread> mb = Mockito
                    .mockStatic(Thread.class)) {
                mb.when(()->Thread.sleep(anyLong())).thenThrow(InterruptedException.class);
                when(taskDto.getDag()).thenReturn(mock(DAG.class));
                when(taskDto.getStatus()).thenReturn("edit");
                when(taskDto.getSyncType()).thenReturn("migrate");
                TaskDto transformedCheck = mock(TaskDto.class);
                when(taskService.findByTaskId(any(ObjectId.class),anyString())).thenReturn(transformedCheck);
                doCallRealMethod().when(taskService).start(taskDto,user,startFlag);
                assertThrows(BizException.class,()->taskService.start(taskDto,user,startFlag));
            }
        }
        @Test
        @DisplayName("test start method when sync type not in migrate or sync")
        void test5(){
            when(taskDto.getTaskRecordId()).thenReturn("");
            when(taskDto.getDag()).thenReturn(mock(DAG.class));
            when(taskDto.getStatus()).thenReturn("edit");
            when(taskDto.getSyncType()).thenReturn("heartbeat");
            doCallRealMethod().when(taskService).start(taskDto,user,startFlag);
            taskService.start(taskDto,user,startFlag);
            verify(taskService,new Times(1)).run(taskDto,user);
        }
    }
    @Nested
    class UpdateTaskRecordsStatusTest{
        private TaskDto dto;
        private String status;
        private DisruptorService disruptorService;
        @BeforeEach
        void beforeEach(){
            dto = mock(TaskDto.class);
            user = mock(UserDetail.class);
            disruptorService = mock(DisruptorService.class);
            ReflectionTestUtils.setField(taskService,"disruptorService",disruptorService);
        }
        @Test
        @DisplayName("test updateTaskRecordStatus method when task status is renewing")
        void test1(){
            status = "renewing";
            doCallRealMethod().when(taskService).updateTaskRecordStatus(dto,status,user);
            taskService.updateTaskRecordStatus(dto,status,user);
            verify(dto,new Times(0)).setStatus(status);
        }
        @Test
        @DisplayName("test updateTaskRecordStatus method normal")
        void test2(){
            status = "stopped";
            when(dto.getTaskRecordId()).thenReturn("111");
            when(dto.getId()).thenReturn(mock(ObjectId.class));
            doCallRealMethod().when(taskService).updateTaskRecordStatus(dto,status,user);
            taskService.updateTaskRecordStatus(dto,status,user);
            verify(disruptorService,new Times(1)).sendMessage(any(DisruptorTopicEnum.class),any(SyncTaskStatusDto.class));
        }
    }
    @Nested
    class PauseTest{
        private boolean force;
        private boolean restart;
        private StateMachineService stateMachineService;
        private ObjectId id;
        private String agentId;
        @BeforeEach
        void beforeEach(){
            taskDto = mock(TaskDto.class);
            id = mock(ObjectId.class);
            when(taskDto.getId()).thenReturn(id);
            agentId = "111";
            when(taskDto.getAgentId()).thenReturn(agentId);
            user = mock(UserDetail.class);
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
        }
        @Test
        @DisplayName("test pause method when task status is stopping")
        void test1(){
            restart = true;
            when(taskDto.getStatus()).thenReturn("stopping");
            when(taskService.findById(any(ObjectId.class),any(Field.class))).thenReturn(mock(TaskDto.class));
            doCallRealMethod().when(taskService).pause(taskDto,user,force,restart);
            taskService.pause(taskDto,user,force,restart);
            verify(taskService,new Times(1)).update(any(Query.class),any(Update.class),any(UserDetail.class));
        }
        @Test
        @DisplayName("test pause method when force is true")
        void test2(){
            force = true;
            when(taskDto.getStatus()).thenReturn("running");
            when(taskService.findById(any(ObjectId.class),any(Field.class))).thenReturn(mock(TaskDto.class));
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.FORCE_STOP, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).pause(taskDto,user,force,restart);
            taskService.pause(taskDto,user,force,restart);
            verify(stateMachineService,new Times(1)).executeAboutTask(taskDto, DataFlowEvent.FORCE_STOP, user);
        }
        @Test
        @DisplayName("test pause method when force is false")
        void test3(){
            when(taskDto.getStatus()).thenReturn("running");
            when(taskService.findById(any(ObjectId.class),any(Field.class))).thenReturn(mock(TaskDto.class));
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOP, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).pause(taskDto,user,force,restart);
            taskService.pause(taskDto,user,force,restart);
            verify(stateMachineService,new Times(1)).executeAboutTask(taskDto, DataFlowEvent.STOP, user);
        }
        @Test
        @DisplayName("test pause method when state machine result is fail")
        void test4(){
            when(taskDto.getStatus()).thenReturn("running");
            when(taskService.findById(any(ObjectId.class),any(Field.class))).thenReturn(mock(TaskDto.class));
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOP, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).pause(taskDto,user,force,restart);
            taskService.pause(taskDto,user,force,restart);
            verify(taskService,new Times(0)).sendStoppingMsg(id.toHexString(),agentId,user,force);
        }
        @Test
        @DisplayName("test pause method when restart is true")
        void test5(){
            restart = true;
            when(taskDto.getStatus()).thenReturn("running");
            when(taskService.findById(any(ObjectId.class),any(Field.class))).thenReturn(mock(TaskDto.class));
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(false);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOP, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).pause(taskDto,user,force,restart);
            taskService.pause(taskDto,user,force,restart);
            verify(taskService,new Times(1)).sendStoppingMsg(id.toHexString(),agentId,user,force);
        }
    }
    @Nested
    class SendStoppingMsgTest{
        private String taskId;
        private String agentId;
        private UserDetail user;
        private boolean force;
        private MessageQueueServiceImpl messageQueueService;
        @Test
        @DisplayName("test sendStoppingMsg method normal")
        void test1(){
            taskId = "111";
            agentId = "222";
            user = mock(UserDetail.class);
            force = true;
            messageQueueService = mock(MessageQueueServiceImpl.class);
            ReflectionTestUtils.setField(taskService,"messageQueueService",messageQueueService);
            doCallRealMethod().when(taskService).sendStoppingMsg(taskId,agentId,user,force);
            taskService.sendStoppingMsg(taskId,agentId,user,force);
            verify(messageQueueService,new Times(1)).sendMessage(any(MessageQueueDto.class));
        }
    }
    @Nested
    class RunningTest{
        private ObjectId id;
        private TaskDto dto;
        private MonitoringLogsService monitoringLogsService;
        private StateMachineService stateMachineService;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user, "_id", "status", "name", "taskRecordId", "startTime", "scheduleDate")).thenReturn(dto);
            monitoringLogsService = mock(MonitoringLogsService.class);
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"monitoringLogsService",monitoringLogsService);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
        }
        @Test
        @DisplayName("test running method when task status is running")
        void test1(){
            when(dto.getStatus()).thenReturn("running");
            doCallRealMethod().when(taskService).running(id,user);
            String actual = taskService.running(id, user);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test running method when state machine result is fail")
        void test2(){
            when(dto.getStatus()).thenReturn("wait_run");
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.RUNNING, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).running(id,user);
            String actual = taskService.running(id, user);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test running method normal")
        void test3(){
            when(dto.getStatus()).thenReturn("wait_run");
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(false);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.RUNNING, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).running(id,user);
            String actual = taskService.running(id, user);
            assertEquals(id.toHexString(),actual);
        }
    }
    @Nested
    class RunErrorTest{
        private ObjectId id;
        private UserDetail user;
        private String errMsg;
        private String errStack;
        private TaskDto dto;
        private StateMachineService stateMachineService;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            user = mock(UserDetail.class);
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user, "_id", "status", "name", "taskRecordId")).thenReturn(dto);
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
        }
        @Test
        @DisplayName("test runError method when state machine result is fail")
        void test1(){
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.ERROR, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).runError(id,user,errMsg,errStack);
            String actual = taskService.runError(id, user, errMsg, errStack);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test runError method normal")
        void test2(){
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(false);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.ERROR, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).runError(id,user,errMsg,errStack);
            String actual = taskService.runError(id,user,errMsg,errStack);
            assertEquals(id.toHexString(),actual);
        }
    }
    @Nested
    class CompleteTest{
        private ObjectId id;
        private UserDetail user;
        private TaskDto dto;
        private StateMachineService stateMachineService;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            user = mock(UserDetail.class);
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user, "_id", "status", "name", "taskRecordId")).thenReturn(dto);
            stateMachineService = mock(StateMachineService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
        }
        @Test
        @DisplayName("test complete method when state machine result is fail")
        void test1(){
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.COMPLETED, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).complete(id,user);
            String actual = taskService.complete(id, user);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test complete method normal")
        void test2(){
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(false);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.COMPLETED, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).complete(id,user);
            String actual = taskService.complete(id, user);
            assertEquals(id.toHexString(),actual);
        }
    }
    @Nested
    class StoppedTest{
        private ObjectId id;
        private UserDetail user;
        private TaskDto dto;
        private StateMachineService stateMachineService;
        private MonitoringLogsService monitoringLogsService;
        private LogCollectorService logCollectorService;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            user = mock(UserDetail.class);
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(id,user, "dag", "name", "status", "_id", "taskRecordId", "agentId", "stopedDate", "restartFlag")).thenReturn(dto);
            stateMachineService = mock(StateMachineService.class);
            monitoringLogsService = mock(MonitoringLogsService.class);
            logCollectorService = mock(LogCollectorService.class);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
            ReflectionTestUtils.setField(taskService,"monitoringLogsService",monitoringLogsService);
            ReflectionTestUtils.setField(taskService,"logCollectorService",logCollectorService);
        }
        @Test
        @DisplayName("test stopped method when state machine result is fail")
        void test1(){
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(true);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.STOPPED, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).stopped(id,user);
            String actual = taskService.stopped(id, user);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test stopped method normal")
        void test2(){
            when(dto.getResetFlag()).thenReturn(true);
            StateMachineResult stateMachineResult = mock(StateMachineResult.class);
            when(stateMachineResult.isFail()).thenReturn(false);
            when(stateMachineService.executeAboutTask(dto, DataFlowEvent.STOPPED, user)).thenReturn(stateMachineResult);
            doCallRealMethod().when(taskService).stopped(id,user);
            String actual = taskService.stopped(id, user);
            verify(taskService,new Times(1)).start(id,user);
            assertEquals(id.toHexString(),actual);
        }
    }
    @Nested
    class RunTimeInfoTest{
        private ObjectId id;
        private Long endTime;
        private UserDetail user;
        private TaskDto dto;
        @BeforeEach
        void beforeEach(){
            id = mock(ObjectId.class);
            user = mock(UserDetail.class);
            dto = mock(TaskDto.class);
        }
        @Test
        @DisplayName("test runtimeInfo method when taskDto is null")
        void test1(){
            when(taskService.findById(id,user)).thenReturn(null);
            doCallRealMethod().when(taskService).runtimeInfo(id,endTime,user);
            RunTimeInfo actual = taskService.runtimeInfo(id, endTime, user);
            assertEquals(null,actual);
        }
        @Test
        @DisplayName("test runtimeInfo method normal")
        void test2(){
            List<Milestone> milestones = new ArrayList<>();
            Milestone milestone = mock(Milestone.class);
            milestones.add(milestone);
            when(dto.getMilestones()).thenReturn(milestones);
            when(taskService.findById(id,user)).thenReturn(dto);
            doCallRealMethod().when(taskService).runtimeInfo(id,endTime,user);
            RunTimeInfo actual = taskService.runtimeInfo(id, endTime, user);
            assertEquals(milestone,actual.getMilestones().get(0));
        }
    }
    @Nested
    class UpdateNodeTest{
        private ObjectId objectId;
        private String nodeId;
        private Document param;
        private UserDetail user;
        @Test
        @DisplayName("test updateNode method normal")
        void test(){
            objectId = mock(ObjectId.class);
            param = mock(Document.class);
            Map<String, Object> map = new HashMap<>();
            map.put("test1","test2");
            Document set = new Document(map);
            when(param.get("$set")).thenReturn(set);
            Set<String> str = new HashSet<>();
            str.add("test");
            user = mock(UserDetail.class);
            doCallRealMethod().when(taskService).updateNode(objectId,nodeId,param,user);
            taskService.updateNode(objectId,nodeId,param,user);
            verify(taskService).update(any(Query.class),any(Update.class),any(UserDetail.class));
        }
    }
    @Nested
    class UpdateSyncProgressTest{
        private ObjectId taskId;
        private Document document;
        @Test
        @DisplayName("test updateSyncProgress method normal")
        void test(){
            taskId = mock(ObjectId.class);
            Map<String, Object> map = new HashMap<>();
            map.put("test1","test2");
            document = new Document(map);
            doCallRealMethod().when(taskService).updateSyncProgress(taskId,document);
            taskService.updateSyncProgress(taskId,document);
            verify(taskService).update(any(Query.class),any(Update.class));
        }
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
    @Nested
    class IncreaseClearTest{
        private ObjectId taskId;
        private String srcNode;
        private String tgtNode;
        @Test
        @DisplayName("test increaseClear method normal")
        void test(){
            taskId = mock(ObjectId.class);
            srcNode = "111";
            tgtNode = "222";
            TaskDto dto = mock(TaskDto.class);
            when(taskService.checkExistById(taskId,user,"attrs")).thenReturn(dto);
            doCallRealMethod().when(taskService).increaseClear(taskId,srcNode,tgtNode,user);
            taskService.increaseClear(taskId,srcNode,tgtNode,user);
            verify(taskService).clear(srcNode,tgtNode,user,dto);
        }
    }
    @Nested
    class ClearTest{
        private String srcNode;
        private String tgtNode;
        private TaskDto dto;
        private TaskRepository repository;
        @BeforeEach
        void beforeEach(){
            srcNode = "111";
            tgtNode = "111";
            dto = new TaskDto();
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
        }
        @Test
        @DisplayName("test clear method when syncProgress is null")
        void test1(){
            Map<String, Object> attrs = new HashMap<>();
            dto.setAttrs(attrs);
            taskService.clear(srcNode,tgtNode,user,dto);
            verify(repository,new Times(0)).updateFirst(any(Query.class),any(Update.class),any(UserDetail.class));
        }
        @Test
        @DisplayName("test clear method normal")
        void test2(){
            Map<String, Object> attrs = new HashMap<>();
            Map syncProgressMap = new HashMap();
            syncProgressMap.put("111","test1");
            syncProgressMap.put("222","test2");
            attrs.put("syncProgress",syncProgressMap);
            dto.setAttrs(attrs);
            dto.setId(mock(ObjectId.class));
            taskService.clear(srcNode,tgtNode,user,dto);
            verify(repository,new Times(1)).updateFirst(any(Query.class),any(Update.class),any(UserDetail.class));
        }
    }
    @Nested
    class IncreaseBacktrackingTest{
        private ObjectId taskId;
        private String srcNode;
        private String tgtNode;
        private TaskDto.SyncPoint point;
        private DAG dag;
        private TaskDto dto;
        private ObjectId id;
        @BeforeEach
        void beforeEach(){
            taskId = mock(ObjectId.class);
            srcNode = "111";
            tgtNode = "222";
            point = mock(TaskDto.SyncPoint.class);
            dto = mock(TaskDto.class);
            when(taskService.checkExistById(taskId, user, "parentId", "attrs", "dag", "syncPoints")).thenReturn(dto);
            dag = mock(DAG.class);
            when(dto.getDag()).thenReturn(dag);
            id = mock(ObjectId.class);
            when(dto.getId()).thenReturn(id);
        }
        @Test
        @DisplayName("test increaseBacktracking method when sync points is empty")
        void test1(){
            try (MockedStatic<Criteria> mb = Mockito
                    .mockStatic(Criteria.class)) {
                Criteria criteria = mock(Criteria.class);
                mb.when(()->Criteria.where(anyString())).thenReturn(criteria);
                when(criteria.is(id)).thenReturn(mock(Criteria.class));
                when(dag.getNode(tgtNode)).thenReturn(mock(DataParentNode.class));
                List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
                when(dto.getSyncPoints()).thenReturn(syncPoints);
                doCallRealMethod().when(taskService).increaseBacktracking(taskId,srcNode,tgtNode,point,user);
                taskService.increaseBacktracking(taskId,srcNode,tgtNode,point,user);
                verify(criteria,new Times(0)).elemMatch(any(Criteria.class));
            }
        }
        @Test
        @DisplayName("test increaseBacktracking method for when sync points is not empty")
        void test2(){
            try (MockedStatic<Criteria> mb = Mockito
                    .mockStatic(Criteria.class)) {
                Criteria criteria = mock(Criteria.class);
                mb.when(()->Criteria.where(anyString())).thenReturn(criteria);
                Criteria mock = mock(Criteria.class);
                when(criteria.is(any())).thenReturn(mock);
                when(mock.and(anyString())).thenReturn(mock);
                when(mock.elemMatch(any(Criteria.class))).thenReturn(mock);
                DataParentNode node = mock(DataParentNode.class);
                when(node.getId()).thenReturn("111");
                when(dag.getNode(tgtNode)).thenReturn(node);
                List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
                TaskDto.SyncPoint item1 = mock(TaskDto.SyncPoint.class);
                syncPoints.add(item1);
                when(item1.getNodeId()).thenReturn("222");
                TaskDto.SyncPoint item2 = mock(TaskDto.SyncPoint.class);
                syncPoints.add(item2);
                when(item1.getNodeId()).thenReturn("111");
                when(dto.getSyncPoints()).thenReturn(syncPoints);
                doCallRealMethod().when(taskService).increaseBacktracking(taskId,srcNode,tgtNode,point,user);
                taskService.increaseBacktracking(taskId,srcNode,tgtNode,point,user);
                verify(mock,new Times(1)).elemMatch(any(Criteria.class));
            }
        }
    }
    @Nested
    class StartPlanMigrateDagTask{
        private UserService userService;
        private StateMachineService stateMachineService;
        private MonitoringLogsService monitoringLogsService;
        @BeforeEach
        void beforeEach(){
            List<TaskDto> taskList = new ArrayList<>();
            TaskDto dto = mock(TaskDto.class);
            taskList.add(dto);
            when(dto.getTransformed()).thenReturn(true);
            when(dto.getId()).thenReturn(mock(ObjectId.class));
            when(dto.getUserId()).thenReturn("111");
            when(taskService.findAll(any(Query.class))).thenReturn(taskList);
            userService = mock(UserService.class);
            stateMachineService = mock(StateMachineService.class);
            monitoringLogsService = mock(MonitoringLogsService.class);
            ReflectionTestUtils.setField(taskService,"userService",userService);
            ReflectionTestUtils.setField(taskService,"stateMachineService",stateMachineService);
            ReflectionTestUtils.setField(taskService,"monitoringLogsService",monitoringLogsService);
        }
        @Test
        @DisplayName("test startPlanMigrateDagTask method when task list is empty")
        void test1(){
            when(taskService.findAll(any(Query.class))).thenReturn(null);
            doCallRealMethod().when(taskService).startPlanMigrateDagTask();
            taskService.startPlanMigrateDagTask();
            verify(userService,new Times(0)).getUserByIdList(anyList());
        }
        @Test
        @DisplayName("test startPlanMigrateDagTask method normal")
        void test2(){
            List<UserDetail> userList = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            userList.add(userDetail);
            when(userDetail.getUserId()).thenReturn("111");
            when(userService.getUserByIdList(anyList())).thenReturn(userList);
            doCallRealMethod().when(taskService).startPlanMigrateDagTask();
            taskService.startPlanMigrateDagTask();
            verify(taskService,new Times(1)).updateById(any(ObjectId.class),any(Update.class),any(UserDetail.class));
        }
        @Test
        @DisplayName("test startPlanMigrateDagTask method for TapCodeException")
        void test3(){
            List<UserDetail> userList = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            userList.add(userDetail);
            when(userDetail.getUserId()).thenReturn("111");
            when(userService.getUserByIdList(anyList())).thenReturn(userList);
            doThrow(TapCodeException.class).when(taskService).start(any(TaskDto.class),any(UserDetail.class),anyString());
            doCallRealMethod().when(taskService).startPlanMigrateDagTask();
            taskService.startPlanMigrateDagTask();
            verify(monitoringLogsService,new Times(1)).startTaskErrorStackTrace(any(TaskDto.class),any(UserDetail.class),any(Throwable.class),any(Level.class));
        }
        @Test
        @DisplayName("test startPlanMigrateDagTask method for other exception")
        void test4(){
            List<UserDetail> userList = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            userList.add(userDetail);
            when(userDetail.getUserId()).thenReturn("111");
            when(userService.getUserByIdList(anyList())).thenReturn(userList);
            doThrow(RuntimeException.class).when(taskService).start(any(TaskDto.class),any(UserDetail.class),anyString());
            doCallRealMethod().when(taskService).startPlanMigrateDagTask();
            taskService.startPlanMigrateDagTask();
            verify(monitoringLogsService,new Times(1)).startTaskErrorStackTrace(any(TaskDto.class),any(UserDetail.class),any(Throwable.class),any(Level.class));
        }
    }
    @Nested
    class StartPlanCronTaskTest{
        private ScheduleService scheduleService;
        @BeforeEach
        void beforeEach(){
            List<TaskDto> taskList = new ArrayList<>();
            TaskDto dto = mock(TaskDto.class);
            taskList.add(dto);
            when(dto.getTransformed()).thenReturn(true);
            when(dto.getId()).thenReturn(mock(ObjectId.class));
            when(dto.getUserId()).thenReturn("111");
            when(taskService.findAll(any(Query.class))).thenReturn(taskList);
            scheduleService = mock(ScheduleService.class);
            ReflectionTestUtils.setField(taskService,"scheduleService",scheduleService);
        }
        @Test
        @DisplayName("test startPlanCronTask method normal")
        void test1(){
            doCallRealMethod().when(taskService).startPlanCronTask();
            taskService.startPlanCronTask();
            verify(scheduleService).executeTask(any(TaskDto.class));
        }
        @Test
        @DisplayName("test startPlanCronTask method with exception")
        void test2(){
            doThrow(RuntimeException.class).when(scheduleService).executeTask(any(TaskDto.class));
            doCallRealMethod().when(taskService).startPlanCronTask();
            taskService.startPlanCronTask();
            verify(scheduleService).executeTask(any(TaskDto.class));
        }
    }
    @Nested
    class FindByVersionTime{
        private String id;
        private Long time;
        private TaskRepository repository;
        @BeforeEach
        void beforeEach(){
            id = "662877df9179877be8b37074";
            time = 1713846744L;
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskServiceImpl(repository));
        }
        @Test
        @DisplayName("test findByVersionTime method when dDlTaskHistories is null")
        void test1(){
            MongoTemplate template = mock(MongoTemplate.class);
            when(repository.getMongoOperations()).thenReturn(template);
            when(template.findOne(any(Query.class),any(Class.class),anyString())).thenReturn(null);
            TaskDto dto = mock(TaskDto.class);
            doReturn(dto).when(taskService).findById(any(ObjectId.class));
            TaskDto actual = taskService.findByVersionTime(id, time);
            assertEquals(dto,actual);
        }
        @Test
        @DisplayName("test findByVersionTime method when dDlTaskHistories is not null")
        void test2(){
            MongoTemplate template = mock(MongoTemplate.class);
            when(repository.getMongoOperations()).thenReturn(template);
            TaskDto dto = new TaskDto();
            when(template.findOne(any(Query.class),any(Class.class),anyString())).thenReturn(dto);
            TaskDto actual = taskService.findByVersionTime(id, time);
            assertEquals(id,actual.getId().toHexString());
        }
    }
    @Nested
    class Clean{

    }
    @Nested
    class TotalAutoInspectResultsDiffTablesTest{

    }
    @Nested
    class UpdateTaskLogSettingTest{

    }
    @Nested
    class Chart6Test{

    }
    @Nested
    class StopTaskIfNeedByAgentIdTest{

    }
    @Nested
    class GetTaskStatsByTableNameOrConnectionIdTest{

    }
    @Nested
    class GetTableStatusTest{

    }
    @Nested
    class JudgeTargetInspectTest{

    }
    @Nested
    class JudgeTargetNodeTest{

    }
    @Nested
    class FindHeartbeatByTaskIdTest{

    }
    @Nested
    class DeleteHeartbeatByConnIdTest{

    }
}