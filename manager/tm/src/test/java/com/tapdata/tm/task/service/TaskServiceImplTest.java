package com.tapdata.tm.task.service;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.dto.CheckEchoOneNodeParam;
import com.tapdata.tm.task.service.utils.TaskServiceUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


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
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(temp, taskDto, user, agentGroupService)).then(a->null);
                Assertions.assertDoesNotThrow(() -> taskService.confirmById(taskDto, user, true));
                tsu.verify(() -> TaskServiceUtil.copyAccessNodeInfo(temp, taskDto, user, agentGroupService), times(findByIdTimes));
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
}