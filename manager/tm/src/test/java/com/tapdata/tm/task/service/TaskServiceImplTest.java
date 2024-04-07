package com.tapdata.tm.task.service;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.Lists;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class TaskServiceImplTest {
    TaskServiceImpl taskService;
    AgentGroupService agentGroupService;

    TaskDto taskDto;
    UserDetail user;
    @BeforeEach
    void init() {
        taskService = mock(TaskServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(taskService, "agentGroupService", agentGroupService);

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
            when(taskDto.getAccessNodeType()).thenReturn(null);

            when(taskDto.getAccessNodeProcessId()).thenReturn("id");

            when(temp.getAccessNodeType()).thenReturn(null);
            doNothing().when(taskDto).setAccessNodeType(null);

            when(temp.getAccessNodeProcessId()).thenReturn(null);
            doNothing().when(taskDto).setAccessNodeProcessId(null);

            when(agentGroupService.getProcessNodeListWithGroup(temp, user)).thenReturn(mock(List.class));
            doNothing().when(taskDto).setAccessNodeProcessIdList(anyList());

            doNothing().when(taskService).checkTaskInspectFlag(taskDto);
            doNothing().when(taskService).checkDagAgentConflict(taskDto,user,true);
            doNothing().when(taskService).checkDDLConflict(taskDto);
            when(taskService.confirmById(taskDto, user, true, false)).thenReturn(mock(TaskDto.class));

            when(taskService.confirmById(taskDto, user, true)).thenCallRealMethod();
        }

        void assertVerify(int getIdTimes, int findByIdTimes,
                          int getAccessNodeTypeTimes,
                          int getAndSetTimes) {
            Assertions.assertDoesNotThrow(() -> taskService.confirmById(taskDto, user, true));
            verify(taskDto, times(getIdTimes)).getId();
            verify(taskService, times(findByIdTimes)).findById(any(ObjectId.class));
            verify(taskDto, times(getAccessNodeTypeTimes)).getAccessNodeType();

            verify(temp, times(getAndSetTimes)).getAccessNodeType();
            verify(taskDto, times(getAndSetTimes)).setAccessNodeType(null);

            verify(temp, times(getAndSetTimes)).getAccessNodeProcessId();
            verify(taskDto, times(getAndSetTimes)).setAccessNodeProcessId(null);

            verify(agentGroupService, times(getAndSetTimes)).getProcessNodeListWithGroup(temp, user);
            verify(taskDto, times(getAndSetTimes)).setAccessNodeProcessIdList(anyList());

            verify(taskService, times(1)).checkTaskInspectFlag(taskDto);
            verify(taskService, times(1)).checkDagAgentConflict(taskDto, user, true);
            verify(taskService, times(1)).checkDDLConflict(taskDto);
            verify(taskService, times(1)).confirmById(taskDto, user, true, false);
        }

        @Test
        void testNormal() {
            assertVerify(2, 1, 1, 1);
        }
        @Test
        void testNullObjectId() {
            when(taskDto.getId()).thenReturn(null);
            assertVerify(1, 0, 0, 0);
        }
        @Test
        void testNullTemp() {
            when(taskService.findById(any(ObjectId.class))).thenReturn(null);
            assertVerify(2, 1, 0, 0);
        }
        @Test
        void testGetAccessNodeTypeNotBlank() {
            when(taskDto.getAccessNodeType()).thenReturn("type");
            assertVerify(2, 1, 1, 0);
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
}