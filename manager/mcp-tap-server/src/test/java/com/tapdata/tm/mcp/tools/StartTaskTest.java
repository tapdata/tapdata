package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartTaskTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private TaskService taskService;

    @Mock
    private McpSyncRequestContext context;

    private StartTask startTask;

    @BeforeEach
    void setUp() {
        startTask = new StartTask(new McpToolSupport(sessionAttribute, userService), taskService);
    }

    @Test
    void testCallConfirmEditTaskBeforeStart() {
        UserDetail userDetail = mockUser();
        ObjectId taskId = new ObjectId();
        TaskDto taskDto = buildTask(taskId, "EditTask", TaskDto.STATUS_EDIT);
        TaskDto confirmedTask = buildTask(taskId, "EditTask", TaskDto.STATUS_WAIT_START);
        MutiResponseMessage responseMessage = new MutiResponseMessage();
        responseMessage.setId(taskId.toHexString());

        when(taskService.checkExistById(taskId, userDetail)).thenReturn(taskDto).thenReturn(confirmedTask);
        when(taskService.confirmById(taskDto, userDetail, true)).thenReturn(confirmedTask);
        when(taskService.batchStart(eq(Collections.singletonList(taskId)), eq(userDetail), isNull(), isNull()))
                .thenReturn(Collections.singletonList(responseMessage));

        Map<String, Object> result = startTask.startTask(context, taskId.toHexString(), null);

        assertNotNull(result);
        assertEquals(List.of(taskId.toHexString()), result.get("taskIds"));
        verify(taskService).confirmById(taskDto, userDetail, true);
        verify(taskService).clearAgentAffinityForManualStart(Collections.singletonList(taskId), userDetail);
        verify(taskService).batchStart(Collections.singletonList(taskId), userDetail, null, null);
    }

    @Test
    void testCallStartWaitStartTaskDirectly() {
        UserDetail userDetail = mockUser();
        ObjectId taskId = new ObjectId();
        TaskDto taskDto = buildTask(taskId, "WaitStartTask", TaskDto.STATUS_WAIT_START);
        MutiResponseMessage responseMessage = new MutiResponseMessage();
        responseMessage.setId(taskId.toHexString());

        when(taskService.checkExistById(taskId, userDetail)).thenReturn(taskDto);
        when(taskService.batchStart(eq(Collections.singletonList(taskId)), eq(userDetail), isNull(), isNull()))
                .thenReturn(Collections.singletonList(responseMessage));

        Map<String, Object> result = startTask.startTask(context, taskId.toHexString(), null);

        assertNotNull(result);
        verify(taskService, never()).confirmById(any(TaskDto.class), eq(userDetail), eq(true));
        verify(taskService).clearAgentAffinityForManualStart(Collections.singletonList(taskId), userDetail);
        verify(taskService).batchStart(Collections.singletonList(taskId), userDetail, null, null);
    }

    @Test
    void testCallBatchTaskIds() {
        UserDetail userDetail = mockUser();
        ObjectId firstTaskId = new ObjectId();
        ObjectId secondTaskId = new ObjectId();
        List<ObjectId> taskObjectIds = List.of(firstTaskId, secondTaskId);
        TaskDto firstTask = buildTask(firstTaskId, "FirstTask", TaskDto.STATUS_WAIT_START);
        TaskDto secondTask = buildTask(secondTaskId, "SecondTask", TaskDto.STATUS_STOP);

        when(taskService.checkExistById(firstTaskId, userDetail)).thenReturn(firstTask);
        when(taskService.checkExistById(secondTaskId, userDetail)).thenReturn(secondTask);
        when(taskService.batchStart(eq(taskObjectIds), eq(userDetail), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        Map<String, Object> result = startTask.startTask(context, null, List.of(firstTaskId.toHexString(), secondTaskId.toHexString()));

        assertNotNull(result);
        verify(taskService).clearAgentAffinityForManualStart(taskObjectIds, userDetail);
        verify(taskService).batchStart(taskObjectIds, userDetail, null, null);
    }

    @Test
    void testCallDeduplicateTaskIdAndTaskIds() {
        UserDetail userDetail = mockUser();
        ObjectId firstTaskId = new ObjectId();
        ObjectId secondTaskId = new ObjectId();
        List<ObjectId> taskObjectIds = List.of(firstTaskId, secondTaskId);
        TaskDto firstTask = buildTask(firstTaskId, "FirstTask", TaskDto.STATUS_WAIT_START);
        TaskDto secondTask = buildTask(secondTaskId, "SecondTask", TaskDto.STATUS_STOP);

        when(taskService.checkExistById(firstTaskId, userDetail)).thenReturn(firstTask);
        when(taskService.checkExistById(secondTaskId, userDetail)).thenReturn(secondTask);
        when(taskService.batchStart(eq(taskObjectIds), eq(userDetail), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        Map<String, Object> result = startTask.startTask(context, firstTaskId.toHexString(),
                List.of(firstTaskId.toHexString(), secondTaskId.toHexString(), firstTaskId.toHexString()));

        assertNotNull(result);
        verify(taskService).checkExistById(firstTaskId, userDetail);
        verify(taskService).checkExistById(secondTaskId, userDetail);
        verify(taskService).clearAgentAffinityForManualStart(taskObjectIds, userDetail);
        verify(taskService).batchStart(taskObjectIds, userDetail, null, null);
    }

    @Test
    void testCallTaskNotFound() {
        UserDetail userDetail = mockUser();
        ObjectId taskId = new ObjectId();

        when(taskService.checkExistById(taskId, userDetail)).thenReturn(null);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> startTask.startTask(context, taskId.toHexString(), null));

        assertTrue(exception.getMessage().contains("Task not found"));
        verify(taskService, never()).clearAgentAffinityForManualStart(anyList(), eq(userDetail));
        verify(taskService, never()).batchStart(anyList(), eq(userDetail), isNull(), isNull());
    }

    @Test
    void testCallWithoutTaskId() {
        mockUser();
        assertThrows(RuntimeException.class, () -> startTask.startTask(context, null, null));
    }

    private UserDetail mockUser() {
        UserDetail userDetail = mock(UserDetail.class);
        when(context.sessionId()).thenReturn("session-1");
        when(sessionAttribute.getAttribute("session-1", "userId")).thenReturn(new ObjectId().toHexString());
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);
        return userDetail;
    }

    private TaskDto buildTask(ObjectId id, String name, String status) {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(id);
        taskDto.setName(name);
        taskDto.setStatus(status);
        return taskDto;
    }
}
