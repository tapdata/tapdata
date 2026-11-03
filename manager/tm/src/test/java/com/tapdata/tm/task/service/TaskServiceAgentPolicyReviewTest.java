package com.tapdata.tm.task.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskServiceAgentPolicyReviewTest {
    @Test
    void sharedCacheIsExemptAtBothSaveAndStartValidationEntries() {
        TaskScheduleService schedule = mock(TaskScheduleService.class);
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        ReflectionTestUtils.setField(service, "taskScheduleService", schedule);
        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setShareCache(true);
        UserDetail user = mock(UserDetail.class);
        service.validateConnectionAgentPolicy(task, user);
        service.checkDagAgentConflict(task, user, true);
        service.checkDagAgentConflict(task, user, false);
        verifyNoInteractions(schedule);
    }

    @Test
    void policyConflictCannotBeBypassedByTheLegacyWarningConfirmation() {
        TaskScheduleService schedule = mock(TaskScheduleService.class);
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        ReflectionTestUtils.setField(service, "taskScheduleService", schedule);
        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        UserDetail user = mock(UserDetail.class);
        doThrow(new BizException("Task.AgentConflict")).when(schedule).validateConnectionAgentPolicy(task, user);
        for (boolean showList : new boolean[]{true, false}) {
            BizException error = assertThrows(BizException.class, () -> service.checkDagAgentConflict(task, user, showList));
            assertEquals("Task.AgentConflict", error.getErrorCode());
        }
    }

    @Test
    void partialSavePreservesTheTaskOwnerAndDagForPolicyValidation() {
        TaskScheduleService schedule = mock(TaskScheduleService.class);
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        ReflectionTestUtils.setField(service, "taskScheduleService", schedule);
        TaskDto persisted = new TaskDto();
        persisted.setUserId("connection-independent-task-owner");
        persisted.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        persisted.setDag(mock(DAG.class));
        TaskDto patch = new TaskDto();
        patch.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        patch.setAccessNodeProcessId("new-task-group");
        UserDetail editor = mock(UserDetail.class);
        ReflectionTestUtils.invokeMethod(service, "validateConnectionAgentPolicyBeforeSave", patch, persisted, editor);
        ArgumentCaptor<TaskDto> validated = ArgumentCaptor.forClass(TaskDto.class);
        verify(schedule).validateConnectionAgentPolicy(validated.capture(), eq(editor));
        assertEquals(persisted.getUserId(), validated.getValue().getUserId());
        assertSame(persisted.getDag(), validated.getValue().getDag());
        assertEquals("new-task-group", validated.getValue().getAccessNodeProcessId());
    }
    @Test
    void fullSaveUsesPersistedOwnerInsteadOfTheRequestOwner() {
        TaskScheduleService schedule = mock(TaskScheduleService.class);
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        ReflectionTestUtils.setField(service, "taskScheduleService", schedule);
        TaskDto persisted = new TaskDto();
        persisted.setUserId("stored-owner");
        TaskDto submitted = new TaskDto();
        submitted.setUserId("acting-editor");
        submitted.setDag(mock(DAG.class));
        UserDetail editor = mock(UserDetail.class);
        ReflectionTestUtils.invokeMethod(service, "validateConnectionAgentPolicyBeforeSave", submitted, persisted, editor);
        ArgumentCaptor<TaskDto> validated = ArgumentCaptor.forClass(TaskDto.class);
        verify(schedule).validateConnectionAgentPolicy(validated.capture(), eq(editor));
        assertEquals("stored-owner", validated.getValue().getUserId());
        assertEquals("acting-editor", submitted.getUserId());
    }
}
