package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceDto;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceRepository;
import com.tapdata.tm.taskrebalance.rule.TaskRebalanceRuleService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskRebalanceServiceTest {

    @Test
    @DisplayName("cancel reason includes operator username")
    void cancelReasonIncludesUsername() {
        UserDetail user = new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());

        assertEquals("Cancelled by Harsen", TaskRebalanceService.buildCancelReason(user));
    }

    @Test
    @DisplayName("cancel reason falls back to user when operator is missing")
    void cancelReasonFallback() {
        assertEquals("Cancelled by user", TaskRebalanceService.buildCancelReason(null));
    }

    @Test
    @DisplayName("STOPPING job resumes from stopped source task")
    void stoppingJobResumesFromStoppedSourceTask() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.STOPPING);
        TaskDto stoppedOnSource = context.task(TaskDto.STATUS_STOP, "source");
        TaskDto running = context.task(TaskDto.STATUS_RUNNING, "target");
        context.stubTaskLookups(stoppedOnSource, stoppedOnSource, running);

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", job, context.user, new AtomicBoolean(false), new AtomicReference<String>());

        verify(context.taskService, never()).pause(any(TaskDto.class), eq(context.user), eq(false));
        verify(context.taskService).updateById(eq(context.taskId), any(Update.class), eq(context.user));
        verify(context.taskService).start(any(TaskDto.class), eq(context.user), eq("11"));
        verify(context.ruleService, never()).isMovableAtExecution(any());
    }

    @Test
    @DisplayName("STARTING job resumes from stopped target task")
    void startingJobResumesFromStoppedTargetTask() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.STARTING);
        TaskDto stoppedOnTarget = context.task(TaskDto.STATUS_STOP, "target");
        TaskDto running = context.task(TaskDto.STATUS_RUNNING, "target");
        context.stubTaskLookups(stoppedOnTarget, stoppedOnTarget, stoppedOnTarget, running);

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", job, context.user, new AtomicBoolean(false), new AtomicReference<String>());

        verify(context.taskService, never()).pause(any(TaskDto.class), eq(context.user), eq(false));
        verify(context.taskService, never()).updateById(eq(context.taskId), any(Update.class), eq(context.user));
        verify(context.taskService).start(any(TaskDto.class), eq(context.user), eq("11"));
        verify(context.ruleService, never()).isMovableAtExecution(any());
    }

    @Test
    @DisplayName("start failure marks current job START_TIMEOUT without aborting rebalance")
    void startFailureMarksCurrentJobOnly() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.STARTING);
        AtomicBoolean abortFlag = new AtomicBoolean(false);
        AtomicReference<String> abortReason = new AtomicReference<>();
        TaskDto targetTask = context.task(TaskDto.STATUS_STOP, "target");
        TaskDto sourceTask = context.task(TaskDto.STATUS_STOP, "source");
        context.stubTaskLookups(targetTask, targetTask, sourceTask);

        ReflectionTestUtils.invokeMethod(context.service, "handleStartFailure", job, context.user, "boom", context.user, abortFlag, abortReason);

        assertFalse(abortFlag.get());
        assertEquals(null, abortReason.get());
        Update update = context.captureLastJobUpdate();
        assertEquals(TaskRebalanceJobStatus.START_TIMEOUT, update.getUpdateObject().get("$set", org.bson.Document.class).get(TaskRebalanceJobDto.FIELD_STATUS));
    }

    @Test
    @DisplayName("target offline marks current job INVALID_AGENT and aborts pending jobs")
    void targetOfflineStillAbortsRebalance() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.PENDING);
        AtomicBoolean abortFlag = new AtomicBoolean(false);
        AtomicReference<String> abortReason = new AtomicReference<>();
        when(context.workerService.findAllEntity(any(Query.class))).thenReturn(List.of());

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", job, context.user, abortFlag, abortReason);

        assertTrue(abortFlag.get());
        assertTrue(abortReason.get().contains("Target agent target offline"));
        Update update = context.captureLastJobUpdate();
        assertEquals(TaskRebalanceJobStatus.INVALID_AGENT, update.getUpdateObject().get("$set", org.bson.Document.class).get(TaskRebalanceJobDto.FIELD_STATUS));
    }

    @Test
    @DisplayName("schedule marks rebalance failed when creator user id is invalid")
    void scheduleMarksInvalidUserRebalanceFailed() {
        TestContext context = new TestContext();
        TaskRebalanceService service = spy(context.service);
        ObjectId rebalanceId = new ObjectId();
        TaskRebalanceDto rebalance = new TaskRebalanceDto();
        rebalance.setId(rebalanceId);
        rebalance.setUserId("bad-user-id");
        rebalance.setStatus(TaskRebalanceStatus.RUNNING);
        doReturn(List.of(rebalance)).when(service).findAll(any(Query.class));
        doReturn(null).when(service).updateById(eq(rebalanceId), any(Update.class), isNull());

        service.scheduleOnce();

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(service).updateById(eq(rebalanceId), updateCaptor.capture(), isNull());
        org.bson.Document set = updateCaptor.getValue().getUpdateObject().get("$set", org.bson.Document.class);
        assertEquals(TaskRebalanceStatus.FAILED, set.get(TaskRebalanceDto.FIELD_STATUS));
        assertTrue(((String) set.get(TaskRebalanceDto.FIELD_ERROR_MESG)).contains("Invalid rebalance userId"));
        verify(context.userService, never()).loadUserById(any(ObjectId.class));
        verify(service, never()).execute(anyString(), any(UserDetail.class));
    }

    private static class TestContext {
        private final ObjectId taskId = new ObjectId();
        private final ObjectId jobId = new ObjectId();
        private final UserDetail user = new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
        private final TaskRebalanceJobService jobService = mock(TaskRebalanceJobService.class);
        private final TaskService taskService = mock(TaskService.class);
        private final WorkerService workerService = mock(WorkerService.class);
        private final UserService userService = mock(UserService.class);
        private final SettingsService settingsService = mock(SettingsService.class);
        private final TaskRebalanceRuleService ruleService = mock(TaskRebalanceRuleService.class);
        private final TaskRebalanceService service = new TaskRebalanceService(
                mock(TaskRebalanceRepository.class),
                jobService,
                taskService,
                workerService,
                userService,
                settingsService,
                ruleService
        );

        private TestContext() {
            new SettingUtil(settingsService);
            when(settingsService.getByCategoryAndKey(any(String.class), any(String.class))).thenReturn("30");
            Worker target = new Worker();
            target.setProcessId("target");
            when(workerService.findAllEntity(any(Query.class))).thenReturn(List.of(target));
            doAnswer(invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }).when(jobService).runAsRebalanceOperation(any(Runnable.class));
        }

        private TaskRebalanceJobDto job(String status) {
            TaskRebalanceJobDto job = new TaskRebalanceJobDto();
            job.setId(jobId);
            job.setTaskId(taskId.toHexString());
            job.setStatus(status);
            job.setSourceAgentId("source");
            job.setTargetAgentId("target");
            when(jobService.findById(jobId, user)).thenReturn(job);
            return job;
        }

        private TaskDto task(String status, String agentId) {
            TaskDto task = new TaskDto();
            task.setId(taskId);
            task.setStatus(status);
            task.setAgentId(agentId);
            return task;
        }

        private void stubTaskLookups(TaskDto... tasks) {
            when(taskService.findByTaskId(eq(taskId), any(String[].class)))
                    .thenReturn(tasks[0], Arrays.copyOfRange(tasks, 1, tasks.length));
        }

        private Update captureLastJobUpdate() {
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(jobService, org.mockito.Mockito.atLeastOnce()).updateById(eq(jobId), updateCaptor.capture(), eq(user));
            List<Update> updates = updateCaptor.getAllValues();
            return updates.get(updates.size() - 1);
        }
    }
}
