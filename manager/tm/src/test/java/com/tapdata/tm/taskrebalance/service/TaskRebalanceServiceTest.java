package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceRepository;
import com.tapdata.tm.taskrebalance.rule.TaskRebalanceRuleService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

    private static class TestContext {
        private final ObjectId taskId = new ObjectId();
        private final ObjectId jobId = new ObjectId();
        private final UserDetail user = new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
        private final TaskRebalanceJobService jobService = mock(TaskRebalanceJobService.class);
        private final TaskService taskService = mock(TaskService.class);
        private final WorkerService workerService = mock(WorkerService.class);
        private final SettingsService settingsService = mock(SettingsService.class);
        private final TaskRebalanceRuleService ruleService = mock(TaskRebalanceRuleService.class);
        private final TaskRebalanceService service = new TaskRebalanceService(
                mock(TaskRebalanceRepository.class),
                jobService,
                taskService,
                workerService,
                mock(UserService.class),
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
    }
}
