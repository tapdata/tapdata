package com.tapdata.tm.taskrebalance.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceDto;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceEntity;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceRepository;
import com.tapdata.tm.taskrebalance.rule.TaskRebalanceRuleService;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
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

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", context.rebalanceId, job, context.user, new AtomicBoolean(false), new AtomicReference<String>());

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

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", context.rebalanceId, job, context.user, new AtomicBoolean(false), new AtomicReference<String>());

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

        ReflectionTestUtils.invokeMethod(context.service, "handleStartFailure", context.rebalanceId, job, context.user, "boom", context.user, abortFlag, abortReason);

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
        when(context.workerService.findAvailableAgentBySystem(any(List.class))).thenReturn(List.of());

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", context.rebalanceId, job, context.user, abortFlag, abortReason);

        assertTrue(abortFlag.get());
        assertTrue(abortReason.get().contains("Target agent target offline"));
        Update update = context.captureLastJobUpdate();
        assertEquals(TaskRebalanceJobStatus.INVALID_AGENT, update.getUpdateObject().get("$set", org.bson.Document.class).get(TaskRebalanceJobDto.FIELD_STATUS));
    }

    @Test
    @DisplayName("pending job does not execute when status transition loses cancel race")
    void pendingJobSkipsExecutionWhenTransitionFails() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.PENDING);
        TaskDto runningOnSource = context.task(TaskDto.STATUS_RUNNING, "source");
        context.stubTaskLookups(runningOnSource);
        when(context.ruleService.isMovableAtExecution(runningOnSource)).thenReturn(true);
        UpdateResult failedTransition = context.updateResult(0);
        when(context.jobService.update(any(Query.class), any(Update.class), eq(context.user))).thenReturn(failedTransition);

        ReflectionTestUtils.invokeMethod(context.service, "executeJob", context.rebalanceId, job, context.user, new AtomicBoolean(false), new AtomicReference<String>());

        verify(context.taskService, never()).pause(any(TaskDto.class), eq(context.user), eq(false));
        verify(context.taskService, never()).updateById(eq(context.taskId), any(Update.class), eq(context.user));
        verify(context.taskService, never()).start(any(TaskDto.class), eq(context.user), eq("11"));
    }

    @Test
    @DisplayName("cancelJob reports active non-pending job cannot be cancelled")
    void cancelJobRejectsRunningJob() {
        TestContext context = new TestContext();
        TaskRebalanceJobDto job = context.job(TaskRebalanceJobStatus.STARTING);
        job.setRebalanceId("rebalance-id");
        when(context.jobService.findOne(any(Query.class), eq(context.user))).thenReturn(job);

        BizException exception = assertThrows(BizException.class,
                () -> context.service.cancelJob("rebalance-id", context.taskId.toHexString(), context.user));

        assertEquals("task.rebalance.jobCannotCancel", exception.getErrorCode());
        verify(context.jobService, never()).update(any(Query.class), any(Update.class), eq(context.user));
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

    @Test
    @DisplayName("read APIs require task rebalance view permission")
    void readApisRequireViewPermission() {
        TestContext context = new TestContext();
        context.user.setFreeAuth(false);
        TaskRebalanceService service = spy(context.service);
        doReturn(0L).when(service).count(any(Query.class), eq(context.user));

        try (MockedStatic<DataPermissionHelper> mocked = mockStatic(DataPermissionHelper.class)) {
            mockPermissionCheck(mocked);

            service.hasActive(context.user);

            verifyPermissionCheck(mocked, context.user, DataPermissionActionEnums.View);
        }
    }

    @Test
    @DisplayName("operation APIs require task rebalance edit permission")
    void operationApisRequireEditPermission() {
        TestContext context = new TestContext();
        context.user.setFreeAuth(false);

        try (MockedStatic<DataPermissionHelper> mocked = mockStatic(DataPermissionHelper.class)) {
            mockPermissionCheck(mocked);

            BizException exception = assertThrows(BizException.class,
                    () -> context.service.createAndExecute(null, context.user));

            assertEquals("task.rebalance.noTask", exception.getErrorCode());
            verifyPermissionCheck(mocked, context.user, DataPermissionActionEnums.Edit);
        }
    }

    @Test
    @DisplayName("active TM marks running rebalance owner")
    void markRunningRebalancesOwner() {
        TestContext context = new TestContext();

        context.service.markRunningRebalancesOwner();

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(context.repository).update(queryCaptor.capture(), updateCaptor.capture(), isNull());
        assertEquals(TaskRebalanceStatus.RUNNING, queryCaptor.getValue().getQueryObject().get(TaskRebalanceDto.FIELD_STATUS));
        assertEquals(context.executeOwner, updateCaptor.getValue().getUpdateObject().get("$set", org.bson.Document.class).get(TaskRebalanceDto.FIELD_EXECUTE_OWNER));
    }

    @Test
    @DisplayName("create rebalance uses CREATING active marker before jobs and activates RUNNING after jobs")
    void createAndExecuteActivatesAfterJobsSaved() {
        TestContext context = new TestContext();
        context.stubCreateValidation();
        when(context.repository.acquireActiveCreating(any(TaskRebalanceEntity.class), eq(context.user)))
                .thenAnswer(invocation -> Optional.of(invocation.getArgument(0)));
        UpdateResult successUpdate = context.updateResult(1);
        when(context.repository.update(any(Query.class), any(Update.class), eq(context.user))).thenReturn(successUpdate);

        context.service.createAndExecute(context.preview(), context.user);

        ArgumentCaptor<TaskRebalanceEntity> rebalanceCaptor = ArgumentCaptor.forClass(TaskRebalanceEntity.class);
        verify(context.repository).acquireActiveCreating(rebalanceCaptor.capture(), eq(context.user));
        assertEquals(TaskRebalanceStatus.CREATING, rebalanceCaptor.getValue().getStatus());
        assertEquals(Boolean.TRUE, rebalanceCaptor.getValue().getIsActived());
        ArgumentCaptor<List<TaskRebalanceJobDto>> jobsCaptor = ArgumentCaptor.forClass(List.class);
        verify(context.jobService).save(jobsCaptor.capture(), eq(context.user));
        assertEquals("initial_sync+cdc", jobsCaptor.getValue().get(0).getType());
        assertEquals("sync", jobsCaptor.getValue().get(0).getSyncType());

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(context.repository, org.mockito.Mockito.atLeastOnce()).update(queryCaptor.capture(), updateCaptor.capture(), eq(context.user));
        org.bson.Document activateSet = updateCaptor.getAllValues().get(updateCaptor.getAllValues().size() - 1).getUpdateObject().get("$set", org.bson.Document.class);
        assertEquals(TaskRebalanceStatus.RUNNING, activateSet.get(TaskRebalanceDto.FIELD_STATUS));
        assertEquals(TaskRebalanceStatus.CREATING, queryCaptor.getAllValues().get(queryCaptor.getAllValues().size() - 1).getQueryObject().get(TaskRebalanceDto.FIELD_STATUS));
    }

    @Test
    @DisplayName("create rebalance rejects when another active marker exists")
    void createAndExecuteRejectsActiveMarker() {
        TestContext context = new TestContext();
        context.stubCreateValidation();
        TaskRebalanceEntity existing = new TaskRebalanceEntity();
        existing.setId(new ObjectId());
        existing.setStatus(TaskRebalanceStatus.RUNNING);
        existing.setIsActived(true);
        when(context.repository.acquireActiveCreating(any(TaskRebalanceEntity.class), eq(context.user))).thenReturn(Optional.of(existing));

        BizException exception = assertThrows(BizException.class, () -> context.service.createAndExecute(context.preview(), context.user));

        assertEquals("task.rebalance.alreadyRunning", exception.getErrorCode());
        verify(context.jobService, never()).save(any(List.class), eq(context.user));
    }

    @Test
    @DisplayName("create rebalance marks creating record failed when job save fails")
    void createAndExecuteFailsCreatingRecordWhenJobsFail() {
        TestContext context = new TestContext();
        context.stubCreateValidation();
        when(context.repository.acquireActiveCreating(any(TaskRebalanceEntity.class), eq(context.user)))
                .thenAnswer(invocation -> Optional.of(invocation.getArgument(0)));
        UpdateResult successUpdate = context.updateResult(1);
        when(context.repository.update(any(Query.class), any(Update.class), eq(context.user))).thenReturn(successUpdate);
        doThrow(new RuntimeException("save jobs failed")).when(context.jobService).save(any(List.class), eq(context.user));

        assertThrows(RuntimeException.class, () -> context.service.createAndExecute(context.preview(), context.user));

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(context.repository).update(any(Query.class), updateCaptor.capture(), eq(context.user));
        org.bson.Document updateObject = updateCaptor.getValue().getUpdateObject();
        assertEquals(TaskRebalanceStatus.FAILED, updateObject.get("$set", org.bson.Document.class).get(TaskRebalanceDto.FIELD_STATUS));
        assertTrue(updateObject.get("$unset", org.bson.Document.class).containsKey(TaskRebalanceDto.FIELD_IS_ACTIVED));
    }

    @Test
    @DisplayName("scheduler expires creating rebalance after one minute and does not execute it")
    void scheduleExpiresCreatingRebalance() {
        TestContext context = new TestContext();
        TaskRebalanceService service = spy(context.service);
        doReturn(List.of()).when(service).findAll(any(Query.class));

        service.scheduleOnce();

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(context.repository).update(queryCaptor.capture(), updateCaptor.capture(), isNull());
        org.bson.Document query = queryCaptor.getValue().getQueryObject();
        assertEquals(TaskRebalanceStatus.CREATING, query.get(TaskRebalanceDto.FIELD_STATUS));
        assertEquals(Boolean.TRUE, query.get(TaskRebalanceDto.FIELD_IS_ACTIVED));
        assertTrue(updateCaptor.getValue().getUpdateObject().get("$unset", org.bson.Document.class).containsKey(TaskRebalanceDto.FIELD_IS_ACTIVED));
        verify(service, never()).execute(anyString(), any(UserDetail.class));
    }

    @Test
    @DisplayName("scheduler skips null rebalance rows")
    void scheduleSkipsNullRebalanceRows() {
        TestContext context = new TestContext();
        TaskRebalanceService service = spy(context.service);
        doReturn(Collections.singletonList(null)).when(service).findAll(any(Query.class));

        service.scheduleOnce();

        verify(context.userService, never()).loadUserById(any(ObjectId.class));
        verify(service, never()).execute(anyString(), any(UserDetail.class));
    }

    @Test
    @DisplayName("execute skips when rebalance owner belongs to another TM")
    void executeSkipsWhenOwnerChanged() {
        TestContext context = new TestContext();
        context.stubRebalanceOwner("other-tm");

        context.service.execute(context.rebalanceId, context.user);

        verify(context.jobService, never()).findAllDto(any(Query.class), eq(context.user));
    }

    @Test
    @DisplayName("updateProgress writes parent counters from aggregated job status")
    void updateProgressUsesAggregatedJobStatus() {
        TestContext context = new TestContext();
        TaskRebalanceJobService.StatusStatistics statistics = new TaskRebalanceJobService.StatusStatistics();
        statistics.record(TaskRebalanceJobStatus.OK, 2);
        statistics.record(TaskRebalanceJobStatus.CANCELLED, 1);
        statistics.record(TaskRebalanceJobStatus.START_TIMEOUT, 1);
        when(context.jobService.countStatusByRebalanceId(context.rebalanceId)).thenReturn(statistics);

        ReflectionTestUtils.invokeMethod(context.service, "updateProgress", context.rebalanceId, "abort", context.user);

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(context.repository).updateFirst(queryCaptor.capture(), updateCaptor.capture(), eq(context.user));
        assertEquals(context.rebalanceObjectId, queryCaptor.getValue().getQueryObject().get("_id"));
        org.bson.Document updateObject = updateCaptor.getValue().getUpdateObject();
        org.bson.Document set = updateObject.get("$set", org.bson.Document.class);
        assertEquals(4, set.get(TaskRebalanceDto.FIELD_TOTAL_COUNT));
        assertEquals(0, set.get(TaskRebalanceDto.FIELD_PENDING_COUNT));
        assertEquals(0, set.get(TaskRebalanceDto.FIELD_STOPPING_COUNT));
        assertEquals(0, set.get(TaskRebalanceDto.FIELD_STARTING_COUNT));
        assertEquals(2, set.get(TaskRebalanceDto.FIELD_OK_COUNT));
        assertEquals(1, set.get(TaskRebalanceDto.FIELD_CANCELLED_COUNT));
        assertEquals(1, set.get(TaskRebalanceDto.FIELD_FAILED_COUNT));
        assertEquals("abort", set.get(TaskRebalanceDto.FIELD_ERROR_MESG));
        assertEquals(TaskRebalanceStatus.FAILED, set.get(TaskRebalanceDto.FIELD_STATUS));
        assertTrue(updateObject.get("$unset", org.bson.Document.class).containsKey(TaskRebalanceDto.FIELD_IS_ACTIVED));
        verify(context.jobService, never()).findAllDto(any(Query.class), eq(context.user));
    }

    @Test
    @DisplayName("preview task query sorts by start time and id")
    void findStatTasksSortsByStartTimeAndId() {
        TestContext context = new TestContext();
        when(context.taskService.findAll(any(Query.class))).thenReturn(List.of());

        ReflectionTestUtils.invokeMethod(context.service, "findStatTasks");

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(context.taskService).findAll(queryCaptor.capture());
        org.bson.Document sort = queryCaptor.getValue().getSortObject();
        assertEquals(1, sort.get("startTime"));
        assertEquals(1, sort.get("_id"));
    }

    private void mockPermissionCheck(MockedStatic<DataPermissionHelper> mocked) {
        mocked.when(() -> DataPermissionHelper.check(
                any(UserDetail.class),
                any(DataPermissionMenuEnums.class),
                any(DataPermissionActionEnums.class),
                any(DataPermissionDataTypeEnums.class),
                any(),
                any(),
                any()
        )).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
    }

    private void verifyPermissionCheck(MockedStatic<DataPermissionHelper> mocked, UserDetail user, DataPermissionActionEnums action) {
        mocked.verify(() -> DataPermissionHelper.check(
                eq(user),
                eq(DataPermissionMenuEnums.TaskRebalance),
                eq(action),
                eq(DataPermissionDataTypeEnums.Task),
                isNull(),
                any(),
                any()
        ));
    }

    private static class TestContext {
        private final ObjectId taskId = new ObjectId();
        private final ObjectId jobId = new ObjectId();
        private final ObjectId rebalanceObjectId = new ObjectId();
        private final String rebalanceId = rebalanceObjectId.toHexString();
        private final String executeOwner = "current-tm";
        private final UserDetail user = new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
        private final TaskRebalanceRepository repository = mock(TaskRebalanceRepository.class);
        private final TaskRebalanceJobService jobService = mock(TaskRebalanceJobService.class);
        private final TaskService taskService = mock(TaskService.class);
        private final WorkerService workerService = mock(WorkerService.class);
        private final UserService userService = mock(UserService.class);
        private final SettingsService settingsService = mock(SettingsService.class);
        private final DBLockConfiguration dbLockConfiguration = mock(DBLockConfiguration.class);
        private final TaskRebalanceRuleService ruleService = mock(TaskRebalanceRuleService.class);
        private final TaskRebalanceService service = new TaskRebalanceService(
                repository,
                jobService,
                taskService,
                workerService,
                userService,
                settingsService,
                dbLockConfiguration,
                ruleService
        );

        private TestContext() {
            user.setFreeAuth(true);
            new SettingUtil(settingsService);
            when(settingsService.getByCategoryAndKey(any(String.class), any(String.class))).thenReturn("30");
            when(dbLockConfiguration.getOwner()).thenReturn(executeOwner);
            stubRebalanceOwner(executeOwner);
            when(workerService.findAvailableAgentBySystem(any(List.class))).thenReturn(List.of(worker("target")));
            doAnswer(invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }).when(jobService).runAsRebalanceOperation(any(Runnable.class));
            UpdateResult successUpdate = updateResult(1);
            when(jobService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(successUpdate);
        }

        private TaskRebalancePreviewVo preview() {
            TaskRebalancePreviewVo preview = new TaskRebalancePreviewVo();
            TaskRebalancePreviewVo.TaskPreview task = new TaskRebalancePreviewVo.TaskPreview();
            task.setTaskId(taskId.toHexString());
            task.setTaskName("task");
            task.setType("initial_sync+cdc");
            task.setSyncType("sync");
            task.setSourceAgentId("source");
            task.setTargetAgentId("target");
            task.setMovable(true);
            task.setChanged(true);
            preview.setTasks(List.of(task));
            return preview;
        }

        private void stubCreateValidation() {
            when(workerService.findAvailableAgentBySystem(any(List.class))).thenReturn(List.of(worker("source"), worker("target")));
            TaskDto sourceTask = task(TaskDto.STATUS_RUNNING, "source");
            when(taskService.findByTaskId(eq(taskId), any(String[].class))).thenReturn(sourceTask);
            TaskRebalancePreviewVo.TaskPreview current = new TaskRebalancePreviewVo.TaskPreview();
            current.setMovable(true);
            current.setSourceAgentId("source");
            when(ruleService.evaluate(eq(sourceTask), any(Set.class))).thenReturn(current);
            when(ruleService.evaluate(eq(sourceTask), any(Set.class), anyInt())).thenReturn(current);
            when(jobService.hasAnyActiveJob(any(List.class), eq(user))).thenReturn(false);
        }

        private Worker worker(String processId) {
            Worker worker = new Worker();
            worker.setProcessId(processId);
            return worker;
        }

        private TaskRebalanceJobDto job(String status) {
            TaskRebalanceJobDto job = new TaskRebalanceJobDto();
            job.setId(jobId);
            job.setRebalanceId(rebalanceId);
            job.setTaskId(taskId.toHexString());
            job.setStatus(status);
            job.setSourceAgentId("source");
            job.setTargetAgentId("target");
            when(jobService.findById(jobId, user)).thenReturn(job);
            return job;
        }

        private void stubRebalanceOwner(String owner) {
            TaskRebalanceEntity rebalance = new TaskRebalanceEntity();
            rebalance.setId(rebalanceObjectId);
            rebalance.setExecuteOwner(owner);
            when(repository.findById(rebalanceObjectId, user)).thenReturn(Optional.of(rebalance));
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

        private UpdateResult updateResult(long modifiedCount) {
            UpdateResult result = mock(UpdateResult.class);
            when(result.getModifiedCount()).thenReturn(modifiedCount);
            return result;
        }
    }
}
