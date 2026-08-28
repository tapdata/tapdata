package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskOpRespDto;
import io.tapdata.dql.recovery.DqlRecoveryTaskSnapshot;
import io.tapdata.dql.recovery.DqlRecoveryTaskStopException;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TapdataTaskSchedulerDqlRecoveryTest {

    @Test
    void usesLiveTaskClientStatusWhenTaskDtoWasLoadedBeforeRunningClaim() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        String taskId = taskObjectId.toHexString();
        TaskDto staleTask = new TaskDto();
        staleTask.setId(taskObjectId);
        staleTask.setVersion(7L);
        staleTask.setStatus(TaskDto.STATUS_WAIT_RUN);
        staleTask.setName("dql-recovery-task");

        TaskClient<TaskDto> client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(staleTask);
        when(client.getStatus()).thenReturn(TaskDto.STATUS_RUNNING, TaskDto.STATUS_COMPLETE);

        TaskDto stoppedTask = new TaskDto();
        stoppedTask.setId(taskObjectId);
        stoppedTask.setVersion(7L);
        stoppedTask.setStatus(TaskDto.STATUS_STOP);
        stoppedTask.setName(staleTask.getName());
        TaskOpRespDto stopResponse = new TaskOpRespDto();
        stopResponse.setSuccessIds(List.of(taskId));

        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        when(mongoOperator.updateById(any(), anyString(), eq(taskId), eq(TaskOpRespDto.class)))
                .thenReturn(stopResponse);
        when(mongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
                .thenReturn(stoppedTask);
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);
        ReflectionTestUtils.setField(scheduler, "taskClientMap",
                new ConcurrentHashMap<>(Map.of(taskId, client)));

        ObsLoggerFactory loggerFactory = mock(ObsLoggerFactory.class);
        ObsLogger logger = mock(ObsLogger.class);
        when(loggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(logger);
        try (MockedStatic<ObsLoggerFactory> mockedLoggerFactory = org.mockito.Mockito.mockStatic(ObsLoggerFactory.class)) {
            mockedLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(loggerFactory);

            DqlRecoveryTaskSnapshot snapshot = scheduler.stopTaskForDqlRecovery(taskId, 7L);

            assertTrue(snapshot.wasRunning());
            verify(mongoOperator).updateById(any(), contains("Task/systemStop"), eq(taskId), eq(TaskOpRespDto.class));
            verify(client, never()).stop();
        }
    }

    @Test
    void waitsForStoppingTaskToBecomeStableStoppedBeforeRecovery() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        String taskId = taskObjectId.toHexString();
        TaskDto stoppingTask = new TaskDto();
        stoppingTask.setId(taskObjectId);
        stoppingTask.setVersion(7L);
        stoppingTask.setStatus(TaskDto.STATUS_STOPPING);
        stoppingTask.setName("dql-recovery-task");
        TaskDto stoppedTask = new TaskDto();
        stoppedTask.setId(taskObjectId);
        stoppedTask.setVersion(7L);
        stoppedTask.setStatus(TaskDto.STATUS_STOP);
        stoppedTask.setName(stoppingTask.getName());

        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        when(mongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
                .thenReturn(stoppingTask, stoppedTask);
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);
        ReflectionTestUtils.setField(scheduler, "taskClientMap", new ConcurrentHashMap<>());

        DqlRecoveryTaskSnapshot snapshot = scheduler.stopTaskForDqlRecovery(taskId, 7L);

        assertEquals(TaskDto.STATUS_STOPPING, snapshot.statusBefore());
        assertFalse(snapshot.wasStopped());
    }

    @Test
    void allowsNonRunningTaskWithoutStartingOrStoppingFormalClient() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        String taskId = taskObjectId.toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskObjectId);
        task.setVersion(7L);
        task.setStatus(TaskDto.STATUS_EDIT);
        task.setName("dql-recovery-task");

        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        when(mongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
                .thenReturn(task);
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);
        ReflectionTestUtils.setField(scheduler, "taskClientMap", new ConcurrentHashMap<>());

        DqlRecoveryTaskSnapshot snapshot = scheduler.stopTaskForDqlRecovery(taskId, 7L);

        assertEquals(TaskDto.STATUS_EDIT, snapshot.statusBefore());
        assertFalse(snapshot.wasRunning());
    }

    @Test
    void waitsForLiveClientToFinishStoppingAfterTmReportsStopped() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        String taskId = taskObjectId.toHexString();
        TaskDto stoppedTask = new TaskDto();
        stoppedTask.setId(taskObjectId);
        stoppedTask.setVersion(7L);
        stoppedTask.setStatus(TaskDto.STATUS_STOP);
        stoppedTask.setName("dql-recovery-task");

        TaskClient<TaskDto> client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(stoppedTask);
        when(client.getStatus()).thenReturn(TaskDto.STATUS_STOPPING, TaskDto.STATUS_STOPPING, TaskDto.STATUS_ERROR);
        when(client.stop()).thenReturn(true);

        TaskOpRespDto stopResponse = new TaskOpRespDto();
        stopResponse.setSuccessIds(List.of(taskId));
        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        when(mongoOperator.updateById(any(), anyString(), eq(taskId), eq(TaskOpRespDto.class)))
                .thenReturn(stopResponse);
        when(mongoOperator.findOne(any(Query.class), eq(ConnectorConstant.TASK_COLLECTION), eq(TaskDto.class)))
                .thenReturn(stoppedTask);
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);
        ReflectionTestUtils.setField(scheduler, "taskClientMap",
                new ConcurrentHashMap<>(Map.of(taskId, client)));

        ObsLoggerFactory loggerFactory = mock(ObsLoggerFactory.class);
        ObsLogger logger = mock(ObsLogger.class);
        when(loggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(logger);
        try (MockedStatic<ObsLoggerFactory> mockedLoggerFactory = org.mockito.Mockito.mockStatic(ObsLoggerFactory.class)) {
            mockedLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(loggerFactory);

            DqlRecoveryTaskSnapshot snapshot = scheduler.stopTaskForDqlRecovery(taskId, 7L);

            assertTrue(snapshot.wasStopped());
            verify(client, atLeast(2)).getStatus();
        }
    }

    @Test
    void carriesPreRecoverySnapshotWhenFormalStopFails() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        String taskId = taskObjectId.toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskObjectId);
        task.setVersion(7L);
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setName("dql-recovery-task");

        TaskClient<TaskDto> client = mock(TaskClient.class);
        when(client.getTask()).thenReturn(task);
        when(client.getStatus()).thenReturn(TaskDto.STATUS_RUNNING);

        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        when(mongoOperator.updateById(any(), contains("Task/systemStop"), eq(taskId), eq(TaskOpRespDto.class)))
                .thenThrow(new IllegalStateException("TM unavailable"));
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);
        ReflectionTestUtils.setField(scheduler, "taskClientMap",
                new ConcurrentHashMap<>(Map.of(taskId, client)));

        DqlRecoveryTaskStopException exception = org.junit.jupiter.api.Assertions.assertThrows(
                DqlRecoveryTaskStopException.class,
                () -> scheduler.stopTaskForDqlRecovery(taskId, 7L));

        assertTrue(exception.snapshot().wasRunning());
        assertEquals(taskObjectId, exception.snapshot().task().getId());
        assertInstanceOf(IllegalStateException.class, exception.getCause());
    }

    @Test
    void restoresFormalTaskThroughTmSystemStartEndpoint() {
        TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
        ObjectId taskObjectId = new ObjectId();
        TaskDto task = new TaskDto();
        task.setId(taskObjectId);
        task.setName("dql-recovery-task");
        task.setStatus(TaskDto.STATUS_STOP);

        ClientMongoOperator mongoOperator = mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(scheduler, "clientMongoOperator", mongoOperator);

        ReflectionTestUtils.invokeMethod(scheduler, "startTaskThroughTmForDqlRecovery", task);

        verify(mongoOperator).postOne(
                eq(null), contains("Task/systemStart/" + taskObjectId.toHexString()), eq(Void.class));
    }
}
