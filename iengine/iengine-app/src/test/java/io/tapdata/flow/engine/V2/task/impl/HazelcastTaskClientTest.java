package io.tapdata.flow.engine.V2.task.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.JobStatus;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class HazelcastTaskClientTest {
    @Test
    void test_deduceSchema(){
        TaskDto taskDto = new TaskDto();
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        Assertions.assertNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }
    @Test
    void test_testRun(){
        TaskDto taskDto = new TaskDto();
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        Assertions.assertNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }
    @Test
    void test_migrate(){
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));
        assertNotNull(ReflectionTestUtils.getField(hazelcastTaskClient,"autoRecovery"));
    }

    @Test
    void testClose() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        HazelcastTaskClient hazelcastTaskClient = new HazelcastTaskClient(mock(Job.class),taskDto,mock(ClientMongoOperator.class),mock(ConfigurationCenter.class),mock(HazelcastInstance.class));

        ObsLoggerFactory mockObsLoggerFactory = mock(ObsLoggerFactory.class);
        ObsLogger obsLogger = mock(ObsLogger.class);
        when(mockObsLoggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
        try (MockedStatic<ObsLoggerFactory> mockObsLogger = mockStatic(ObsLoggerFactory.class)) {
            mockObsLogger.when(() -> ObsLoggerFactory.getInstance()).thenReturn(mockObsLoggerFactory);
            Assertions.assertDoesNotThrow(() -> {
                hazelcastTaskClient.close();
            });
        }
    }

    @Nested
    class testIsRunning {
        HazelcastTaskClient taskClient;
        TaskDto taskDto;
        Job mockJob;
        @BeforeEach
        void beforeEach() {
            // Setup
            taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            when(dag.getNodes()).thenReturn(new ArrayList<>());
            taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
            mockJob = mock(Job.class);
            when(mockJob.getId()).thenReturn(123L);

        }
        @Test
        public void testIsRunningNormal() {
            when(mockJob.getStatus()).thenReturn(JobStatus.STARTING);

            taskClient = new HazelcastTaskClient(
                    mockJob,
                    taskDto,
                    mock(ClientMongoOperator.class),
                    mock(ConfigurationCenter.class),
                    null
            );

            boolean isRunning = taskClient.isRunning();
            assertTrue(isRunning, "Task should be considered running when job is running");
        }

        @Test
        public void testIsRunningHandlesJobNotFoundException() {
            when(mockJob.getStatus()).thenThrow(new JobNotFoundException("Job not found"));

            taskClient = new HazelcastTaskClient(
                    mockJob,
                    taskDto,
                    mock(ClientMongoOperator.class),
                    mock(ConfigurationCenter.class),
                    null
            );

            boolean isRunning = taskClient.isRunning();
            assertFalse(isRunning, "Task should not be considered running when job is not found");
        }

        @Test
        public void testIsRunningHandlesException() {
            when(mockJob.getStatus()).thenThrow(new RuntimeException("test exception"));

            taskClient = new HazelcastTaskClient(
                    mockJob,
                    taskDto,
                    mock(ClientMongoOperator.class),
                    mock(ConfigurationCenter.class),
                    null
            );

            assertThrows(RuntimeException.class, () -> taskClient.isRunning());
        }
    }

    @Test
    public void testGetStatusHandlesJobNotFoundException() {
        // Setup
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

        Job mockJob = mock(Job.class);
        when(mockJob.getStatus()).thenThrow(new JobNotFoundException("Job not found"));
        when(mockJob.getId()).thenReturn(123L);

        HazelcastTaskClient taskClient = new HazelcastTaskClient(
                mockJob,
                taskDto,
                mock(ClientMongoOperator.class),
                mock(ConfigurationCenter.class),
                null
        );

        String status = taskClient.getStatus();
        assertNotNull(status, "Status should not be null");
    }

    @Nested
    class testStop {
        HazelcastTaskClient taskClient;
        TaskDto taskDto;
        Job mockJob;

        @BeforeEach
        void beforeEach() {
            taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            when(dag.getNodes()).thenReturn(new ArrayList<>());
            taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
            mockJob = mock(Job.class);
            when(mockJob.getId()).thenReturn(123L);
            taskClient = new HazelcastTaskClient(
                    mockJob,
                    taskDto,
                    mock(ClientMongoOperator.class),
                    mock(ConfigurationCenter.class),
                    null
            );
        }

        @Test
        void testReplacementClientHasFreshTerminationFlags() {
            when(mockJob.getStatus()).thenReturn(JobStatus.RUNNING);
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            try (MockedStatic<ObsLoggerFactory> factory = mockStatic(ObsLoggerFactory.class)) {
                ObsLoggerFactory loggerFactory = mock(ObsLoggerFactory.class);
                factory.when(ObsLoggerFactory::getInstance).thenReturn(loggerFactory);
                when(loggerFactory.getObsLogger(any(TaskDto.class))).thenReturn(mock(ObsLogger.class));
                when(mockJob.getStatus()).thenReturn(JobStatus.COMPLETED);
                assertTrue(taskClient.stop()); // Stop/close the old run before opening its replacement.
                when(mockJob.getStatus()).thenReturn(JobStatus.RUNNING);
                HazelcastTaskClient replacement = new HazelcastTaskClient(mockJob, taskDto,
                        mock(ClientMongoOperator.class), mock(ConfigurationCenter.class), null);
                try {
                    assertFalse(replacement.stop());
                    verify(mockJob, times(2)).suspend();
                } finally { replacement.close(); }
            }
        }

        @Test
        void testSuspendOnlyRequestedOnce() {
            // Job stays RUNNING (as it does while an async suspend request is in flight), so a
            // repeated stop() poll must not re-issue job.suspend() every time.
            when(mockJob.getStatus()).thenReturn(JobStatus.RUNNING);

            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());

            verify(mockJob, times(1)).suspend();
            verify(mockJob, never()).cancel();
        }

        @Test
        void testCancelOnlyRequestedOnce() {
            // Job stays SUSPENDED (as it does while an async cancel request is in flight), so a
            // repeated stop() poll must not re-issue job.cancel() every time.
            when(mockJob.getStatus()).thenReturn(JobStatus.SUSPENDED);

            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());

            verify(mockJob, never()).suspend();
            verify(mockJob, times(1)).cancel();
        }

        @Test
        void testTransientSuspendFailureCanBeRetried() {
            when(mockJob.getStatus()).thenReturn(JobStatus.RUNNING);
            doThrow(new IllegalStateException("job is restarting")).doNothing().when(mockJob).suspend();
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            verify(mockJob, times(2)).suspend();
        }

        @Test
        void testTransientCancelFailureCanBeRetried() {
            when(mockJob.getStatus()).thenReturn(JobStatus.SUSPENDED);
            doThrow(new IllegalStateException("job changed state")).doNothing().when(mockJob).cancel();
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());
            verify(mockJob, times(2)).cancel();
        }

        @Test
        void testSuspendRejectedByIllegalStateExceptionIsNotRetried() {
            when(mockJob.getStatus()).thenReturn(JobStatus.RUNNING);
            doThrow(new IllegalStateException("already terminating")).when(mockJob).suspend();

            assertFalse(taskClient.stop());
            assertFalse(taskClient.stop());

            verify(mockJob, times(1)).suspend();
        }
    }

    @Test
    public void testGetJetStatusHandlesJobNotFoundException() {
        // Setup
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

        Job mockJob = mock(Job.class);
        when(mockJob.getStatus()).thenThrow(new JobNotFoundException("Job not found"));
        when(mockJob.getId()).thenReturn(123L);

        HazelcastTaskClient taskClient = new HazelcastTaskClient(
                mockJob,
                taskDto,
                mock(ClientMongoOperator.class),
                mock(ConfigurationCenter.class),
                null
        );

        JobStatus status = taskClient.getJetStatus();
        assertEquals(JobStatus.FAILED, status);
    }

    @Test
    public void stopWaitsForJetCancellationToBecomeTerminal() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

        Job mockJob = mock(Job.class);
        when(mockJob.getStatus()).thenReturn(
                JobStatus.RUNNING,
                JobStatus.RUNNING,
                JobStatus.SUSPENDED,
                JobStatus.COMPLETING,
                JobStatus.COMPLETING,
                JobStatus.COMPLETED);
        HazelcastTaskClient taskClient = spy(new HazelcastTaskClient(
                mockJob,
                taskDto,
                mock(ClientMongoOperator.class),
                mock(ConfigurationCenter.class),
                mock(HazelcastInstance.class)));
        doNothing().when(taskClient).close();

        assertTrue(taskClient.stop());
        verify(mockJob).suspend();
        verify(mockJob, atLeastOnce()).cancel();
    }

    @Test
    public void stopDoesNotCancelWhileSuspendIsStillTerminating() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

        AtomicInteger statusReads = new AtomicInteger();
        Job mockJob = mock(Job.class);
        when(mockJob.getStatus()).thenAnswer(invocation -> {
            int read = statusReads.getAndIncrement();
            return switch (read) {
                case 0, 1, 2, 3 -> JobStatus.RUNNING;
                case 4, 5 -> JobStatus.SUSPENDED;
                default -> JobStatus.COMPLETED;
            };
        });
        doAnswer(invocation -> {
            if (statusReads.get() <= 4) {
                throw new IllegalStateException(
                        "Cannot CANCEL_FORCEFUL: Job is already terminating in mode: SUSPEND_FORCEFUL");
            }
            return null;
        }).when(mockJob).cancel();

        HazelcastTaskClient taskClient = spy(new HazelcastTaskClient(
                mockJob,
                taskDto,
                mock(ClientMongoOperator.class),
                mock(ConfigurationCenter.class),
                mock(HazelcastInstance.class)));
        doNothing().when(taskClient).close();

        assertTrue(taskClient.stop());
        verify(mockJob).suspend();
        verify(mockJob).cancel();
    }

    @Test
    public void stopContinuesWhenSuspendWasAlreadySubmitted() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        DAG dag = mock(DAG.class);
        taskDto.setDag(dag);
        when(dag.getNodes()).thenReturn(new ArrayList<>());
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

        Job mockJob = mock(Job.class);
        when(mockJob.getStatus()).thenReturn(
                JobStatus.RUNNING,
                JobStatus.SUSPENDED,
                JobStatus.COMPLETING,
                JobStatus.COMPLETED);
        doThrow(new IllegalStateException(
                "Cannot SUSPEND_FORCEFUL: Job is already terminating in mode: SUSPEND_FORCEFUL"))
                .when(mockJob).suspend();

        HazelcastTaskClient taskClient = spy(new HazelcastTaskClient(
                mockJob,
                taskDto,
                mock(ClientMongoOperator.class),
                mock(ConfigurationCenter.class),
                mock(HazelcastInstance.class)));
        doNothing().when(taskClient).close();

        assertTrue(taskClient.stop());
        verify(mockJob).suspend();
        verify(mockJob).cancel();
    }

}
