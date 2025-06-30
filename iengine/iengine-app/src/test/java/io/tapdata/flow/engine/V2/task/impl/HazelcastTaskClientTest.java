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

}
