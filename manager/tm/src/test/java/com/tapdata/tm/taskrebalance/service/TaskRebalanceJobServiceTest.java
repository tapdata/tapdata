package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceJobRepository;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class TaskRebalanceJobServiceTest {

    private final TaskRebalanceJobService jobService =
            new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class));

    @Test
    @DisplayName("default state has no rebalance flags")
    void defaultFlagsAreFalse() {
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("runAsRebalanceOperation sets both flags and restores them")
    void rebalanceOperationFlags() {
        jobService.runAsRebalanceOperation(() -> {
            assertTrue(jobService.isCheckBypassed());
            assertTrue(jobService.isRebalanceOperation());
        });
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("nested runAsRebalanceOperation restores outer state correctly")
    void nestedRestoresOuterState() {
        jobService.runAsRebalanceOperation(() -> {
            assertTrue(jobService.isCheckBypassed());
            assertTrue(jobService.isRebalanceOperation());
            jobService.runAsRebalanceOperation(() -> {
                assertTrue(jobService.isCheckBypassed());
                assertTrue(jobService.isRebalanceOperation());
            });
            assertTrue(jobService.isCheckBypassed());
            assertTrue(jobService.isRebalanceOperation());
        });
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("flags are properly restored even when the runnable throws")
    void flagsRestoredOnException() {
        try {
            jobService.runAsRebalanceOperation(() -> {
                throw new RuntimeException("boom");
            });
        } catch (RuntimeException ignored) {
        }
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("flags are thread-local and do not leak across threads")
    void flagsAreThreadLocal() throws InterruptedException {
        final boolean[] otherThreadBypassed = {true};
        final boolean[] otherThreadRebalance = {true};
        jobService.runAsRebalanceOperation(() -> {
            Thread other = new Thread(() -> {
                otherThreadBypassed[0] = jobService.isCheckBypassed();
                otherThreadRebalance[0] = jobService.isRebalanceOperation();
            });
            other.start();
            try {
                other.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertFalse(otherThreadBypassed[0]);
        assertFalse(otherThreadRebalance[0]);
    }

    @Test
    @DisplayName("hasActiveJob skips blank task id")
    void hasActiveJobReturnsFalseWhenTaskIdIsNull() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));

        assertFalse(service.hasActiveJob(null, mock(UserDetail.class)));

        verify(service, never()).count(any(Query.class), any(UserDetail.class));
    }

    @Test
    @DisplayName("hasActiveJob queries task id and active statuses")
    void hasActiveJobQueriesActiveStatus() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));
        UserDetail user = mock(UserDetail.class);
        doReturn(1L).when(service).count(any(Query.class), eq(user));

        assertTrue(service.hasActiveJob("task-1", user));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(service).count(queryCaptor.capture(), eq(user));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("task-1", query.get("taskId"));
        assertEquals(TaskRebalanceJobStatus.ACTIVE_STATUS, query.get("status", Document.class).get("$in"));
    }

    @Test
    @DisplayName("hasAnyActiveJob skips empty task ids")
    void hasAnyActiveJobReturnsFalseWhenTaskIdsAreEmpty() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));

        assertFalse(service.hasAnyActiveJob(List.of(), mock(UserDetail.class)));

        verify(service, never()).count(any(Query.class), any(UserDetail.class));
    }

    @Test
    @DisplayName("hasAnyActiveJob queries task ids and active statuses")
    void hasAnyActiveJobQueriesActiveStatus() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));
        UserDetail user = mock(UserDetail.class);
        List<String> taskIds = List.of("task-1", "task-2");
        doReturn(0L).when(service).count(any(Query.class), eq(user));

        assertFalse(service.hasAnyActiveJob(taskIds, user));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(service).count(queryCaptor.capture(), eq(user));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals(taskIds, query.get("taskId", Document.class).get("$in"));
        assertEquals(TaskRebalanceJobStatus.ACTIVE_STATUS, query.get("status", Document.class).get("$in"));
    }

    @Test
    @DisplayName("hasBlockingActiveJob queries only stopping and starting jobs")
    void hasBlockingActiveJobQueriesBlockingStatuses() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));
        UserDetail user = mock(UserDetail.class);
        doReturn(List.of()).when(service).findAllDto(any(Query.class), eq(user));

        assertFalse(service.hasBlockingActiveJob("task-1", 300000L, user));

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(service).findAllDto(queryCaptor.capture(), eq(user));
        Document query = queryCaptor.getValue().getQueryObject();
        assertEquals("task-1", query.get("taskId"));
        assertEquals(List.of(TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING),
                query.get("status", Document.class).get("$in"));
    }

    @Test
    @DisplayName("hasBlockingActiveJob returns true when stopping job has not timed out")
    void hasBlockingActiveJobReturnsTrueBeforeTimeout() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));
        UserDetail user = mock(UserDetail.class);
        TaskRebalanceJobDto job = blockingJob(TaskRebalanceJobStatus.STOPPING, System.currentTimeMillis() - 1000L);
        doReturn(List.of(job)).when(service).findAllDto(any(Query.class), eq(user));

        assertTrue(service.hasBlockingActiveJob("task-1", 300000L, user));
    }

    @Test
    @DisplayName("hasBlockingActiveJob returns false when starting job timed out")
    void hasBlockingActiveJobReturnsFalseAfterTimeout() {
        TaskRebalanceJobService service = spy(new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class)));
        UserDetail user = mock(UserDetail.class);
        TaskRebalanceJobDto job = blockingJob(TaskRebalanceJobStatus.STARTING, System.currentTimeMillis() - 600000L);
        doReturn(List.of(job)).when(service).findAllDto(any(Query.class), eq(user));

        assertFalse(service.hasBlockingActiveJob("task-1", 300000L, user));
    }

    private TaskRebalanceJobDto blockingJob(String status, long beginAt) {
        TaskRebalanceJobDto job = new TaskRebalanceJobDto();
        job.setTaskId("task-1");
        job.setStatus(status);
        job.setBeginAt(new Date(beginAt));
        job.setCreateAt(new Date(beginAt));
        return job;
    }
}
