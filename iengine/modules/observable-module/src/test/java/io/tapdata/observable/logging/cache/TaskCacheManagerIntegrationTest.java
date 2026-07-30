package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TaskCacheManagerIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldIsolateTaskDirectoriesAndDispatchRecordsByTask() {
        List<MonitoringLogsDto> dispatched = new ArrayList<>();
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> dispatched.add(log),
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.activateTask("task-b", "Task B"));
            assertTrue(manager.append(log("task-a", "Task A", "message-a")));
            assertTrue(manager.append(log("task-b", "Task B", "message-b")));

            assertTrue(Files.isDirectory(tempDir.resolve("task-a").resolve("segments")));
            assertTrue(Files.isDirectory(tempDir.resolve("task-b").resolve("segments")));

            manager.pollOnce(CacheLogSink.FILE);
            manager.pollOnce(CacheLogSink.FILE);
        }

        assertTrue(dispatched.stream().anyMatch(log -> "task-a".equals(log.getTaskId())));
        assertTrue(dispatched.stream().anyMatch(log -> "task-b".equals(log.getTaskId())));
    }

    @Test
    void shouldPollTasksRoundRobinWhenOneTaskRemainsBusy() {
        List<MonitoringLogsDto> dispatched = new ArrayList<>();
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> dispatched.add(log),
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("noisy-task", "Noisy"));
            assertTrue(manager.activateTask("quiet-task", "Quiet"));
            assertTrue(manager.append(log("noisy-task", "Noisy", "one")));
            assertTrue(manager.append(log("noisy-task", "Noisy", "two")));
            assertTrue(manager.append(log("noisy-task", "Noisy", "three")));
            assertTrue(manager.append(log("quiet-task", "Quiet", "only")));

            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
        }

        assertTrue(dispatched.stream().anyMatch(log -> "quiet-task".equals(log.getTaskId())));
    }

    @Test
    void shouldRotateAndEvictOldestUnconsumedGeneration() throws IOException {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 1),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.append(log("task-a", "Task A", "two")));
            assertTrue(manager.append(log("task-a", "Task A", "three")));

            Path segments = tempDir.resolve("task-a").resolve("segments");
            try (Stream<Path> paths = Files.list(segments)) {
                assertTrue(paths.filter(Files::isDirectory).count() <= 2);
            }
        }

        verify(audit, atLeastOnce()).evict(
                eq("task-a"),
                eq("Task A"),
                anyLong(),
                eq(true),
                anyString(),
                anyLong(),
                anyLong());
    }

    @Test
    void shouldNotReportDataLossAfterBothSinksConsumeGeneration() {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 1),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertTrue(manager.append(log("task-a", "Task A", "two")));
        }

        verify(audit).evict(
                eq("task-a"),
                eq("Task A"),
                anyLong(),
                eq(false),
                anyString(),
                anyLong(),
                anyLong());
    }

    @Test
    void shouldApplyLowerBackupLimitWhenTaskCacheIsReopened() throws IOException {
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 2),
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.append(log("task-a", "Task A", "two")));
        }

        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 0),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            try (Stream<Path> paths = Files.list(tempDir.resolve("task-a").resolve("segments"))) {
                assertTrue(paths.filter(Files::isDirectory).count() <= 1);
            }
        }

        verify(audit, atLeastOnce()).evict(
                eq("task-a"),
                eq("Task A"),
                anyLong(),
                eq(true),
                anyString(),
                anyLong(),
                anyLong());
    }

    @Test
    void shouldStopResumeAndDeleteWithoutLateRecreation() {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "before stop")));

            manager.deactivateTask("task-a");
            assertFalse(manager.append(log("task-a", "Task A", "late")));

            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "after resume")));

            manager.deleteTaskCache("task-a");
            assertFalse(Files.exists(tempDir.resolve("task-a")));
            assertFalse(manager.append(log("task-a", "Task A", "after delete")));
            assertFalse(manager.activateTask("task-a", "Task A"));
            manager.deleteTaskCache("task-a");
        }
    }

    @Test
    void shouldRejectPathTraversingTaskId() {
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L, 1),
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertFalse(manager.activateTask("../outside", "bad"));
            assertFalse(manager.append(log("../outside", "bad", "message")));
            assertFalse(manager.activateTask(".", "root"));
            assertFalse(manager.activateTask("..", "parent"));
        }

        assertFalse(Files.exists(tempDir.getParent().resolve("outside")));
        assertFalse(Files.exists(tempDir.resolve("segments")));
    }

    private MonitoringLogsDto log(String taskId, String taskName, String message) {
        Date now = new Date();
        return MonitoringLogsDto.builder()
                .date(now)
                .timestamp(now.getTime())
                .level("INFO")
                .taskId(taskId)
                .taskName(taskName)
                .message(message)
                .build();
    }
}
