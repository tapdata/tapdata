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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void shouldRestoreConsumedStateWhenGenerationIsReopened() {
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 1),
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
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
    void shouldReloadBackupLimitWhenTaskResumes() throws IOException {
        AtomicReference<CacheObserveLogConfig> currentConfig = new AtomicReference<>(
                new CacheObserveLogConfig(1L, 2));
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                currentConfig::get,
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.append(log("task-a", "Task A", "two")));
            assertTrue(manager.append(log("task-a", "Task A", "three")));
            manager.deactivateTask("task-a");

            currentConfig.set(new CacheObserveLogConfig(1L, 0));
            assertTrue(manager.activateTask("task-a", "Task A"));

            try (Stream<Path> paths = Files.list(tempDir.resolve("task-a").resolve("segments"))) {
                assertEquals(1L, paths.filter(Files::isDirectory).count());
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

    @Test
    void shouldRejectImmediatelyAndAuditWhenIngressQueueIsFull() throws Exception {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        ExecutorService pausedWriter = Executors.newSingleThreadExecutor();
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        pausedWriter.execute(() -> {
            writerStarted.countDown();
            try {
                releaseWriter.await(5L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(writerStarted.await(5L, TimeUnit.SECONDS));
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> {
                },
                audit,
                false,
                pausedWriter,
                1)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "accepted")));

            long startNanos = System.nanoTime();
            assertFalse(manager.append(log("task-a", "Task A", "rejected")));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertTrue(elapsedMillis < 100L);
        } finally {
            releaseWriter.countDown();
        }

        verify(audit).writeRejected(
                eq("task-a"),
                eq("Task A"),
                eq("INFO"),
                eq(1L),
                eq(1),
                eq(1));
    }

    @Test
    void shouldNotBlockProducerWhenFileDispatcherIsBlocked() throws Exception {
        CountDownLatch dispatcherEntered = new CountDownLatch(1);
        CountDownLatch releaseDispatcher = new CountDownLatch(1);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> {
                    if (sink == CacheLogSink.FILE) {
                        dispatcherEntered.countDown();
                        try {
                            releaseDispatcher.await(5L, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                mock(CacheObserveLogAudit.class),
                true)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "first")));
            assertTrue(dispatcherEntered.await(5L, TimeUnit.SECONDS));

            long startNanos = System.nanoTime();
            assertTrue(manager.append(log("task-a", "Task A", "second")));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertTrue(elapsedMillis < 100L);
            assertEquals(1L, releaseDispatcher.getCount());
            releaseDispatcher.countDown();
        } finally {
            releaseDispatcher.countDown();
        }
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
