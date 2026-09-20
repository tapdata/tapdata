package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    void shouldCoalesceBurstWakeSignalsPerSink() {
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            for (int index = 0; index < 100; index++) {
                assertTrue(manager.append(log("task-a", "Task A", "message-" + index)));
            }

            assertEquals(1, manager.availablePermits(CacheLogSink.FILE));
            assertEquals(1, manager.availablePermits(CacheLogSink.TM));
        }
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
    void shouldNotReportDataLossAfterBothSinksConsumeGeneration() throws IOException {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 5),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertEquals(2L, generationCount(segments));

            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertEquals(2L, generationCount(segments));

            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertEquals(1L, generationCount(segments));
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
    void shouldNeverReclaimActiveGeneration() throws IOException {
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 5),
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertEquals(1L, generationCount(segments));
        }

        verify(audit, never()).evict(
                anyString(),
                anyString(),
                anyLong(),
                eq(false),
                anyString(),
                anyLong(),
                anyLong());
    }

    @Test
    void shouldReclaimConsumedGenerationWhenTaskIsReopened() throws IOException {
        CacheObserveLogConfig config = new CacheObserveLogConfig(1L, 5);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        Path sealedGeneration;
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));
            sealedGeneration = generationPaths(segments).get(0);
        }
        consumeGeneration(sealedGeneration);

        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertEquals(1L, generationCount(segments));
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
    void shouldNotReclaimWhileAnotherSinkDispatchIsInFlight() throws Exception {
        CountDownLatch fileDispatcherEntered = new CountDownLatch(1);
        CountDownLatch releaseFileDispatcher = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Path segments = tempDir.resolve("task-a").resolve("segments");
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 5),
                (log, sink) -> {
                    if (sink == CacheLogSink.FILE) {
                        fileDispatcherEntered.countDown();
                        try {
                            releaseFileDispatcher.await(5L, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));

            Future<Boolean> filePoll = executor.submit(() -> manager.pollOnce(CacheLogSink.FILE));
            assertTrue(fileDispatcherEntered.await(5L, TimeUnit.SECONDS));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertEquals(2L, generationCount(segments));

            releaseFileDispatcher.countDown();
            assertTrue(filePoll.get(5L, TimeUnit.SECONDS));
            assertEquals(1L, generationCount(segments));
        } finally {
            releaseFileDispatcher.countDown();
            executor.shutdownNow();
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
    void shouldReclaimFailedDispatchGenerationAfterBothSinksAdvance() throws IOException {
        Path segments = tempDir.resolve("task-a").resolve("segments");
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1L, 1),
                (log, sink) -> {
                    if (sink == CacheLogSink.FILE) {
                        throw new IllegalStateException("file dispatch failed");
                    }
                },
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "one")));

            assertFalse(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertEquals(1L, generationCount(segments));
        }

        verify(audit).evict(
                eq("task-a"),
                eq("Task A"),
                anyLong(),
                eq(true),
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
    void shouldContinueLastGenerationAfterManagerRestart() throws IOException {
        CacheObserveLogConfig config = new CacheObserveLogConfig(1024L * 1024L, 2);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        Path activeGeneration;
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "before restart")));
            activeGeneration = onlyGeneration(segments);
            assertTrue(Files.isRegularFile(activeGeneration.resolve(CacheGeneration.PAYLOAD_BYTES_FILE)));
        }

        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "after restart")));
            assertEquals(activeGeneration, onlyGeneration(segments));
        }
    }

    @Test
    void shouldRecoverLegacyPayloadBytesBeforeContinuingLastGeneration() throws IOException {
        CacheObserveLogConfig config = new CacheObserveLogConfig(1024L * 1024L, 2);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        Path activeGeneration = segments.resolve("00000000000000000000");
        Files.createDirectories(activeGeneration);
        MonitoringLogCodec codec = new MonitoringLogCodec();
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(activeGeneration).build();
             ExcerptAppender appender = queue.acquireAppender()) {
            appender.writeDocument(wire ->
                    codec.write(wire.getValueOut(), log("task-a", "Task A", "legacy record")));
        }

        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "after migration")));
            assertEquals(activeGeneration, onlyGeneration(segments));
            assertEquals(
                    Long.BYTES,
                    Files.size(activeGeneration.resolve(CacheGeneration.PAYLOAD_BYTES_FILE)));
        }
    }

    @Test
    void shouldDrainAcceptedLogsOnStopBeforeRemovingContext() throws IOException {
        List<MonitoringLogsDto> dispatched = new ArrayList<>();
        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                new CacheObserveLogConfig(1024L * 1024L, 2),
                (log, sink) -> dispatched.add(log),
                audit,
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "before stop")));

            manager.deactivateTask("task-a");
            assertTrue(Files.exists(segments));
            assertFalse(manager.append(log("task-a", "Task A", "late")));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertTrue(manager.pollOnce(CacheLogSink.TM));
            assertFalse(Files.exists(segments));
            assertEquals(0, manager.contextCount());
            assertEquals(2, dispatched.size());
            assertEquals("before stop", dispatched.get(0).getMessage());
            assertEquals("before stop", dispatched.get(1).getMessage());

            assertTrue(manager.activateTask("task-a", "Task A"));
            assertFalse(manager.pollOnce(CacheLogSink.FILE));
            assertFalse(manager.pollOnce(CacheLogSink.TM));
            assertTrue(manager.append(log("task-a", "Task A", "after resume")));
            assertTrue(manager.pollOnce(CacheLogSink.FILE));
            assertEquals(3, dispatched.size());
            assertEquals("after resume", dispatched.get(2).getMessage());

            manager.deleteTaskCache("task-a");
            assertFalse(Files.exists(tempDir.resolve("task-a")));
            assertFalse(manager.append(log("task-a", "Task A", "after delete")));
            assertFalse(manager.activateTask("task-a", "Task A"));
            manager.deleteTaskCache("task-a");
        }

        verify(audit, never()).stopDiscard(
                anyString(),
                anyString(),
                anyInt(),
                anyLong(),
                anyLong());
    }

    @Test
    void shouldDiscardStoppedTaskInventoryBeforeItIsReactivatedAfterManagerRestart() {
        CacheObserveLogConfig config = new CacheObserveLogConfig(1024L * 1024L, 2);
        Path segments = tempDir.resolve("task-a").resolve("segments");
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                mock(CacheObserveLogAudit.class),
                false)) {
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertTrue(manager.append(log("task-a", "Task A", "before restart")));
        }
        assertTrue(Files.isDirectory(segments));

        CacheObserveLogAudit audit = mock(CacheObserveLogAudit.class);
        try (TaskCacheManager manager = new TaskCacheManager(
                tempDir,
                config,
                (log, sink) -> {
                },
                audit,
                false)) {
            manager.deactivateTask("task-a");
            assertFalse(Files.exists(segments));
            assertTrue(manager.activateTask("task-a", "Task A"));
            assertFalse(manager.pollOnce(CacheLogSink.FILE));
            assertFalse(manager.pollOnce(CacheLogSink.TM));
        }

        verify(audit).stopDiscard(
                eq("task-a"),
                eq(null),
                eq(0),
                eq(0L),
                anyLong());
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

    private Path onlyGeneration(Path segments) throws IOException {
        List<Path> generations = generationPaths(segments);
        assertEquals(1, generations.size());
        return generations.get(0);
    }

    private long generationCount(Path segments) throws IOException {
        return generationPaths(segments).size();
    }

    private List<Path> generationPaths(Path segments) throws IOException {
        try (Stream<Path> paths = Files.list(segments)) {
            return paths.filter(Files::isDirectory)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(java.util.stream.Collectors.toList());
        }
    }

    private void consumeGeneration(Path generation) {
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(generation).build();
             ExcerptTailer fileTailer = queue.createTailer(CacheGeneration.FILE_TAILER_ID);
             ExcerptTailer tmTailer = queue.createTailer(CacheGeneration.TM_TAILER_ID)) {
            consume(fileTailer);
            consume(tmTailer);
        }
    }

    private void consume(ExcerptTailer tailer) {
        while (tailer.readDocument(wire -> {
            // Advancing the named Tailer persists its next-read position.
        })) {
            // Consume the complete generation.
        }
    }
}
