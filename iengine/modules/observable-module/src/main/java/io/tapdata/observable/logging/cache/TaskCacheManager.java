package io.tapdata.observable.logging.cache;

import com.tapdata.constant.BeanUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.common.SettingService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class TaskCacheManager implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(TaskCacheManager.class);
    private static final Pattern SAFE_TASK_ID = Pattern.compile("[A-Za-z0-9._-]{1,256}");
    private static final String CACHE_DIRECTORY = "CacheObserveLogs";
    private static final int DEFAULT_INGRESS_CAPACITY = 10000;
    private static final int WRITE_DRAIN_BATCH_SIZE = 256;
    // Covers the one-hour delayed TaskLogger cleanup without retaining deleted task IDs forever.
    private static final long DELETED_TASK_RETENTION_MILLIS = TimeUnit.HOURS.toMillis(2L);

    private final Path rootPath;
    private final Supplier<CacheObserveLogConfig> configSupplier;
    private final CacheLogDispatcher dispatcher;
    private final CacheObserveLogAudit audit;
    private final MonitoringLogCodec codec = new MonitoringLogCodec();
    private final Map<String, TaskCacheContext> contexts = new ConcurrentHashMap<>();
    private final Map<String, Long> deletedTasks = new ConcurrentHashMap<>();
    private final Semaphore fileAvailable = new Semaphore(0);
    private final Semaphore tmAvailable = new Semaphore(0);
    private final Object availableSignalLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger filePollCursor = new AtomicInteger();
    private final AtomicInteger tmPollCursor = new AtomicInteger();
    private final ExecutorService pollers;
    private final ExecutorService cacheWriters;
    private final int ingressCapacity;
    private final AtomicBoolean closed = new AtomicBoolean();

    TaskCacheManager(Path rootPath,
                     CacheObserveLogConfig config,
                     CacheLogDispatcher dispatcher,
                     CacheObserveLogAudit audit,
                     boolean startPolling) {
        this(rootPath, () -> config, dispatcher, audit, startPolling);
    }

    TaskCacheManager(Path rootPath,
                     Supplier<CacheObserveLogConfig> configSupplier,
                     CacheLogDispatcher dispatcher,
                     CacheObserveLogAudit audit,
                     boolean startPolling) {
        this(
                rootPath,
                configSupplier,
                dispatcher,
                audit,
                startPolling,
                startPolling ? Executors.newFixedThreadPool(2, writerThreadFactory()) : null,
                DEFAULT_INGRESS_CAPACITY);
    }

    TaskCacheManager(Path rootPath,
                     CacheObserveLogConfig config,
                     CacheLogDispatcher dispatcher,
                     CacheObserveLogAudit audit,
                     boolean startPolling,
                     ExecutorService cacheWriters,
                     int ingressCapacity) {
        this(
                rootPath,
                () -> config,
                dispatcher,
                audit,
                startPolling,
                cacheWriters,
                ingressCapacity);
    }

    private TaskCacheManager(Path rootPath,
                             Supplier<CacheObserveLogConfig> configSupplier,
                             CacheLogDispatcher dispatcher,
                             CacheObserveLogAudit audit,
                             boolean startPolling,
                             ExecutorService cacheWriters,
                             int ingressCapacity) {
        this.rootPath = rootPath.toAbsolutePath().normalize();
        this.configSupplier = configSupplier;
        this.dispatcher = dispatcher;
        this.audit = audit;
        this.cacheWriters = cacheWriters;
        this.ingressCapacity = ingressCapacity;
        this.pollers = startPolling
                ? Executors.newFixedThreadPool(2, pollerThreadFactory())
                : null;
        if (startPolling) {
            running.set(true);
            pollers.submit(() -> poll(CacheLogSink.FILE));
            pollers.submit(() -> poll(CacheLogSink.TM));
        }
    }

    public static TaskCacheManager createDefault(CacheLogDispatcher dispatcher) {
        return new TaskCacheManager(
                defaultRootPath(),
                () -> CacheObserveLogConfig.from(BeanUtil.getBean(SettingService.class)),
                dispatcher,
                new CacheObserveLogAudit(),
                true);
    }

    public boolean activateTask(String taskId, String taskName) {
        Path taskPath = resolveTaskPath(taskId);
        if (taskPath == null || isDeletedTask(taskId)) {
            return false;
        }
        CacheObserveLogConfig config = configSupplier.get();
        AtomicBoolean activated = new AtomicBoolean();
        contexts.compute(taskId, (id, current) -> {
            boolean created = current == null;
            TaskCacheContext context = created
                    ? new TaskCacheContext(
                            id,
                            taskPath,
                            codec,
                            audit,
                            this::totalCacheBytes,
                            dispatcher,
                            ingressCapacity)
                    : current;
            try {
                activated.set(context.activate(taskName, config));
            } catch (RuntimeException e) {
                if (created) {
                    context.closeRetainingCache();
                }
                throw e;
            }
            return context;
        });
        return activated.get();
    }

    public boolean append(MonitoringLogsDto log) {
        if (closed.get()
                || log == null
                || StringUtils.isBlank(log.getTaskId())
                || isDeletedTask(log.getTaskId())) {
            return false;
        }
        TaskCacheContext context = contexts.get(log.getTaskId());
        if (context == null) {
            return false;
        }
        try {
            if (cacheWriters == null) {
                boolean appended = context.append(log);
                if (appended) {
                    signalAvailable();
                }
                return appended;
            }
            boolean accepted = context.enqueue(log);
            if (accepted) {
                scheduleWrite(context);
            }
            return accepted;
        } catch (RuntimeException e) {
            LOGGER.warn("Append log to task CacheObserveLogs failed, taskId={}", log.getTaskId(), e);
            return false;
        }
    }

    public void deactivateTask(String taskId) {
        Path taskPath = resolveTaskPath(taskId);
        if (taskPath == null) {
            return;
        }
        TaskCacheContext context = contexts.computeIfAbsent(taskId, id -> new TaskCacheContext(
                id,
                taskPath,
                codec,
                audit,
                this::totalCacheBytes,
                dispatcher,
                ingressCapacity));
        context.stop();
        removeStoppedContext(context);
    }

    public void deleteTaskCache(String taskId) {
        Path taskPath = resolveTaskPath(taskId);
        if (taskPath == null) {
            throw new IllegalArgumentException("Invalid task id: " + taskId);
        }
        purgeExpiredDeletedTasks();
        deletedTasks.put(taskId, System.currentTimeMillis());
        TaskCacheContext context = contexts.computeIfAbsent(taskId, id -> new TaskCacheContext(
                id,
                taskPath,
                codec,
                audit,
                this::totalCacheBytes,
                dispatcher,
                ingressCapacity));
        try {
            context.delete();
        } finally {
            contexts.remove(taskId, context);
        }
    }

    boolean pollOnce(CacheLogSink sink) {
        List<TaskCacheContext> snapshot = new ArrayList<>(contexts.values());
        if (snapshot.isEmpty()) {
            return false;
        }
        AtomicInteger cursor = sink == CacheLogSink.FILE ? filePollCursor : tmPollCursor;
        int start = Math.floorMod(cursor.getAndIncrement(), snapshot.size());
        for (int offset = 0; offset < snapshot.size(); offset++) {
            TaskCacheContext context = snapshot.get((start + offset) % snapshot.size());
            try {
                boolean dispatched = context.poll(sink);
                removeStoppedContext(context);
                if (dispatched) {
                    return true;
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Dispatch cached task log failed, sink={}", sink, e);
                removeStoppedContext(context);
            }
        }
        return false;
    }

    int contextCount() {
        return contexts.size();
    }

    int availablePermits(CacheLogSink sink) {
        return (sink == CacheLogSink.FILE ? fileAvailable : tmAvailable).availablePermits();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (pollers != null && running.compareAndSet(true, false)) {
            fileAvailable.release();
            tmAvailable.release();
            pollers.shutdownNow();
            try {
                if (!pollers.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("CacheObserveLogs pollers did not stop within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (TaskCacheContext context : contexts.values()) {
            context.closeRetainingCache();
        }
        if (cacheWriters != null) {
            cacheWriters.shutdownNow();
            try {
                if (!cacheWriters.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("CacheObserveLogs writers did not stop within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void scheduleWrite(TaskCacheContext context) {
        if (closed.get() || !context.markWriteScheduled()) {
            return;
        }
        try {
            cacheWriters.execute(() -> drainWrites(context));
        } catch (RejectedExecutionException e) {
            context.clearWriteScheduled();
            if (!closed.get()) {
                LOGGER.warn("Schedule CacheObserveLogs writer failed", e);
            }
        }
    }

    private void drainWrites(TaskCacheContext context) {
        try {
            int drained = 0;
            TaskCacheContext.PendingWrite pendingWrite;
            while (drained < WRITE_DRAIN_BATCH_SIZE
                    && (pendingWrite = context.pollPendingWrite()) != null) {
                try {
                    if (context.append(pendingWrite)) {
                        signalAvailable();
                    }
                } catch (RuntimeException e) {
                    LOGGER.warn(
                            "Append log to task CacheObserveLogs failed, taskId={}",
                            pendingWrite.getLog().getTaskId(),
                            e);
                }
                drained++;
            }
        } finally {
            context.clearWriteScheduled();
            if (!closed.get() && context.hasPendingWrites()) {
                scheduleWrite(context);
            } else {
                context.completeWriteDrain();
                removeStoppedContext(context);
            }
        }
    }

    private void poll(CacheLogSink sink) {
        Semaphore available = sink == CacheLogSink.FILE ? fileAvailable : tmAvailable;
        while (running.get()) {
            if (pollOnce(sink)) {
                available.tryAcquire();
                continue;
            }
            try {
                if (!available.tryAcquire(100, TimeUnit.MILLISECONDS) && !running.get()) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private Path resolveTaskPath(String taskId) {
        if (StringUtils.isBlank(taskId) || !SAFE_TASK_ID.matcher(taskId).matches()) {
            LOGGER.warn("Reject invalid CacheObserveLogs task id: {}", taskId);
            return null;
        }
        Path taskPath = rootPath.resolve(taskId).normalize();
        if (!taskPath.startsWith(rootPath) || taskPath.equals(rootPath)) {
            LOGGER.warn("Reject CacheObserveLogs path outside root, taskId={}", taskId);
            return null;
        }
        return taskPath;
    }

    private long totalCacheBytes() {
        return contexts.values().stream()
                .mapToLong(TaskCacheContext::sizeBytes)
                .sum();
    }

    private void signalAvailable() {
        synchronized (availableSignalLock) {
            if (fileAvailable.availablePermits() == 0) {
                fileAvailable.release();
            }
            if (tmAvailable.availablePermits() == 0) {
                tmAvailable.release();
            }
        }
    }

    private void removeStoppedContext(TaskCacheContext context) {
        contexts.computeIfPresent(context.getTaskId(), (taskId, current) ->
                current == context && current.isStopped() ? null : current);
    }

    private boolean isDeletedTask(String taskId) {
        Long deletedAt = deletedTasks.get(taskId);
        if (deletedAt == null) {
            return false;
        }
        if (System.currentTimeMillis() - deletedAt < DELETED_TASK_RETENTION_MILLIS) {
            return true;
        }
        deletedTasks.remove(taskId, deletedAt);
        return false;
    }

    private void purgeExpiredDeletedTasks() {
        long oldestRetained = System.currentTimeMillis() - DELETED_TASK_RETENTION_MILLIS;
        deletedTasks.entrySet().removeIf(entry -> entry.getValue() < oldestRetained);
    }

    private static Path defaultRootPath() {
        String workDir = System.getenv("TAPDATA_WORK_DIR");
        if (StringUtils.isBlank(workDir)) {
            return Paths.get("." + File.separator + CACHE_DIRECTORY);
        }
        return Paths.get(workDir, CACHE_DIRECTORY);
    }

    private static ThreadFactory pollerThreadFactory() {
        return new ThreadFactory() {
            private int index;

            @Override
            public synchronized Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "CacheObserveLogs-Poller-" + index++);
                thread.setDaemon(true);
                return thread;
            }
        };
    }

    private static ThreadFactory writerThreadFactory() {
        return new ThreadFactory() {
            private int index;

            @Override
            public synchronized Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "CacheObserveLogs-Writer-" + index++);
                thread.setDaemon(true);
                return thread;
            }
        };
    }
}
