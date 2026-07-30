package io.tapdata.observable.logging.cache;

import com.tapdata.constant.BeanUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.common.SettingService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class TaskCacheManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCacheManager.class);
    private static final Pattern SAFE_TASK_ID = Pattern.compile("[A-Za-z0-9._-]{1,256}");
    private static final String CACHE_DIRECTORY = "CacheObserveLogs";

    private final Path rootPath;
    private final CacheObserveLogConfig config;
    private final CacheLogDispatcher dispatcher;
    private final CacheObserveLogAudit audit;
    private final MonitoringLogCodec codec = new MonitoringLogCodec();
    private final Map<String, TaskCacheContext> contexts = new ConcurrentHashMap<>();
    private final Set<String> deletedTasks = ConcurrentHashMap.newKeySet();
    private final Semaphore available = new Semaphore(0);
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger filePollCursor = new AtomicInteger();
    private final AtomicInteger tmPollCursor = new AtomicInteger();
    private final ExecutorService pollers;

    TaskCacheManager(Path rootPath,
                     CacheObserveLogConfig config,
                     CacheLogDispatcher dispatcher,
                     CacheObserveLogAudit audit,
                     boolean startPolling) {
        this.rootPath = rootPath.toAbsolutePath().normalize();
        this.config = config;
        this.dispatcher = dispatcher;
        this.audit = audit;
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
        SettingService settingService = BeanUtil.getBean(SettingService.class);
        return new TaskCacheManager(
                defaultRootPath(),
                CacheObserveLogConfig.from(settingService),
                dispatcher,
                new CacheObserveLogAudit(),
                true);
    }

    public boolean activateTask(String taskId, String taskName) {
        Path taskPath = resolveTaskPath(taskId);
        if (taskPath == null || deletedTasks.contains(taskId)) {
            return false;
        }
        TaskCacheContext context = contexts.computeIfAbsent(taskId, id -> new TaskCacheContext(
                id,
                taskName,
                taskPath,
                config,
                codec,
                audit,
                this::totalCacheBytes,
                dispatcher));
        return context.activate(taskName);
    }

    public boolean append(MonitoringLogsDto log) {
        if (log == null || StringUtils.isBlank(log.getTaskId()) || deletedTasks.contains(log.getTaskId())) {
            return false;
        }
        TaskCacheContext context = contexts.get(log.getTaskId());
        if (context == null) {
            return false;
        }
        try {
            boolean appended = context.append(log);
            if (appended) {
                available.release();
            }
            return appended;
        } catch (RuntimeException e) {
            LOGGER.warn("Append log to task CacheObserveLogs failed, taskId={}", log.getTaskId(), e);
            return false;
        }
    }

    public void deactivateTask(String taskId) {
        TaskCacheContext context = contexts.get(taskId);
        if (context != null) {
            context.stop();
        }
    }

    public void deleteTaskCache(String taskId) {
        Path taskPath = resolveTaskPath(taskId);
        if (taskPath == null) {
            throw new IllegalArgumentException("Invalid task id: " + taskId);
        }
        deletedTasks.add(taskId);
        TaskCacheContext context = contexts.computeIfAbsent(taskId, id -> new TaskCacheContext(
                id,
                null,
                taskPath,
                config,
                codec,
                audit,
                this::totalCacheBytes,
                dispatcher));
        context.delete();
        contexts.remove(taskId, context);
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
                if (context.poll(sink)) {
                    return true;
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Dispatch cached task log failed, sink={}", sink, e);
            }
        }
        return false;
    }

    @Override
    public void close() {
        if (pollers != null && running.compareAndSet(true, false)) {
            available.release(2);
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
            context.stop();
        }
    }

    private void poll(CacheLogSink sink) {
        while (running.get()) {
            if (pollOnce(sink)) {
                continue;
            }
            try {
                available.tryAcquire(100, TimeUnit.MILLISECONDS);
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
        try {
            return CacheGeneration.directorySize(rootPath);
        } catch (IOException e) {
            LOGGER.warn("Read CacheObserveLogs total size failed, root={}", rootPath, e);
            return -1L;
        }
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
}
