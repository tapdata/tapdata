package io.tapdata.observable.alert;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JVM-shared bounded dispatcher. Data threads only {@code offer} and never wait on HTTP.
 */
public final class TaskAlertDispatcher {
    public static final String ENABLED_PROPERTY = "task.alert.publisher.enabled";
    public static final String CAPACITY_PROPERTY = "task.alert.dispatcher.capacity";
    public static final String WORKERS_PROPERTY = "task.alert.dispatcher.workers";
    public static final String MAX_RETRIES_PROPERTY = "task.alert.dispatcher.maxRetries";
    public static final String RETRY_INITIAL_MILLIS_PROPERTY = "task.alert.dispatcher.retryInitialMillis";
    public static final String RETRY_MAX_MILLIS_PROPERTY = "task.alert.dispatcher.retryMaxMillis";

    private static final int DEFAULT_CAPACITY = 4096;
    private static final int DEFAULT_WORKERS = 1;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_INITIAL_MILLIS = 500L;
    private static final long DEFAULT_RETRY_MAX_MILLIS = 5000L;
    private static final long COALESCE_WINDOW_MILLIS = 1000L;
    private static final int COALESCE_MAX_KEYS = 4096;
    private static final long REJECT_LOG_INTERVAL = 1000L;

    private static volatile TaskAlertDispatcher INSTANCE;

    private final LinkedBlockingQueue<TaskAlertEvent> queue;
    private final ThreadPoolExecutor workers;
    private final TaskAlertPublisher publisher;
    private final int maxRetries;
    private final long retryInitialMillis;
    private final long retryMaxMillis;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong rejectedCount = new AtomicLong();
    private final Map<String, CoalescedAlert> coalescedAlerts = new ConcurrentHashMap<>();

    private TaskAlertDispatcher() {
        this(
                createPublisher(),
                CommonUtils.getPropertyInt(CAPACITY_PROPERTY, DEFAULT_CAPACITY),
                CommonUtils.getPropertyInt(WORKERS_PROPERTY, DEFAULT_WORKERS),
                CommonUtils.getPropertyInt(MAX_RETRIES_PROPERTY, DEFAULT_MAX_RETRIES),
                CommonUtils.getPropertyLong(RETRY_INITIAL_MILLIS_PROPERTY, DEFAULT_RETRY_INITIAL_MILLIS),
                CommonUtils.getPropertyLong(RETRY_MAX_MILLIS_PROPERTY, DEFAULT_RETRY_MAX_MILLIS)
        );
    }

    TaskAlertDispatcher(TaskAlertPublisher publisher,
                        int capacity,
                        int workers,
                        int maxRetries,
                        long retryInitialMillis,
                        long retryMaxMillis) {
        this.publisher = publisher;
        int boundedCapacity = Math.max(1, capacity);
        int workerCount = Math.max(1, Math.min(4, workers));
        this.maxRetries = Math.max(0, Math.min(10, maxRetries));
        this.retryInitialMillis = Math.max(1L, retryInitialMillis);
        this.retryMaxMillis = Math.max(this.retryInitialMillis, retryMaxMillis);
        this.queue = new LinkedBlockingQueue<>(boundedCapacity);
        this.workers = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                dispatcherThreadFactory()
        );
        for (int index = 0; index < workerCount; index++) {
            this.workers.submit(this::drainLoop);
        }
    }

    public static TaskAlertDispatcher getInstance() {
        if (INSTANCE == null) {
            synchronized (TaskAlertDispatcher.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TaskAlertDispatcher();
                }
            }
        }
        return INSTANCE;
    }

    public boolean submit(TaskAlertEvent event) {
        if (event == null || !running.get() || !isEnabled()) {
            return false;
        }
        TaskAlertEvent coalesced = coalesce(event);
        if (coalesced == null) {
            TaskAlertMetrics.coalesced(typeName(event), event.getCode());
            return true;
        }
        boolean accepted = queue.offer(coalesced);
        TaskAlertMetrics.setQueueSize(queue.size());
        if (accepted) {
            TaskAlertMetrics.submitted(typeName(coalesced), coalesced.getCode());
            return true;
        }
        long rejected = rejectedCount.incrementAndGet();
        TaskAlertMetrics.publishRejected(typeName(coalesced), coalesced.getCode());
        if (rejected == 1L || rejected % REJECT_LOG_INTERVAL == 0L) {
            TaskAlertAudit.queueRejected(typeName(coalesced), coalesced.getCode(), rejected);
        }
        return false;
    }

    public void shutdown() {
        running.set(false);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                int remaining = queue.size();
                workers.shutdownNow();
                TaskAlertAudit.publishFailed(null, null, "shutdown remaining=" + remaining, null);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    int queueSize() {
        return queue.size();
    }

    private void drainLoop() {
        while (running.get() || !queue.isEmpty()) {
            try {
                TaskAlertEvent event = queue.poll(200, TimeUnit.MILLISECONDS);
                TaskAlertMetrics.setQueueSize(queue.size());
                if (event == null) {
                    continue;
                }
                publishWithRetry(event);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException runtimeException) {
                TaskAlertAudit.publishFailed(null, null, "drain", runtimeException);
            }
        }
    }

    private void publishWithRetry(TaskAlertEvent event) {
        String type = typeName(event);
        int attempt = 0;
        while (true) {
            TaskAlertPublisher.PublishResult result;
            try {
                result = publisher.publish(event);
            } catch (RuntimeException runtimeException) {
                result = TaskAlertPublisher.PublishResult.RETRYABLE;
                TaskAlertAudit.publishRetry(type, event.getCode(), runtimeException.getClass().getSimpleName(), attempt);
            }
            if (result == TaskAlertPublisher.PublishResult.SUCCESS) {
                TaskAlertMetrics.publishSuccess(type, event.getCode());
                return;
            }
            if (result == TaskAlertPublisher.PublishResult.NON_RETRYABLE || attempt >= maxRetries) {
                TaskAlertMetrics.publishFailed(type, event.getCode(), result.name().toLowerCase());
                TaskAlertAudit.publishFailed(type, event.getCode(), result.name(), null);
                return;
            }
            attempt++;
            TaskAlertMetrics.publishRetry(type, event.getCode(), "retryable");
            TaskAlertAudit.publishRetry(type, event.getCode(), "retryable", attempt);
            sleepQuietly(retryDelayMillis(attempt));
        }
    }

    private long retryDelayMillis(int attempt) {
        long exponential = retryInitialMillis * (1L << Math.min(attempt, 8));
        long bounded = Math.min(retryMaxMillis, exponential);
        long jitter = (long) (Math.random() * bounded * 0.2d);
        return bounded + jitter;
    }

    private TaskAlertEvent coalesce(TaskAlertEvent event) {
        long now = System.currentTimeMillis();
        evictExpiredCoalesceKeys(now);
        if (coalescedAlerts.size() >= COALESCE_MAX_KEYS) {
            return event;
        }
        CoalescedAlert existing = coalescedAlerts.get(event.coalesceKey());
        if (existing != null && now - existing.windowStartMillis < COALESCE_WINDOW_MILLIS) {
            existing.occurrenceCount.incrementAndGet();
            return null;
        }
        coalescedAlerts.put(event.coalesceKey(), new CoalescedAlert(now));
        return event;
    }

    private void evictExpiredCoalesceKeys(long now) {
        Iterator<Map.Entry<String, CoalescedAlert>> iterator = coalescedAlerts.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CoalescedAlert> entry = iterator.next();
            if (now - entry.getValue().windowStartMillis >= COALESCE_WINDOW_MILLIS) {
                iterator.remove();
            }
        }
    }

    private static boolean isEnabled() {
        return CommonUtils.getPropertyBool(ENABLED_PROPERTY, true);
    }

    private static TaskAlertPublisher createPublisher() {
        return new HttpTaskAlertPublisher();
    }

    private static ThreadFactory dispatcherThreadFactory() {
        AtomicLong sequence = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("task-alert-dispatcher-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static String typeName(TaskAlertEvent event) {
        return event.getType() == null ? null : event.getType().name();
    }

    private static final class CoalescedAlert {
        private final long windowStartMillis;
        private final AtomicLong occurrenceCount = new AtomicLong(1L);

        private CoalescedAlert(long windowStartMillis) {
            this.windowStartMillis = windowStartMillis;
        }
    }
}
