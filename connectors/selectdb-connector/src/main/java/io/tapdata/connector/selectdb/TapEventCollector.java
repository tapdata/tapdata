package io.tapdata.connector.selectdb;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2022/12/17
 **/
public class TapEventCollector {
    private static final String TAG = TapEventCollector.class.getSimpleName();
    private List<TapRecordEvent> events = new CopyOnWriteArrayList<>();
    private List<TapRecordEvent> pendingUploadEvents;

    private boolean isUploading = false;

    private final Object lock = new int[0];
    private ScheduledFuture<?> future;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private Long touch;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            monitorIdle();
        }
    }

    public void stop() {
        if (future != null)
            future.cancel(true);
    }

    public interface EventCollected {
        void collected(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table);
    }

    public static TapEventCollector create() {
        return new TapEventCollector();
    }

    private TapTable table;

    public TapEventCollector table(TapTable table) {
        this.table = table;
        return this;
    }

    private int idleSeconds = 5;

    public TapEventCollector idleSeconds(int idleSeconds) {
        this.idleSeconds = idleSeconds;
        return this;
    }

    private int maxRecords = 1000;

    public TapEventCollector maxRecords(int maxRecords) {
        this.maxRecords = maxRecords;

        return this;
    }

    private EventCollected eventCollected;

    public TapEventCollector eventCollected(EventCollected eventCollected) {
        this.eventCollected = eventCollected;
        return this;
    }

    private Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;

    public TapEventCollector writeListResultConsumer(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        this.writeListResultConsumer = writeListResultConsumer;
        return this;
    }

    public void monitorIdle() {
        synchronized (lock) {
            if (future == null) {
                future = scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    try {
                        tryUpload(true);
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, "tryUpload failed in scheduler, {}", throwable.getMessage());
                    }
                }, 1, 10, TimeUnit.SECONDS);
            }
        }
    }

    private synchronized void tryUpload(boolean forced) {
        if (!isUploading && pendingUploadEvents != null) {
            TapLogger.info(TAG, "tryUpload forced {} delay {} pendingUploadEvents {}", forced, touch != null ? System.currentTimeMillis() - touch : 0, pendingUploadEvents.size());
            uploadEvents();
        } else if ((pendingUploadEvents == null && !events.isEmpty()) && (forced || (touch != null && System.currentTimeMillis() - touch > idleSeconds * 1000L))) {
            pendingUploadEvents = events;
            events = new CopyOnWriteArrayList<>();
            TapLogger.info(TAG, "tryUpload forced {} delay {} pendingUploadEvents {}", forced, touch != null ? System.currentTimeMillis() - touch : 0, pendingUploadEvents.size());
            uploadEvents();
        }
    }

    private void uploadEvents() {
        isUploading = true;
        try {
            if (pendingUploadEvents != null) {
                if (eventCollected != null)
                    eventCollected.collected(writeListResultConsumer, pendingUploadEvents, table);
                pendingUploadEvents = null;
            }
        } finally {
            isUploading = false;
        }
    }

    public void addTapEvents(List<TapRecordEvent> eventList) {
        if (eventList != null && !eventList.isEmpty()) {
            events.addAll(eventList);
        }
        touch = System.currentTimeMillis();
        tryUpload(events.size() > maxRecords);
    }
}
