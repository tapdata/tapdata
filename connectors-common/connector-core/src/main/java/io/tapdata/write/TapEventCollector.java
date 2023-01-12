package io.tapdata.write;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class TapEventCollector {
    private static final String TAG = TapEventCollector.class.getSimpleName();

    private long touch;
    private int initDelay = 0;
    private TapTable table;
    private int idleSeconds = 5;
    private int maxRecords = 1000;
    private ScheduledFuture<?> future;
    private boolean isUploading = false;
    private EventCollector eventCollector;
    private final Object lock = new int[0];
    private List<TapRecordEvent> pendingUploadEvents;
    private Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private List<TapRecordEvent> events = new CopyOnWriteArrayList<>();
    private RecordProcessor recordProcessor = (eventList, table) -> {
    };

    public static TapEventCollector create() {
        return new TapEventCollector();
    }

    public TapEventCollector recoverCovert(RecordProcessor recordCovert) {
        this.recordProcessor = recordCovert;
        return this;
    }

    public TapEventCollector table(TapTable table) {
        this.table = table;
        return this;
    }

    public TapEventCollector idleSeconds(int idleSeconds) {
        this.idleSeconds = idleSeconds;
        return this;
    }

    public TapEventCollector maxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public TapEventCollector initDelay(int initDelay) {
        this.initDelay = initDelay;
        return this;
    }

    public TapEventCollector eventCollected(EventCollector eventCollector) {
        this.eventCollector = eventCollector;
        return this;
    }

    public TapEventCollector writeListResultConsumer(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        this.writeListResultConsumer = writeListResultConsumer;
        return this;
    }

    public void start() {
        if (this.isStarted.compareAndSet(false, true)) {
            monitorIdle();
        }
    }

    public void stop() {
        if (Objects.nonNull(this.future))
            this.future.cancel(true);
    }

    public void monitorIdle() {
        synchronized (this.lock) {
            if (Objects.nonNull(future)) {
                this.future = this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    try {
                        this.tryUpload(true);
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, "Try upload failed in scheduler, {}", throwable.getMessage());
                    }
                }, this.initDelay, this.idleSeconds, TimeUnit.SECONDS);
            }
        }
    }

    private synchronized void tryUpload(boolean forced) {
        if (!this.isUploading && this.pendingUploadEvents != null) {
            //TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            uploadEvents();
        } else if (this.pendingUploadEvents == null && !this.events.isEmpty() && (forced || System.currentTimeMillis() - this.touch > this.idleSeconds * 1000L)) {
            this.pendingUploadEvents = this.events;
            this.events = new CopyOnWriteArrayList<>();
            //TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            uploadEvents();
        }
    }

    private void uploadEvents() {
        this.isUploading = true;
        try {
            if (this.pendingUploadEvents != null) {
                if (this.eventCollector != null)
                    this.eventCollector.collected(this.writeListResultConsumer, this.pendingUploadEvents, this.table);
                this.pendingUploadEvents = null;
            }
        } finally {
            touch = System.currentTimeMillis();
            this.isUploading = false;
        }
    }

    public void addTapEvents(List<TapRecordEvent> eventList, TapTable table) {
        if (eventList != null && !eventList.isEmpty()) {
            this.recordProcessor.covert(eventList, table);
            this.events.addAll(eventList);
        }
        tryUpload(this.events.size() > this.maxRecords);
    }
}
