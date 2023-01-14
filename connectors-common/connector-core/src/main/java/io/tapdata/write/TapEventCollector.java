package io.tapdata.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

final class TapEventCollector {
    private static final String TAG = TapEventCollector.class.getSimpleName();

    private long touch;
    private int initDelay = 0;
    //private TapTable table;
    private Map<String, TapTable> collectedTable = new ConcurrentHashMap<>();
    private int idleSeconds = 5;
    private int maxRecords = 1000;
    private ScheduledFuture<?> future;
    private boolean isUploading = false;
    private EventCollector eventCollector;
    private final Object lock = new int[0];
    private List<TapRecordEvent> pendingUploadEvents;
    private final Object pendingLock = new Object();
    private Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private List<TapRecordEvent> events = new CopyOnWriteArrayList<>();
    private EventProcessor eventProcessor = (eventList, table) -> {
    };

    public static TapEventCollector create() {
        return new TapEventCollector();
    }

    public TapEventCollector recoverCovert(EventProcessor recordCovert) {
        this.eventProcessor = recordCovert;
        return this;
    }

    public TapEventCollector table(TapTable table) {
        //this.table = table;
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
            this.monitorIdle();
        }
    }

    public void stop() {
        if (Objects.nonNull(this.future)) {
            this.uploadEvents();
            this.future.cancel(true);
        }
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
            this.uploadEvents();
        } else if (this.pendingUploadEvents == null && !this.events.isEmpty() && (forced || System.currentTimeMillis() - this.touch > this.idleSeconds * 1000L)) {
//            synchronized (this.pendingLock) {
                this.pendingUploadEvents = this.events;
//            }
            this.events = new CopyOnWriteArrayList<>();
            //TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            this.uploadEvents();
        }
    }

    public void uploadEvents() {
        this.uploadEvents(null);
    }

    public void uploadEvents(List<String> tableIdList) {
        this.isUploading = true;
        try {
            if (this.pendingUploadEvents != null) {
                Map<String, List<TapRecordEvent>> eventGroup;
                int groupSize = 0;
                if (this.eventCollector != null) {
//                    synchronized (this.pendingLock) {
                        eventGroup = this.pendingUploadEvents.stream()
                                .filter(e -> Objects.nonNull(e) && (Objects.isNull(tableIdList) || tableIdList.contains(e.getTableId())))
                                .collect(Collectors.groupingBy(TapBaseEvent::getTableId));
                    for (Map.Entry<String, List<TapRecordEvent>> entry : eventGroup.entrySet()) {
                        List<TapRecordEvent> eventList = entry.getValue();
                        groupSize += Objects.isNull(eventList) || eventList.isEmpty() ? 0 : eventList.size();
                        this.eventCollector.collected(this.writeListResultConsumer, eventList, this.collectedTable.get(entry.getKey()));
                    }
//                        eventGroup.forEach((tab, events) -> this.eventCollector.collected(this.writeListResultConsumer, events, this.collectedTable.get(tab)));
//                    }
                }
//                synchronized (this.pendingLock) {
                if (Objects.isNull(tableIdList) || groupSize == this.pendingUploadEvents.size()) {
                    this.pendingUploadEvents = null;
                }else {
                    this.pendingUploadEvents = this.pendingUploadEvents.stream()
                            .filter(e -> Objects.nonNull(e) && !tableIdList.contains(e.getTableId())).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                }
//                }
            }
        } finally {
            touch = System.currentTimeMillis();
            this.isUploading = false;
        }
    }

    public void addTapEvents(List<TapRecordEvent> eventList, TapTable table) {
        if (Objects.isNull(table) || Objects.isNull(table.getId())) {
            throw new CoreException(" Unable to process event record, invalid TapTable or empty table name. ");
        }
        if (eventList != null && !eventList.isEmpty()) {
            this.eventProcessor.covert(eventList, table);
            this.events.addAll(eventList);
            Optional.ofNullable(eventList.get(0)).flatMap(event -> Optional.ofNullable(event.getTableId())).ifPresent(tableId -> this.collectedTable.put(tableId, table));
            //this.collectedTable.put(table.getId(), table);
        }
        tryUpload(this.events.size() > this.maxRecords);
    }
}
