package io.tapdata.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.toJson;

final class TapEventCollector {
    private static final String TAG = TapEventCollector.class.getSimpleName();

    private long touch;
    private int initDelay = 0;
    private boolean autoCheckTable = false;
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
    private Throwable throwable;
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

    public TapEventCollector autoCheckTable(boolean autoCheckTable) {
        this.autoCheckTable = autoCheckTable;
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

    public void stop(boolean stopNow) {
        synchronized (this.lock) {
            if (Objects.nonNull(this.future)) {
                try {
                    this.tryUpload(true);
                } catch (Throwable throwable) {
                    TapLogger.warn(TAG, "tryUpload failed when stop {}, error {}", stopNow, throwable.getMessage());
                    throw throwable;
                } finally {
                    this.future.cancel(stopNow);
                    this.future = null;
                }
            }
        }
    }

    public void monitorIdle() {
        synchronized (this.lock) {
            if (Objects.isNull(future)) {
                this.future = this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
                    try {
                        this.tryUpload(true);
                    } catch (Throwable throwable) {
                        this.throwable = throwable;
                        TapLogger.error(TAG, "Try upload failed in scheduler, {}, will retry after {}", throwable.getMessage(), this.idleSeconds);
                    }
                }, this.initDelay, this.idleSeconds, TimeUnit.SECONDS);
            }
        }
    }

    private synchronized void tryUpload(boolean forced) {
        if (!this.isUploading && this.pendingUploadEvents != null) {
            //TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            this.uploadEvents();
        } else if (this.pendingUploadEvents == null && !this.events.isEmpty() && (forced || System.nanoTime() - this.touch > this.idleSeconds * 1000000000L)) {
//            synchronized (this.pendingLock) {
            this.pendingUploadEvents = this.events;
//            }
            this.events = new CopyOnWriteArrayList<>();
            //TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            this.uploadEvents();
        }
    }

    public synchronized void uploadEvents() {
        this.uploadEvents(null);
    }

    public synchronized void uploadEvents(List<String> tableIdList) {
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
                } else {
                    this.pendingUploadEvents = this.pendingUploadEvents.stream()
                            .filter(e -> Objects.nonNull(e) && !tableIdList.contains(e.getTableId())).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                }
//                }
            }
        } finally {
            touch = System.nanoTime();
            this.isUploading = false;
        }
    }

    public void addTapEvents(List<TapRecordEvent> eventList, TapTable table) {
        if (Objects.isNull(table) || Objects.isNull(table.getId())) {
            throw new CoreException(" Unable to process event record, invalid TapTable or empty table name. ");
        }
        if (Objects.nonNull(eventList) && !eventList.isEmpty()) {
            this.eventProcessor.covert(eventList, table);
            this.events.addAll(eventList);
            Optional.ofNullable(eventList.get(0))
                    .flatMap(event ->
                            Optional.ofNullable(event.getTableId()))
                    .ifPresent(tableId -> {
                        if (this.autoCheckTable) {
                            TapTable agoTable = this.collectedTable.get(tableId);
                            if (Objects.nonNull(agoTable)) {
                                //TODO
                                List<String> tabs = new ArrayList<>();
                                tabs.add(tableId);
                                try {
                                    this.uploadEvents(tabs);
                                } catch (Exception e) {
                                    throw new CoreException(String.format(" The table structure has been changed, and the writing failed after data submission.\nchange ago: %s. \nchange after: %s.\n Please check the table structure. ERROR: %s",
                                            toJson(agoTable),
                                            toJson(table),
                                            e.getMessage())
                                    );
                                }
                            }
                        }
                        this.collectedTable.put(tableId, table);
                    });
            //this.collectedTable.put(table.getId(), table);
        }
        tryUpload(this.events.size() > this.maxRecords);
    }

    public Throwable throwable(){
        return this.throwable;
    }
}
