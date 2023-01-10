package io.tapdata.bigquery.service.stream.handle;

import io.tapdata.bigquery.util.bigQueryUtil.SqlValueConvert;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TapEventCollector {
    private static final String TAG = TapEventCollector.class.getSimpleName();

    private Long touch;
    private TapTable table;
    private int idleSeconds = 5;
    private int maxRecords = 1000;
    private ScheduledFuture<?> future;
    private boolean isUploading = false;
    private EventCollected eventCollected;
    private final Object lock = new int[0];
    private List<TapRecordEvent> pendingUploadEvents;
    private Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private List<TapRecordEvent> events = new CopyOnWriteArrayList<>();

    public void start() {
        if (this.isStarted.compareAndSet(false, true)) {
            monitorIdle();
        }
    }

    public void stop() {
        if (Objects.nonNull(this.future))
            this.future.cancel(true);
    }

    public interface EventCollected {
        void collected(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table);
    }

    public static TapEventCollector create() {
        return new TapEventCollector();
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

    public TapEventCollector eventCollected(EventCollected eventCollected) {
        this.eventCollected = eventCollected;
        return this;
    }

    public TapEventCollector writeListResultConsumer(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        this.writeListResultConsumer = writeListResultConsumer;
        return this;
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
                }, 1, 5, TimeUnit.SECONDS);
            }
        }
    }

    private synchronized void tryUpload(boolean forced) {
        if (!this.isUploading && this.pendingUploadEvents != null) {
            TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            uploadEvents();
        } else if ((this.pendingUploadEvents == null && !this.events.isEmpty()) && (forced || (this.touch != null && System.currentTimeMillis() - this.touch > this.idleSeconds * 1000L))) {
            this.pendingUploadEvents = this.events;
            this.events = new CopyOnWriteArrayList<>();
            TapLogger.info(TAG, "Try upload forced {} delay {} pendingUploadEvents {}", forced, this.touch != null ? System.currentTimeMillis() - this.touch : 0, this.pendingUploadEvents.size());
            uploadEvents();
        }
    }

    private void uploadEvents() {
        this.isUploading = true;
        try {
            if (this.pendingUploadEvents != null) {
                if (this.eventCollected != null)
                    this.eventCollected.collected(this.writeListResultConsumer, this.pendingUploadEvents, this.table);
                this.pendingUploadEvents = null;
            }
        } finally {
            this.isUploading = false;
        }
    }

    public void addTapEvents(List<TapRecordEvent> eventList,TapTable table) {
        if (eventList != null && !eventList.isEmpty()) {
            this.transform(eventList, table);
            this.events.addAll(eventList);
        }
        this.touch = System.currentTimeMillis();
        tryUpload(this.events.size() > this.maxRecords);
    }

    private void transform(List<TapRecordEvent> eventList,TapTable table){
        //if (!isMixedUpdates) return;
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        if (Objects.isNull(nameFieldMap) || nameFieldMap.isEmpty()) {
            throw new CoreException("TapTable not any fields.");
        }
        for (TapRecordEvent event : eventList) {
            if (Objects.isNull(event)) continue;
            Map<String,Object> record = new HashMap<>();
            if (event instanceof TapInsertRecordEvent){
                Map<String,Object> recordMap = new HashMap<>();
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent)event;
                record = insertRecordEvent.getAfter();
                Map<String, Object> finalRecord = record;
                nameFieldMap.forEach((key, f)-> {
                    Object value = finalRecord.get(key);
                    if (Objects.nonNull(value)) {
                        recordMap.put(key, SqlValueConvert.streamJsonArrayValue(value, f));
                    }
                });
                insertRecordEvent.after(recordMap);
            }
            //else if(event instanceof TapUpdateRecordEvent){
            //    record = ((TapUpdateRecordEvent)event).getAfter();
            //}
            //else if(event instanceof TapDeleteRecordEvent){
            //    record = ((TapDeleteRecordEvent)event).getBefore();
            //}
            //else {
            //}
        }
    }
}
