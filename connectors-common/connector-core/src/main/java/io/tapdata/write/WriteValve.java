package io.tapdata.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public class WriteValve {
    //
    private int initDelay = 0;
    //
    private int writeSize = 10000;
    //
    //private TapTable targetTable;
    //
    private int submissionInterval = 5;
    //
    private EventCollector eventCollector;
    //
    private TapEventCollector tapEventCollector;
    //
    private Consumer<WriteListResult<TapRecordEvent>> consumer;
    //
    private EventProcessor eventProcessor = (list, table) -> {
    };
    //
    private boolean autoCheckTableWhenWriteOnce = false;

    private Check check;

    //
    public static WriteValve open(
            int writeSize,
            int submissionInterval,
            EventCollector eventCollector,
            EventProcessor recordCovert,
            Consumer<WriteListResult<TapRecordEvent>> writeConsumer) {
        return new WriteValve()
                .writeSize(writeSize)
                .submissionInterval(submissionInterval)
                .eventCollected(eventCollector)
                .recoverCovert(recordCovert)
                .writeConsumer(writeConsumer);
    }

    //
    public static WriteValve open(
            int writeSize,
            int submissionInterval,
            EventCollector eventCollector,
            Consumer<WriteListResult<TapRecordEvent>> writeConsumer) {
        return new WriteValve()
                .writeSize(writeSize)
                .submissionInterval(submissionInterval)
                .eventCollected(eventCollector)
                .writeConsumer(writeConsumer);
    }

    public static WriteValve open(
            EventCollector eventCollector,
            Consumer<WriteListResult<TapRecordEvent>> writeConsumer) {
        return new WriteValve()
                .eventCollected(eventCollector)
                .writeConsumer(writeConsumer);
    }

    private WriteValve() {

    }

    public WriteValve start() {
        if (Objects.isNull(this.tapEventCollector)) {
            synchronized (this) {
                if (Objects.isNull(this.tapEventCollector)) {
                    this.tapEventCollector = TapEventCollector.create()
                            .maxRecords(this.writeSize)
                            .initDelay(this.initDelay)
                            .idleSeconds(this.submissionInterval)
                            //.table(this.targetTable)
                            .writeListResultConsumer(this.consumer)
                            .eventCollected(this.eventCollector);
                    this.tapEventCollector.start();
                }
            }
        }
        return this;
    }

    public void close(boolean stopNow) {
        Optional.ofNullable(this.tapEventCollector).ifPresent(tap -> tap.stop(stopNow));
    }
    public void close() {
        this.close(true);
        this.check.check();
    }

    public void commit(String tableId) {
        if (Objects.isNull(tableId) || "".equals(tableId.trim())) {
            throw new CoreException(" Unable to submit data for an empty table, table name is empty.");
        }
        Optional.ofNullable(this.tapEventCollector).ifPresent(e -> {
            List<String> tab = new ArrayList<>();
            tab.add(tableId);
            e.uploadEvents(tab);
        });
        this.check.check();
    }

    public void commitAll() {
        Optional.ofNullable(this.tapEventCollector).ifPresent(TapEventCollector::uploadEvents);
    }

    public void commit(List<String> tableIdList) {
        if (Objects.isNull(tableIdList) || tableIdList.isEmpty()) {
            throw new CoreException(" If you need to submit the data of all tables immediately, please use commitAll(). ");
        }
        Optional.ofNullable(this.tapEventCollector).ifPresent(e -> e.uploadEvents(tableIdList));
        this.check.check();
    }

    public WriteValve write(List<TapRecordEvent> eventList, TapTable table) {
        this.tapEventCollector.addTapEvents(eventList, table);
        return this.check();
    }

    public WriteValve writeSize(int writeSize) {
        this.writeSize = writeSize;
        return this;
    }

    public WriteValve initDelay(int initDelay) {
        this.initDelay = initDelay;
        return this;
    }

    private WriteValve writeTable(TapTable targetTable) {
        //this.targetTable = targetTable;
        return this;
    }

    public WriteValve submissionInterval(int submissionInterval) {
        this.submissionInterval = submissionInterval;
        return this;
    }

    private WriteValve eventCollected(EventCollector eventCollector) {
        this.eventCollector = eventCollector;
        return this;
    }

    public WriteValve recoverCovert(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
        return this;
    }

    private WriteValve writeConsumer(Consumer<WriteListResult<TapRecordEvent>> writeConsumer) {
        this.consumer = writeConsumer;
        return this;
    }

    public WriteValve autoCheckTableWhenWriteOnce(boolean autoCheck) {
        this.autoCheckTableWhenWriteOnce = autoCheck;
        this.tapEventCollector.autoCheckTable(autoCheck);
        return this;
    }

    public WriteValve check() {
        if (null == check) check = new Check() {
            @Override
            public void check() {
                if (null != tapEventCollector.throwable()) {
                    throw new RuntimeException(tapEventCollector.throwable());
                }
            }
        };
        check.check();
        return this;
    }
}
