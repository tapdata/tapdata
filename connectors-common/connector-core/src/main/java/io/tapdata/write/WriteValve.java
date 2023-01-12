package io.tapdata.write;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

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
    private TapTable targetTable;
    //
    private int submissionInterval = 5;
    //
    private EventCollector eventCollector;
    //
    private TapEventCollector tapEventCollector;
    //
    private Consumer<WriteListResult<TapRecordEvent>> consumer;
    //
    private RecordProcessor recordCovert = (list, table)->{};
    //
    public static WriteValve open(
            TapTable writeTable,
            int writeSize,
            int submissionInterval,
            EventCollector eventCollector,
            RecordProcessor recordCovert,
            Consumer<WriteListResult<TapRecordEvent>> writeConsumer ) {
        return new WriteValve()
                .writeSize(writeSize)
                .writeTable(writeTable)
                .submissionInterval(submissionInterval)
                .eventCollected(eventCollector)
                .recoverCovert(recordCovert)
                .writeConsumer(writeConsumer).start();
    }

    public static WriteValve open(
            TapTable writeTable,
            EventCollector eventCollector,
            Consumer<WriteListResult<TapRecordEvent>> writeConsumer ) {
        return new WriteValve()
                .writeTable(writeTable)
                .eventCollected(eventCollector)
                .writeConsumer(writeConsumer).start();
    }

    private WriteValve() {

    }

    private WriteValve start() {
        if (Objects.isNull(this.tapEventCollector)) {
            synchronized (this) {
                if (Objects.isNull(this.tapEventCollector)) {
                    this.tapEventCollector = TapEventCollector.create()
                            .maxRecords(this.writeSize)
                            .initDelay(this.initDelay)
                            .idleSeconds(this.submissionInterval)
                            .table(this.targetTable)
                            .writeListResultConsumer(this.consumer)
                            .eventCollected(this.eventCollector);
                    this.tapEventCollector.start();
                }
            }
        }
        return this;
    }

    public void close(){
        Optional.ofNullable(this.tapEventCollector).ifPresent(TapEventCollector::stop);
    }

    public WriteValve write(List<TapRecordEvent> eventList) {
        this.tapEventCollector.addTapEvents(eventList, targetTable);
        return this;
    }

    public WriteValve writeSize(int writeSize){
        this.writeSize = writeSize;
        return this;
    }
    public WriteValve initDelay(int initDelay){
        this.initDelay = initDelay;
        return this;
    }
    private WriteValve writeTable(TapTable targetTable){
        this.targetTable = targetTable;
        return this;
    }
    public WriteValve submissionInterval(int submissionInterval){
        this.submissionInterval = submissionInterval;
        return this;
    }
    private WriteValve eventCollected(EventCollector eventCollector){
        this.eventCollector = eventCollector;
        return this;
    }
    public WriteValve recoverCovert(RecordProcessor recordCovert){
        this.recordCovert = recordCovert;
        return this;
    }
    private WriteValve writeConsumer(Consumer<WriteListResult<TapRecordEvent>> writeConsumer){
        this.consumer = writeConsumer;
        return this;
    }
}
