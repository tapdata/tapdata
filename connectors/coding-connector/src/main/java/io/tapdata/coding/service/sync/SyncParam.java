package io.tapdata.coding.service.sync;

import io.tapdata.entity.event.TapEvent;

import java.util.List;
import java.util.function.BiConsumer;

public class SyncParam {
    Long readStartTime;
    Long readEndTime;
    int readSize;
    Object offsetState;
    BiConsumer<List<TapEvent>, Object> consumer;

    public Long getReadStartTime() {
        return readStartTime;
    }

    public void setReadStartTime(Long readStartTime) {
        this.readStartTime = readStartTime;
    }

    public Long getReadEndTime() {
        return readEndTime;
    }

    public void setReadEndTime(Long readEndTime) {
        this.readEndTime = readEndTime;
    }

    public int getReadSize() {
        return readSize;
    }

    public void setReadSize(int readSize) {
        this.readSize = readSize;
    }

    public Object getOffsetState() {
        return offsetState;
    }

    public void setOffsetState(Object offsetState) {
        this.offsetState = offsetState;
    }

    public BiConsumer<List<TapEvent>, Object> getConsumer() {
        return consumer;
    }

    public void setConsumer(BiConsumer<List<TapEvent>, Object> consumer) {
        this.consumer = consumer;
    }
}
