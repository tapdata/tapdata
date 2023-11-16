package io.tapdata.observable.metric.util;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.metric.handler.HandlerUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SyncGetMemorySizeHandler {
    final AtomicLong memorySize;
    public SyncGetMemorySizeHandler(AtomicLong memorySize){
        this.memorySize = memorySize;
    }

    public HandlerUtil.EventTypeRecorder getEventTypeRecorderSyncTapEvent(List<? extends TapEvent> events) {
        if (null == memorySize) return HandlerUtil.countTapEvent(events);
        HandlerUtil.EventTypeRecorder recorder;
        if (null == events) events = new ArrayList<>();
        synchronized (memorySize) {
            if (memorySize.get() >= 0) {
                recorder = HandlerUtil.countTapEvent(events, memorySize.get());
            } else {
                recorder = HandlerUtil.countTapEvent(events);
            }
        }
        return recorder;
    }
    public HandlerUtil.EventTypeRecorder getEventTypeRecorderSyncTapDataEvent(List<TapdataEvent> events) {
        if (null == memorySize) return HandlerUtil.countTapdataEvent(events);
        HandlerUtil.EventTypeRecorder recorder;
        if (null == events) events = new ArrayList<>();
        synchronized (memorySize) {
            if (memorySize.get() >= 0) {
                recorder = HandlerUtil.countTapDataEvent(events, memorySize.get());
            } else {
                recorder = HandlerUtil.countTapdataEvent(events);
            }
        }
        return recorder;
    }
}
