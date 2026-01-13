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
    public HandlerUtil.EventTypeRecorder getEventTypeRecorderSyncTapEvent(List<? extends TapEvent> events,String nodeId) {
        if (null == events) events = new ArrayList<>();
        if (null == memorySize) return HandlerUtil.countTapEvent(events,nodeId);
        HandlerUtil.EventTypeRecorder recorder;
        synchronized (memorySize) {
            if (memorySize.get() >= 0) {
                recorder = HandlerUtil.countTapEvent(events, memorySize.get(),nodeId);
                memorySize.set(-1);
            } else {
                recorder = HandlerUtil.countTapEvent(events,nodeId);
                memorySize.set(recorder.getMemorySize());
            }
        }
        return recorder;
    }
    public HandlerUtil.EventTypeRecorder getEventTypeRecorderSyncTapDataEvent(List<TapdataEvent> events,String nodeId) {
        if (null == events) events = new ArrayList<>();
        if (null == memorySize) return HandlerUtil.countTapdataEvent(events,nodeId);
        HandlerUtil.EventTypeRecorder recorder;
        synchronized (memorySize) {
            if (memorySize.get() >= 0) {
                recorder = HandlerUtil.countTapDataEvent(events, memorySize.get(),nodeId);
                memorySize.set(-1);
            } else {
                recorder = HandlerUtil.countTapdataEvent(events,nodeId);
                memorySize.set(recorder.getMemorySize());
            }
        }
        return recorder;
    }
}