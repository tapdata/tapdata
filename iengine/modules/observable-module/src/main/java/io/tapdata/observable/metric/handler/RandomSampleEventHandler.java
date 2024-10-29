package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.*;
import java.util.stream.Collectors;

public class RandomSampleEventHandler {
    protected final double sampleRate;

    long totalSize;
    public RandomSampleEventHandler(double sampleRate) {
        if (sampleRate <= 0 || sampleRate > 1) sampleRate = 1;
        this.sampleRate = sampleRate;
    }

    public void sampleMemoryTapEvent(HandlerUtil.EventTypeRecorder recorder, List<?> events, HandleEvent handle) {
        if (null == events || events.isEmpty()) return;
        List<Object> samples = Collections.singletonList(events.get(0));
        totalSize = events.size();
        long sizeOfSampleListByte = 0L;
        for (Object item : samples) {
            TapEvent tapEvent = handle.handel(item);
            if (null == tapEvent) {
                continue;
            }
            if (null != tapEvent.getMemorySize()) {
                sizeOfSampleListByte += tapEvent.getMemorySize();
            } else {
                sizeOfSampleListByte += sizeOfTapEvent(tapEvent);
            }
        }
        unitConversion(recorder, sizeOfSampleListByte);
    }

    public long sampleMemoryTapEvent(List<?> events, HandleEvent handle) {
        if (null == events) return 0L;
        List<Object> samples = Collections.singletonList(events.get(0));
        totalSize = events.size();
		long sizeOfSampleByte = 0L;
        for (Object item : samples) {
            TapEvent tapEvent = handle.handel(item);
            sizeOfSampleByte += sizeOfTapEvent(tapEvent);
        }
        return sizeOfSampleByte;
    }

    protected long sizeOfTapEvent(TapEvent tapEvent) {
        if (!(tapEvent instanceof TapRecordEvent)) {
            return 0;
        }
        return sizeOfDataMap(TapEventUtil.getAfter(tapEvent), sizeOfDataMap(TapEventUtil.getBefore(tapEvent), 0));
    }

    protected long sizeOfDataMap(Map<?, ?> map, long sizeOfSampleListByte) {
        if (sizeOfSampleListByte < 0) sizeOfSampleListByte = 0;
        if (MapUtils.isEmpty(map)) {
            return sizeOfSampleListByte;
        }
        return RamUsageEstimator.sizeOfMap(map) + sizeOfSampleListByte;
    }

    protected List<Object> randomSampleList(List<?> events, Double sampleRate) {
        List<Object> randomSampleList = new ArrayList<>();
        if (CollectionUtils.isEmpty(events)) {
            return randomSampleList;
        }
        //if (sampleRate <= 0 || sampleRate >= 1) return (List<Object>)events;
        List<Object> copyList = new ArrayList<>();
        copyList.addAll(events);
        int size = copyList.size();
        int sampleSize = (int)(Math.random() * size);
        randomSampleList.add(events.get(sampleSize));
        setTotalSize(size);
        return randomSampleList;
    }

    protected void unitConversion(HandlerUtil.EventTypeRecorder recorder, long sizeOfSampleListByte) {
        long size = sizeOfSampleListByte * getTotalSize();
        recorder.setMemorySize(size / 4);
        recorder.setMemoryUtil("B");
    }

    public interface HandleEvent {
        TapEvent handel(Object eventObject);
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }
}
