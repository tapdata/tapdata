package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.*;
import java.util.stream.Collectors;

public class RandomSampleEventHandler {
    protected final double sampleRate;

    public RandomSampleEventHandler(double sampleRate) {
        if (sampleRate <= 0 || sampleRate > 1) sampleRate = 1;
        this.sampleRate = sampleRate;
    }

    public void sampleMemoryTapEvent(HandlerUtil.EventTypeRecorder recorder, List<?> events, HandleEvent handle) {
        if (null == recorder || null == events || events.isEmpty()) return;
        final long batchSize = events.size();
        long sizeOfSampleListByte = 0L;
        TapEvent firstEvent = handle.handel(events.get(0));
        if (null != firstEvent) {
            if (null != firstEvent.getMemorySize()) {
                sizeOfSampleListByte = firstEvent.getMemorySize();
            } else {
                sizeOfSampleListByte = sizeOfTapEvent(firstEvent);
            }
        }
        unitConversion(recorder, sizeOfSampleListByte, batchSize);
    }

    public long sampleMemoryTapEvent(List<?> events, HandleEvent handle) {
        if (null == events || events.isEmpty()) return 0L;
        TapEvent firstEvent = handle.handel(events.get(0));
        if (null == firstEvent) return 0L;
        return sizeOfTapEvent(firstEvent);
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
        List<Object> copyList = new ArrayList<>();
        copyList.addAll(events);
        int size = copyList.size();
        int sampleSize = (int)(Math.random() * size);
        randomSampleList.add(events.get(sampleSize));
        return randomSampleList;
    }

    protected void unitConversion(HandlerUtil.EventTypeRecorder recorder, long sizeOfSampleListByte, long batchSize) {
        if (null == recorder) {
            return;
        }
        final long size;
        if (sizeOfSampleListByte <= 0L || batchSize <= 0L) {
            size = 0L;
        } else if (sizeOfSampleListByte > Long.MAX_VALUE / batchSize) {
            size = Long.MAX_VALUE;
        } else {
            size = sizeOfSampleListByte * batchSize;
        }
        recorder.setMemorySize(size / 4);
        recorder.setMemoryUtil("B");
    }

    public interface HandleEvent {
        TapEvent handel(Object eventObject);
    }
}
