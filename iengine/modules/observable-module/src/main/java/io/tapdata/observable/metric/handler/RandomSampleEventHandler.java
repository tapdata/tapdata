package io.tapdata.observable.metric.handler;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class RandomSampleEventHandler {
    public static final int MIN_SAMPLE_SIZE = 10;
    public static final int MAX_SAMPLE_SIZE = 100;
    protected final double sampleRate;
    public RandomSampleEventHandler(double sampleRate) {
        if (sampleRate <= 0 || sampleRate > 1) sampleRate = 1;
        this.sampleRate = sampleRate;
    }

    public void sampleMemoryTapEvent(HandlerUtil.EventTypeRecorder recorder, List<?> events, HandleEvent handle) {
        List<Object> samples = randomSampleList(events, sampleRate);
        if (samples.isEmpty()) return;
        long sizeOfSampleListByte = 0L;
        String tableId = null;
        for (Object item : samples) {
            if (null == item) continue;
            TapEvent tapEvent = handle.handel(item);
            if (tapEvent instanceof TapRecordEvent) {
                if (null == tableId) {
                    tableId = ((TapRecordEvent) tapEvent).getTableId();
                }
                sizeOfSampleListByte += sizeOfTapEvent(tapEvent);
            }
        }
        unitConversion(recorder, sizeOfSampleListByte);
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
        if (sampleRate <= 0 || sampleRate >= 1) return (List<Object>)events;
        List<Object> copyList = new ArrayList<>();
        copyList.addAll(events);
        int size = copyList.size();
        int sampleSize = Math.max(MIN_SAMPLE_SIZE, (int) (size * sampleRate));
        sampleSize = Math.min(MAX_SAMPLE_SIZE, sampleSize);
        sampleSize = Math.min(size, sampleSize);
        HashSet<Integer> indexSet = new HashSet<>();
        for (int i = 0; i < sampleSize; i++) {
            indexSet.add(i);
        }
        for (Integer randomIndex : indexSet) {
            randomSampleList.add(copyList.get(randomIndex));
        }
        return randomSampleList;
    }

    protected void unitConversion(HandlerUtil.EventTypeRecorder recorder, long sizeOfSampleListByte) {
        recorder.setMemorySize(sizeOfSampleListByte);
        recorder.setMemoryUtil("B");
    }

    public interface HandleEvent {
        TapEvent handel(Object eventObject);
    }
}
