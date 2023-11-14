package io.tapdata.observable.metric.handler;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RandomSampleEventHandler {
    public static final int MIN_SAMPLE_SIZE = 10;
    public static final int MAX_SAMPLE_SIZE = 100;
    private final double sampleRate;
    public RandomSampleEventHandler(double sampleRate) {
        if (sampleRate <= 0 || sampleRate > 1) sampleRate = 1;
        this.sampleRate = sampleRate;
    }

    public void simpleMemoryTapEvent(HandlerUtil.EventTypeRecorder recorder, List<?> events, HandleEvent handle) {
        List<Object> simples = randomSampleList(events, sampleRate);
        if (null == simples || simples.isEmpty()) return;
        long sizeOfSampleListByte = 0L;
        String tableId = null;
        for (Object item : simples) {
            if (null == item) continue;
            TapEvent tapEvent = handle.handel(item);
            if (null == tableId) {
                tableId = ((TapRecordEvent) tapEvent).getTableId();
            }
            sizeOfSampleListByte += sizeOfTapEvent(tapEvent);
        }
        unitConversion(recorder, sizeOfSampleListByte);
    }

    private long sizeOfTapEvent(TapEvent tapEvent) {
        if (!(tapEvent instanceof TapRecordEvent)) {
            return 0;
        }
        return sizeOfDataMap(TapEventUtil.getAfter(tapEvent), sizeOfDataMap(TapEventUtil.getBefore(tapEvent), 0));
    }

    private long sizeOfDataMap(Map<?, ?> map, long sizeOfSampleListByte) {
        if (MapUtils.isEmpty(map)) {
            return sizeOfSampleListByte;
        }
        return RamUsageEstimator.sizeOfMap(map) + sizeOfSampleListByte;
    }

    private List<Object> randomSampleList(List<?> events, Double sampleRate) {
        if (CollectionUtils.isEmpty(events)) {
            return null;
        }
        if (sampleRate <= 0) return null;
        if (sampleRate >= 1) return (List<Object>) events;
        List<Object> copyList = new ArrayList<>(events);
        int size = copyList.size();
        List<Object> randomSampleList = new ArrayList<>();
        int sampleSize = Math.max(MIN_SAMPLE_SIZE, (int) (size * sampleRate));
        sampleSize = Math.min(MAX_SAMPLE_SIZE, sampleSize);
        sampleSize = Math.min(size, sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            int randomIndex = RandomUtils.nextInt(0, size);
            randomSampleList.add(copyList.get(randomIndex));
            copyList.remove(randomIndex);
        }
        return randomSampleList;
    }

    private void unitConversion(HandlerUtil.EventTypeRecorder recorder, long sizeOfSampleListByte) {
        recorder.setMemorySize(sizeOfSampleListByte);
        recorder.setMemoryUtil("B");
    }

    interface HandleEvent {
        TapEvent handel(Object eventObject);
    }
}
