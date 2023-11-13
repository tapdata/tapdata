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
import java.util.concurrent.atomic.AtomicReference;

public class RandomSampleEventHandler {
    public static final int MIN_SAMPLE_SIZE = 10;
    public static final int MAX_SAMPLE_SIZE = 100;
    private double sampleRate;
    public RandomSampleEventHandler(double sampleRate) {
        if (sampleRate <= 0 || sampleRate > 1) sampleRate = 1;
        this.sampleRate = sampleRate;
    }

    public void simpleMemoryTapEvent(HandlerUtil.EventTypeRecorder recorder, List<?> events, HandleEvent handle) {
        List<Object> simples = randomSampleList(events, sampleRate);
        if (null == simples || simples.isEmpty()) return;
        long sizeOfSampleListByte = 0L;
        AtomicReference<String> tableId = new AtomicReference<>();
        for (Object item : simples) {
            TapEvent tapEvent = handle.handel(item);
            if (!(tapEvent instanceof TapRecordEvent)) {
                continue;
            }
            if (null == tableId.get()) {
                tableId.set(((TapRecordEvent) tapEvent).getTableId());
            }
            Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
            Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
            if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
                continue;
            }
            if (MapUtils.isNotEmpty(before)) {
                sizeOfSampleListByte += RamUsageEstimator.sizeOfMap(before);
            }
            if (MapUtils.isNotEmpty(after)) {
                sizeOfSampleListByte += RamUsageEstimator.sizeOfMap(after);
            }
        }
        unitConversion(recorder, sizeOfSampleListByte);
    }

    private List<Object> randomSampleList(List<?> events, Double sampleRate) {
        if (CollectionUtils.isEmpty(events)) {
            return null;
        }
        List<Object> copyList = new ArrayList<>(events);
        if (sampleRate.intValue() == 1) return copyList;
        List<Object> randomSampleList = new ArrayList<>();
        int sampleSize = Math.max(MIN_SAMPLE_SIZE, (int) (copyList.size() * sampleRate));
        sampleSize = Math.min(MAX_SAMPLE_SIZE, sampleSize);
        sampleSize = Math.min(copyList.size(), sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            int randomIndex = RandomUtils.nextInt(0, copyList.size());
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
