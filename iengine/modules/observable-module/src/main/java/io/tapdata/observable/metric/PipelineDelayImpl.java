package io.tapdata.observable.metric;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.module.api.PipelineDelay;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * @author Dexter
 */
@Implementation(PipelineDelay.class)
public class PipelineDelayImpl implements PipelineDelay {

    Map<String, Map<String, Long>> eventFinishTimeMap = new ConcurrentHashMap<>();
    Map<String, Map<String, Long>> eventReferenceTimeMap = new ConcurrentHashMap<>();

    void refreshDelay(String taskId, String nodeId, Long eventFinishTime, Long eventReferenceTime ) {
        eventFinishTimeMap.putIfAbsent(taskId, new HashMap<>());
        eventFinishTimeMap.get(taskId).putIfAbsent(nodeId, eventFinishTime);

        eventReferenceTimeMap.putIfAbsent(taskId, new HashMap<>());
        eventReferenceTimeMap.get(taskId).putIfAbsent(nodeId, eventReferenceTime);
    }

    private Long getEventFinishTime(String taskId, String nodeId) {
        Long eventFinishTime = null;
        Map<String, Long> eventFinishTimeNodeMap = eventFinishTimeMap.get(taskId);
        if (null != eventFinishTimeNodeMap) {
            eventFinishTime = eventFinishTimeNodeMap.get(nodeId);
        }

        return eventFinishTime;
    }

    private Long getEventReferenceTime(String taskId, String nodeId) {
        Long eventReferenceTime = null;
        Map<String, Long> eventReferenceTimeNodeMap = eventReferenceTimeMap.get(taskId);
        if (null != eventReferenceTimeNodeMap) {
            eventReferenceTime = eventReferenceTimeNodeMap.get(nodeId);
        }

        return eventReferenceTime;
    }

    public void clear(String taskId) {
        this.eventFinishTimeMap.remove(taskId);
        this.eventReferenceTimeMap.remove(taskId);
    }

    @Override
    public Long getDelay(String taskId, String nodeId) {
        Long eventFinishTime = getEventFinishTime(taskId, nodeId);
        Long eventReferenceTime = getEventReferenceTime(taskId, nodeId);
        if (null != eventFinishTime && null != eventReferenceTime) {
            return eventFinishTime - eventReferenceTime;
        }

        return null;
    }

    @Override
    public void consumeDelay(String taskId, String nodeId, BiConsumer<Long, Long> consumer) {
        consumer.accept(getEventFinishTime(taskId, nodeId), getEventReferenceTime(taskId, nodeId));
    }
}
