package io.tapdata.observable.metric;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.module.api.PipelineDelay;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Dexter
 */
@Implementation(PipelineDelay.class)
public class PipelineDelayImpl implements PipelineDelay {
    Map<String, Map<String, Long>> eventProcessCostMap = new ConcurrentHashMap<>();
    Map<String, Map<String, Long>> eventReferenceTimeMap = new ConcurrentHashMap<>();

    void refreshDelay(String taskId, String nodeId, Long eventProcessCost, Long eventReferenceTime ) {
        eventProcessCostMap.putIfAbsent(taskId, new HashMap<>());
        eventProcessCostMap.get(taskId).putIfAbsent(nodeId, eventProcessCost);

        eventReferenceTimeMap.putIfAbsent(taskId, new HashMap<>());
        eventReferenceTimeMap.get(taskId).putIfAbsent(nodeId, eventReferenceTime);
    }

    @Override
    public Long getEventProcessCost(String taskId, String nodeId) {
        Long eventProcessCost = null;
        Map<String, Long> eventProcessCostNodeMap = eventProcessCostMap.get(taskId);
        if (null != eventProcessCostNodeMap) {
            eventProcessCost = eventProcessCostNodeMap.get(nodeId);
        }

        return eventProcessCost;
    }


    @Override
    public Long getEventReferenceTime(String taskId, String nodeId) {
        Long eventReferenceTime = null;
        Map<String, Long> eventReferenceTimeNodeMap = eventReferenceTimeMap.get(taskId);
        if (null != eventReferenceTimeNodeMap) {
            eventReferenceTime = eventReferenceTimeNodeMap.get(nodeId);
        }

        return eventReferenceTime;
    }

    public void clear(String taskId) {
        this.eventProcessCostMap.remove(taskId);
        this.eventReferenceTimeMap.remove(taskId);
    }

    @Override
    public Long getDelay(String taskId, String nodeId) {
        return getEventProcessCost(taskId, nodeId);
    }

}
