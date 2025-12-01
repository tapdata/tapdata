package io.tapdata.flow.engine.V2.node.hazelcast.data.adk.vo.increase;

import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Data
public class AdjustInfo {
    final long timestamp;
    final String taskId;

    //number of events
    int eventSize;
    //Current batch count
    int batchSize;

    //The size of the event queue has been used
    int eventQueueSize;
    //Maximum capacity of event queue
    int eventQueueCapacity;
    //Determine the threshold for event queue to be full, should more than 'eventQueueIdleThreshold' and less than 1.0
    double eventQueueFullThreshold = 0.95D;
    //Determine the threshold for idle event queue, should more than 0.0, and less than 'eventQueueFullThreshold'
    double eventQueueIdleThreshold = 0.7D;

    //Average delay of events
    long eventDelay;
    //Threshold for determining excessive event delay, should be more than 0
    long eventDelayThresholdMs = 1_000L;

    //Current number of task nodes
    int currentTaskNodes;
    //The sum of all task nodes on the current engine
    int allTaskNodes;
    //The current number of tasks on the engine
    int allTaskCount;

    //Total memory usage of the event
    long eventMem;
    //Average memory usage of events
    long eventMemAvg;
    //Current total memory usage of the engine
    long sysMem;
    //The current engine is using memory
    long sysMemUsed;
    //Determine the threshold for high task memory, should more than 0.0, and less than 1.0
    double taskMemThreshold = 0.8D;

    public AdjustInfo(String taskId) {
        this.timestamp = System.currentTimeMillis();
        this.taskId = taskId;
    }

    public static AdjustInfo avg(String taskId, List<AdjustInfo> adjustInfos) {
        if (CollectionUtils.isEmpty(adjustInfos)) {
            return new AdjustInfo(taskId);
        }
        if (adjustInfos.size() == 1) {
            return adjustInfos.get(0);
        }
        AdjustInfo adjustInfo = new AdjustInfo(taskId);
        for (AdjustInfo item : adjustInfos) {
            adjustInfo.eventSize += item.eventSize;
            adjustInfo.batchSize = item.batchSize;
            adjustInfo.eventQueueSize = item.eventQueueSize;
            adjustInfo.eventQueueCapacity = item.eventQueueCapacity;
            adjustInfo.eventQueueFullThreshold = item.eventQueueFullThreshold;
            adjustInfo.eventQueueIdleThreshold = item.eventQueueIdleThreshold;
            adjustInfo.eventDelay = Math.max(item.eventDelay, adjustInfo.eventDelay);
            adjustInfo.eventDelayThresholdMs = item.eventDelayThresholdMs;
            adjustInfo.eventMem += item.eventMem;
            adjustInfo.eventMemAvg += item.eventMemAvg;
            adjustInfo.taskMemThreshold = item.taskMemThreshold;
        }
        int size = adjustInfos.size();
        adjustInfo.eventSize = adjustInfo.eventSize / size;
        adjustInfo.eventMem = adjustInfo.eventMem / size;
        adjustInfo.eventMemAvg = adjustInfo.eventMemAvg / size;
        return adjustInfo;
    }
}