package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import io.tapdata.entity.event.TapEvent;
import lombok.Data;

import java.util.List;
import java.util.function.Consumer;

public interface AdjustStage {

    /**
     * Set up consumers for event batch events, paying attention to actively pushing batch data to task nodes and receiving and processing batch data for consumption tasks
     * */
    void setConsumer(Consumer<List<TapEvent>> consumer);

    Consumer<List<TapEvent>> consumer();

    /**
     * Obtain real-time data, event queue depth, memory usage, etc. of the current task node
     * */
    TaskInfo getTaskInfo();

    /**
     * Indicator for reporting changes in batch frequency
     * */
    void metric(MetricInfo metricInfo);

    /**
     * Obtain the node ID of the task node
     * */
    String getNodeId();

    void updateIncreaseReadSize(int increaseReadSize);

    @Data
    class MetricInfo {
        int increaseReadSize;
    }

    @Data
    class TaskInfo {
        int increaseReadSize;

        int eventQueueSize;
        int eventQueueCapacity;
    }
}
