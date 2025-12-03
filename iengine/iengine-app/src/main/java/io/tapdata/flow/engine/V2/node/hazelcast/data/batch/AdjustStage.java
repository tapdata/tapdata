package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import lombok.Data;

import java.util.List;
import java.util.function.Consumer;

public interface AdjustStage {

    void needAdjustBatchSize(boolean open);

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

        //Determine the threshold for event queue to be full, should more than 'eventQueueIdleThreshold' and less than 1.0
        double eventQueueFullThreshold = 0.95D;
        //Determine the threshold for idle event queue, should more than 0.0, and less than 'eventQueueFullThreshold'
        double eventQueueIdleThreshold = 0.7D;
        //Threshold for determining excessive event delay, should be more than 0
        long eventDelayThresholdMs = 1_000L;
        //Determine the threshold for high task memory, should more than 0.0, and less than 1.0
        double taskMemThreshold = 0.8D;

        public Number parse(String value, Number defaultVal) {
            try {
                return Double.parseDouble(value);
            } catch (Exception e) {
                return defaultVal;
            }
        }
    }
}
