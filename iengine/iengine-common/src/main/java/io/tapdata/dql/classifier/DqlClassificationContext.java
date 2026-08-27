package io.tapdata.dql.classifier;

import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.Objects;

/**
 * Context supplied by an Engine capture point to the DQL classifier.
 */
public final class DqlClassificationContext {
    private final DqlFailedStage failedStage;
    private final DqlNodeType nodeType;
    private final TapRecordEvent event;
    private final DqlBatchContext batchContext;
    private final DqlTaskContext taskContext;

    public DqlClassificationContext(DqlFailedStage failedStage,
                                    DqlNodeType nodeType,
                                    TapRecordEvent event,
                                    DqlBatchContext batchContext,
                                    DqlTaskContext taskContext) {
        this.failedStage = Objects.requireNonNull(failedStage, "failedStage must not be null");
        this.nodeType = Objects.requireNonNull(nodeType, "nodeType must not be null");
        this.event = event;
        this.batchContext = Objects.requireNonNull(batchContext, "batchContext must not be null");
        this.taskContext = Objects.requireNonNull(taskContext, "taskContext must not be null");
    }

    public DqlFailedStage getFailedStage() {
        return failedStage;
    }

    public DqlNodeType getNodeType() {
        return nodeType;
    }

    public TapRecordEvent getEvent() {
        return event;
    }

    public DqlBatchContext getBatchContext() {
        return batchContext;
    }

    public DqlTaskContext getTaskContext() {
        return taskContext;
    }
}
