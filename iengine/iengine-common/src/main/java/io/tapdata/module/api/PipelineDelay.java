package io.tapdata.module.api;

/**
 * @author Dexter
 */
public interface PipelineDelay {
    Long getEventFinishTime(String taskId, String nodeId);

    Long getEventReferenceTime(String taskId, String nodeId);

    Long getDelay(String taskId, String nodeId);
}
