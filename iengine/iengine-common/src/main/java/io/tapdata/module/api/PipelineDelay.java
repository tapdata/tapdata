package io.tapdata.module.api;

import java.util.function.BiConsumer;

/**
 * @author Dexter
 */
public interface PipelineDelay {
    Long getDelay(String taskId, String nodeId);

    // The first arg of consumer is event finish time while the second arg is event reference time;
    void consumeDelay(String taskId, String nodeId, BiConsumer<Long, Long> consumer);
}
