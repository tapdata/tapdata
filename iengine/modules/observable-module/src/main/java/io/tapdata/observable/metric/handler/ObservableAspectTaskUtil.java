package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ObservableAspectTaskUtil {
    private ObservableAspectTaskUtil() {}
    public static void streamReadComplete(
            CompletableFuture<Void> streamReadFuture,
            List<TapEvent> events,
            HandlerUtil.EventTypeRecorder recorder,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            TaskSampleHandler taskSampleHandler,
            Long timestamp) {
        streamReadFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleStreamReadReadComplete(timestamp, recorder));
            taskSampleHandler.handleStreamReadAccept(recorder);
        });
    }

    public static void streamReadProcessComplete(
            CompletableFuture<Void> streamProcessFuture,
            List<TapdataEvent> events,
            HandlerUtil.EventTypeRecorder recorder,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            Long timestamp
    ) {
        streamProcessFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleStreamReadProcessComplete(timestamp, recorder));
        });
    }


    public static void batchReadComplete(
            CompletableFuture<Void> batchReadFuture,
            List<TapEvent> events,
            HandlerUtil.EventTypeRecorder recorder,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            TaskSampleHandler taskSampleHandler,
            Long timestamp) {
        batchReadFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleBatchReadReadComplete(timestamp, recorder));
            taskSampleHandler.handleBatchReadAccept(recorder);
        });
    }

    public static void batchReadProcessComplete(
            CompletableFuture<Void> batchProcessFuture,
            List<TapdataEvent> events,
            HandlerUtil.EventTypeRecorder recorder,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            Long timestamp
    ) {
        batchProcessFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleBatchReadProcessComplete(timestamp, recorder));
        });
    }
}
