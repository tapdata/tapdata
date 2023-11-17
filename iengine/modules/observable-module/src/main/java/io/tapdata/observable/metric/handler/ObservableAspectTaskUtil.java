package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ObservableAspectTaskUtil {
    private ObservableAspectTaskUtil() {}
    public static void streamReadComplete(
            CompletableFuture<Void> streamReadFuture,
            List<TapEvent> events,
            SyncGetMemorySizeHandler syncGetMemorySizeHandler,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            TaskSampleHandler taskSampleHandler) {
        streamReadFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            HandlerUtil.EventTypeRecorder recorder = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
                    handler -> {
                        handler.handleStreamReadReadComplete(System.currentTimeMillis(), recorder);
                    }
            );
            taskSampleHandler.handleStreamReadAccept(recorder);
        });
    }

    public static void streamReadProcessComplete(
            CompletableFuture<Void> streamProcessFuture,
            List<TapdataEvent> events,
            SyncGetMemorySizeHandler syncGetMemorySizeHandler,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers
    ) {
        streamProcessFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            HandlerUtil.EventTypeRecorder recorder = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapDataEvent(events);
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
                    handler -> {
                        handler.handleStreamReadProcessComplete(System.currentTimeMillis(), recorder);
                    }
            );
        });
    }


    public static void batchReadComplete(
            CompletableFuture<Void> batchReadFuture,
            List<TapEvent> events,
            SyncGetMemorySizeHandler syncGetMemorySizeHandler,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers,
            TaskSampleHandler taskSampleHandler) {
        batchReadFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            HandlerUtil.EventTypeRecorder recorder = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler ->
                    handler.handleBatchReadReadComplete(System.currentTimeMillis(), recorder));
            taskSampleHandler.handleBatchReadAccept(recorder);
        });
    }

    public static void batchReadProcessComplete(
            CompletableFuture<Void> batchProcessFuture,
            List<TapdataEvent> events,
            SyncGetMemorySizeHandler syncGetMemorySizeHandler,
            String nodeId,
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers
    ) {
        batchProcessFuture.thenRun(() -> {
            if (null == events || events.isEmpty()) {
                return;
            }
            HandlerUtil.EventTypeRecorder recorder = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapDataEvent(events);
            Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler ->
                    handler.handleBatchReadProcessComplete(System.currentTimeMillis(), recorder)
            );
        });
    }
}
