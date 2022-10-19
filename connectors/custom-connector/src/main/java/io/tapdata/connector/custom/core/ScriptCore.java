package io.tapdata.connector.custom.core;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class ScriptCore extends Core {

    private static final String TAG = ScriptCore.class.getSimpleName();
    private final String collectionName;
    private final Consumer<List<TapEvent>> eventConsumer;
    private final List<TapEvent> eventList = new ArrayList<>();
    private final int eventBatchSize;

    public ScriptCore(String collectionName, int eventBatchSize, Consumer<List<TapEvent>> eventConsumer) {
        this.collectionName = collectionName;
        this.eventConsumer = eventConsumer;
        this.eventBatchSize = eventBatchSize;
    }

    public void pullAll() {
        if (eventList.size() > 0) {
            eventConsumer.accept(eventList);
            eventList.clear();
        }
    }

    @Override
    public void push(List<Object> data, String op, Object contextMap) {
        if (EmptyKit.isNotEmpty(data)) {
            if (!op.equals(MESSAGE_OPERATION_INSERT)
                    && !op.equals(MESSAGE_OPERATION_UPDATE)
                    && !op.equals(MESSAGE_OPERATION_DELETE)) {
                TapLogger.warn(TAG, "Invalid op value: {}, will skip this data.", op);
                return;
            }
            TapLogger.debug(TAG, "Received data, data size: {}", data.size());
            for (Object datum : data) {
                if (datum instanceof Map) {
                    Map<String, Object> dataMap = (Map) datum;
                    if (EmptyKit.isNotEmpty(dataMap)) {
                        eventList.add(TapSimplify.insertRecordEvent(dataMap, collectionName));
                        if (eventList.size() == eventBatchSize) {
                            eventConsumer.accept(eventList);
                            eventList.clear();
                        }
                    }
                }
            }
        }
    }

}
