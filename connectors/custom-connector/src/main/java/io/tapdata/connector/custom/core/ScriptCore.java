package io.tapdata.connector.custom.core;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ScriptCore extends Core {

    private static final String TAG = ScriptCore.class.getSimpleName();
    private final String collectionName;
    private final LinkedBlockingQueue<TapEvent> eventQueue = new LinkedBlockingQueue<>(5000);
    private Object contextMap;

    public ScriptCore(String collectionName) {
        this.collectionName = collectionName;
    }

    public LinkedBlockingQueue<TapEvent> getEventQueue() {
        return eventQueue;
    }

    public Object getContextMap() {
        return contextMap;
    }

    public void setContextMap(Object contextMap) {
        this.contextMap = contextMap;
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
                    Map<String, Object> dataMap = (Map<String, Object>) datum;
                    if (EmptyKit.isNotEmpty(dataMap)) {
                        //deep copy to resolve problem with multi thread
                        Map<String, Object> newMap = new HashMap<>(dataMap);
                        try {
                            //put into the queue
                            while (!eventQueue.offer(TapSimplify.insertRecordEvent(newMap, collectionName).referenceTime(System.currentTimeMillis()), 1, TimeUnit.SECONDS)) {
                                TapLogger.warn(TAG, "log queue is full, waiting...");
                            }
                        } catch (InterruptedException ignored) {

                        }
                    }
                }
            }
            this.contextMap = contextMap;
        }
    }

}
