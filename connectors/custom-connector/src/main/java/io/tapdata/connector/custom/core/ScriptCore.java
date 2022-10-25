package io.tapdata.connector.custom.core;

import io.tapdata.connector.custom.CustomEventMessage;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ScriptCore extends Core {

    private static final String TAG = ScriptCore.class.getSimpleName();
    private static final ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class); //bean util
    private final String collectionName;
    private final LinkedBlockingQueue<CustomEventMessage> eventQueue = new LinkedBlockingQueue<>(5000);
    private int alwaysWarn = 0;

    public ScriptCore(String collectionName) {
        this.collectionName = collectionName;
    }

    public LinkedBlockingQueue<CustomEventMessage> getEventQueue() {
        return eventQueue;
    }

    @Override
    public void push(List<Object> data, String op, Object contextMap) {
        if (EmptyKit.isNotEmpty(data)) {
            if (!op.equals(MESSAGE_OPERATION_INSERT)
                    && !op.equals(MESSAGE_OPERATION_UPDATE)
                    && !op.equals(MESSAGE_OPERATION_DELETE)) {
                if (alwaysWarn < 3) {
                    TapLogger.warn(TAG, "Invalid op value: {}, will skip this data.", op);
                }
                alwaysWarn++;
                if (alwaysWarn >= 5000) {
                    TapLogger.warn(TAG, "5000 rows has been skipped as invalid op {}.", op);
                    alwaysWarn = 0;
                }
                return;
            }
            alwaysWarn = 0;
            TapLogger.debug(TAG, "Received data, data size: {}", data.size());
            for (Object datum : data) {
                if (datum instanceof Map) {
                    Map<String, Object> dataMap = (Map) datum;
                    if (EmptyKit.isNotEmpty(dataMap)) {
                        //deep copy to resolve problem with multi thread
                        Map<String, Object> newMap = (Map) objectSerializable.toObject(objectSerializable.fromObject(new HashMap<>(dataMap)));
                        try {
                            //put into the queue
                            while (!eventQueue.offer(new CustomEventMessage()
                                    .tapEvent(TapSimplify.insertRecordEvent(newMap, collectionName).referenceTime(System.currentTimeMillis()))
                                    .contextMap(contextMap), 1, TimeUnit.SECONDS)) {
                                TapLogger.warn(TAG, "log queue is full, waiting...");
                            }
                        } catch (InterruptedException ignored) {

                        }
                    }
                }
            }
        }
    }

}
