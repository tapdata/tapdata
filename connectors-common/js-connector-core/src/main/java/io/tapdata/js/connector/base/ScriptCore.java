package io.tapdata.js.connector.base;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.js.connector.iengine.LoadJavaScripter;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScriptCore extends Core {

    private static final String TAG = ScriptCore.class.getSimpleName();
    private final LinkedBlockingQueue<CustomEventMessage> eventQueue = new LinkedBlockingQueue<>(5000);
    private int fullQueueWarn = 0;
    private final io.tapdata.js.connector.base.EventType eventType = new io.tapdata.js.connector.base.EventType();

    public ScriptCore() {
    }

    public LinkedBlockingQueue<CustomEventMessage> getEventQueue() {
        return eventQueue;
    }

    @Override
    public void push(List<Object> data, String op, Object contextMap) {
        AtomicInteger dataIndex = new AtomicInteger(1);
        if (Objects.nonNull(data) && !data.isEmpty()) {
            for (Object dataEventMap : data) {
                if (Objects.isNull(dataEventMap)) {
                    continue;
                }
                TapEvent event = this.eventType.setEvent(dataEventMap, dataIndex.get(), TAG);
                dataIndex.getAndIncrement();
                try {
                    while (!eventQueue.offer(new CustomEventMessage().tapEvent(event)
                            .contextMap(this.paper(contextMap)), 1, TimeUnit.SECONDS)) {
                        fullQueueWarn++;
                        if (fullQueueWarn < 4) {
                            TapLogger.info(TAG, "log queue is full, waiting...");
                        }
                    }
                    if (fullQueueWarn > 0) {
                        TapLogger.info(TAG, "log queue has been released!");
                        fullQueueWarn = 0;
                    }
                } catch (Exception ignored) {

                }
            }
        }
    }

    @Override
    public void updateOffset(Object offset) {
        try {
            while (!eventQueue.offer(new CustomEventMessage().tapEvent(null)
                    .contextMap(this.paper(offset)), 1, TimeUnit.SECONDS)) {
                fullQueueWarn++;
                if (fullQueueWarn < 4) {
                    TapLogger.info(TAG, "log queue is full, waiting...");
                }
            }
            if (fullQueueWarn > 0) {
                TapLogger.info(TAG, "log queue has been released!");
                fullQueueWarn = 0;
            }
        } catch (Exception ignored) {

        }
    }

    private Object paper(Object offset){
       return LoadJavaScripter.covertData(offset);
    }
}
