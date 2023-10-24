package io.tapdata.wsserver.eventbus;

import com.google.common.eventbus.EventBus;
import io.tapdata.entity.logger.TapLogger;

public class EventBusHolder {
    private static final String TAG = EventBusHolder.class.getSimpleName();
    private static EventBus eventBus;
    static {
        eventBus = new EventBus((throwable, subscriberExceptionContext) -> {
            TapLogger.error(TAG, "EventBus occurred uncaught error {} type {} context {}", throwable.getMessage(), throwable.getClass().getSimpleName(), subscriberExceptionContext);
        });
    }

    public static EventBus getEventBus() {
        return eventBus;
    }
}