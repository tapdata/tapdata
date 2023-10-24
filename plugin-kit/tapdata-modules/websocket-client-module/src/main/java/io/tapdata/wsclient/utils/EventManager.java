package io.tapdata.wsclient.utils;

import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressWarnings("ALL")
public class EventManager {

    private static final String TAG = EventManager.class.getSimpleName();

    public interface EventListener<T> {
        void onMessage(String eventType, T message);
    }

    public static class EventHolder {
        public EventHolder(EventListener<?> eventListener, String type) {
            this.eventListener = eventListener;
            this.type = type;
        }
        EventListener<?> eventListener;
        String type;
    }

    private final ConcurrentHashMap<String, List<EventListener>> listenerMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Object, List<EventHolder>> keyMap = new ConcurrentHashMap<>();

    private static EventManager instance;

    private EventManager() {

    }

    public static synchronized EventManager getInstance() {
        if (instance == null) {
            instance = new EventManager();
        }
        return instance;
    }

    List<EventListener> getOrCreateListeners(String eventType) {
        List<EventListener> listeners = listenerMap.get(eventType);
        if (listeners == null) {
            listeners = new CopyOnWriteArrayList<>();
            List<EventListener> theListeners = listenerMap.putIfAbsent(eventType, listeners);
            if(theListeners != null) {
                listeners = theListeners;
            }
        }
        return listeners;
    }

    List<EventHolder> getOrCreateKeyMap(Object key) {
        List<EventHolder> listeners = keyMap.get(key);
        if (listeners == null) {
            listeners = new CopyOnWriteArrayList<>();
            List<EventHolder> theListeners = keyMap.putIfAbsent(key, listeners);
            if(theListeners != null) {
                listeners = theListeners;
            }
        }
        return listeners;
    }

    private final Object mObject = new Object();

    public void sendEvent(String eventType, Object message) {
        synchronized (mObject) {
            List<EventListener> listeners = getOrCreateListeners(eventType);
            for (EventListener listener : listeners) {
                try {
                    listener.onMessage(eventType, message);
                } catch (Throwable throwable) {
                    TapLogger.error(TAG, "Event onMessage failed, for eventType {}, eventListener {}, message {} error {}", eventType, listener, message, ExceptionUtils.getStackTrace(throwable));
                }
            }
        }

    }

    public synchronized <T> void registerEventListener(String eventType, EventListener<T> listener) {
        List<EventListener> listeners = getOrCreateListeners(eventType);
        listeners.add(listener);
    }

    public synchronized void unregisterEventListener(String eventType, EventListener listener) {
        List<EventListener> listeners = getOrCreateListeners(eventType);
        listeners.remove(listener);
    }

    public synchronized <T> void registerEventListener(Object key, String eventType, EventListener<T> listener) {
        List<EventListener> listeners = getOrCreateListeners(eventType);
        listeners.add(listener);
        List<EventHolder> keyListeners = getOrCreateKeyMap(key);
        keyListeners.add(new EventHolder(listener, eventType));
    }

    public synchronized void unregisterEventListener(Object key) {
        List<EventHolder> listeners = keyMap.remove(key);
        if(listeners != null) {
            for(EventHolder eventHolder : listeners) {
                unregisterEventListener(eventHolder.type, eventHolder.eventListener);
            }
        }
    }
}
