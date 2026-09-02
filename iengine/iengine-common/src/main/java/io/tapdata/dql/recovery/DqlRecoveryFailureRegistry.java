package io.tapdata.dql.recovery;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Process-local bridge from Engine capture boundaries to the active recovery
 * barrier. The common module owns the bridge so the capture module does not
 * depend on the Engine application module.
 */
public final class DqlRecoveryFailureRegistry {
    private static final ConcurrentMap<String, Consumer<Throwable>> LISTENERS = new ConcurrentHashMap<>();

    private DqlRecoveryFailureRegistry() {
    }

    public static void register(String eventId, Consumer<Throwable> listener) {
        validateEventId(eventId);
        Objects.requireNonNull(listener, "listener must not be null");
        if (LISTENERS.putIfAbsent(eventId, listener) != null) {
            throw new IllegalStateException("recovery failure listener already exists for event " + eventId);
        }
    }

    public static void unregister(String eventId) {
        if (StringUtils.isNotBlank(eventId)) {
            LISTENERS.remove(eventId);
        }
    }

    /**
     * Delivers one failure to the active attempt. Removing before invoking
     * the callback makes duplicate capture paths harmless and bounds the
     * listener lifetime even when a callback implementation fails.
     */
    public static boolean fail(String eventId, Throwable failure) {
        if (StringUtils.isBlank(eventId)) {
            return false;
        }
        Consumer<Throwable> listener = LISTENERS.remove(eventId);
        if (listener == null) {
            return false;
        }
        try {
            listener.accept(failure);
        } catch (RuntimeException ignored) {
            // The original capture error must continue through its task path.
        }
        return true;
    }

    private static void validateEventId(String eventId) {
        if (StringUtils.isBlank(eventId)) {
            throw new IllegalArgumentException("recovery eventId must not be blank");
        }
    }
}
