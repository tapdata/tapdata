package com.tapdata.tm.dql.config;

import java.util.Locale;
import java.util.Map;

/**
 * Validated DQL runtime settings shared by TM and Engine.
 *
 * <p>Settings are read as strings at the boundary. A JVM property wins over
 * an environment variable, which wins over the persisted Settings value;
 * every malformed or unsafe value falls back to the frozen POC default.</p>
 */
public final class DqlRuntimeConfig {
    public static final String EVENT_ENABLED = "dql.event.enabled";
    public static final String ERROR_DETAILS_MAX_LENGTH = "dql.event.errorDetails.maxLength";
    public static final String PAYLOAD_MAX_BYTES = "dql.event.payload.maxBytes";
    public static final String PREVIEW_FIELD_MAX_LENGTH = "dql.event.preview.fieldMaxLength";
    public static final String PREVIEW_MAX_DEPTH = "dql.event.preview.maxDepth";
    public static final String PREVIEW_MAX_ITEMS = "dql.event.preview.maxItems";
    public static final String RECOVERY_BATCH_MAX_SIZE = "dql.recovery.batch.maxSize";
    public static final String RECOVERY_EVENT_TIMEOUT_SECONDS = "dql.recovery.eventTimeoutSeconds";
    public static final String RECOVERY_BATCH_TIMEOUT_SECONDS = "dql.recovery.batchTimeoutSeconds";
    public static final String RECOVERY_CONTINUE_ON_EVENT_FAILURE = "dql.recovery.continueOnEventFailure";
    public static final String UNKNOWN_GUARD_WINDOW_SECONDS = "dql.unknown.guard.windowSeconds";
    public static final String UNKNOWN_GUARD_MAX_EVENTS = "dql.unknown.guard.maxEvents";
    public static final String UNKNOWN_GUARD_MAX_BATCH_RATIO = "dql.unknown.guard.maxBatchRatio";
    public static final String UNKNOWN_GUARD_DECISION = "dql.unknown.guard.decision";

    public static final boolean DEFAULT_EVENT_ENABLED = true;
    public static final int DEFAULT_ERROR_DETAILS_MAX_LENGTH = 4_000;
    public static final long DEFAULT_PAYLOAD_MAX_BYTES = 1_048_576L;
    public static final int DEFAULT_PREVIEW_FIELD_MAX_LENGTH = 512;
    public static final int DEFAULT_PREVIEW_MAX_DEPTH = 4;
    public static final int DEFAULT_PREVIEW_MAX_ITEMS = 50;
    public static final int DEFAULT_RECOVERY_BATCH_MAX_SIZE = 200;
    public static final long DEFAULT_RECOVERY_EVENT_TIMEOUT_SECONDS = 60L;
    public static final long DEFAULT_RECOVERY_BATCH_TIMEOUT_SECONDS = 1_800L;
    public static final boolean DEFAULT_RECOVERY_CONTINUE_ON_EVENT_FAILURE = true;
    public static final long DEFAULT_UNKNOWN_GUARD_WINDOW_SECONDS = 60L;
    public static final long DEFAULT_UNKNOWN_GUARD_MAX_EVENTS = 20L;
    public static final double DEFAULT_UNKNOWN_GUARD_MAX_BATCH_RATIO = 0.2d;
    public static final String DEFAULT_UNKNOWN_GUARD_DECISION = "TASK_RETRY";

    private static final int MAX_ERROR_DETAILS_LENGTH = 1_000_000;
    private static final long MAX_PAYLOAD_BYTES = 64L * 1024L * 1024L;
    private static final int MAX_PREVIEW_DEPTH = 32;
    private static final int MAX_PREVIEW_ITEMS = 10_000;
    private static final int MAX_RECOVERY_BATCH_SIZE = 10_000;
    private static final long MAX_TIMEOUT_SECONDS = Long.MAX_VALUE / 1_000L;
    private static final long MAX_GUARD_EVENTS = 10_000_000L;

    @FunctionalInterface
    public interface ValueSource {
        String get(String key);
    }

    private final boolean eventEnabled;
    private final int errorDetailsMaxLength;
    private final long payloadMaxBytes;
    private final int previewFieldMaxLength;
    private final int previewMaxDepth;
    private final int previewMaxItems;
    private final int recoveryBatchMaxSize;
    private final long recoveryEventTimeoutSeconds;
    private final long recoveryBatchTimeoutSeconds;
    private final boolean recoveryContinueOnEventFailure;
    private final long unknownGuardWindowSeconds;
    private final long unknownGuardMaxEvents;
    private final double unknownGuardMaxBatchRatio;
    private final String unknownGuardDecision;

    private DqlRuntimeConfig(boolean eventEnabled,
                             int errorDetailsMaxLength,
                             long payloadMaxBytes,
                             int previewFieldMaxLength,
                             int previewMaxDepth,
                             int previewMaxItems,
                             int recoveryBatchMaxSize,
                             long recoveryEventTimeoutSeconds,
                             long recoveryBatchTimeoutSeconds,
                             boolean recoveryContinueOnEventFailure,
                             long unknownGuardWindowSeconds,
                             long unknownGuardMaxEvents,
                             double unknownGuardMaxBatchRatio,
                             String unknownGuardDecision) {
        this.eventEnabled = eventEnabled;
        this.errorDetailsMaxLength = errorDetailsMaxLength;
        this.payloadMaxBytes = payloadMaxBytes;
        this.previewFieldMaxLength = previewFieldMaxLength;
        this.previewMaxDepth = previewMaxDepth;
        this.previewMaxItems = previewMaxItems;
        this.recoveryBatchMaxSize = recoveryBatchMaxSize;
        this.recoveryEventTimeoutSeconds = recoveryEventTimeoutSeconds;
        this.recoveryBatchTimeoutSeconds = recoveryBatchTimeoutSeconds;
        this.recoveryContinueOnEventFailure = recoveryContinueOnEventFailure;
        this.unknownGuardWindowSeconds = unknownGuardWindowSeconds;
        this.unknownGuardMaxEvents = unknownGuardMaxEvents;
        this.unknownGuardMaxBatchRatio = unknownGuardMaxBatchRatio;
        this.unknownGuardDecision = unknownGuardDecision;
    }

    public static DqlRuntimeConfig defaults() {
        return new DqlRuntimeConfig(
                DEFAULT_EVENT_ENABLED,
                DEFAULT_ERROR_DETAILS_MAX_LENGTH,
                DEFAULT_PAYLOAD_MAX_BYTES,
                DEFAULT_PREVIEW_FIELD_MAX_LENGTH,
                DEFAULT_PREVIEW_MAX_DEPTH,
                DEFAULT_PREVIEW_MAX_ITEMS,
                DEFAULT_RECOVERY_BATCH_MAX_SIZE,
                DEFAULT_RECOVERY_EVENT_TIMEOUT_SECONDS,
                DEFAULT_RECOVERY_BATCH_TIMEOUT_SECONDS,
                DEFAULT_RECOVERY_CONTINUE_ON_EVENT_FAILURE,
                DEFAULT_UNKNOWN_GUARD_WINDOW_SECONDS,
                DEFAULT_UNKNOWN_GUARD_MAX_EVENTS,
                DEFAULT_UNKNOWN_GUARD_MAX_BATCH_RATIO,
                DEFAULT_UNKNOWN_GUARD_DECISION);
    }

    public static DqlRuntimeConfig fromSystemProperties() {
        return from(null);
    }

    public static DqlRuntimeConfig fromMap(Map<String, ?> values) {
        return from(key -> {
            if (values == null || !values.containsKey(key) || values.get(key) == null) {
                return null;
            }
            return String.valueOf(values.get(key));
        });
    }

    public static DqlRuntimeConfig from(ValueSource settings) {
        ValueSource source = settings == null ? key -> null : settings;
        return new DqlRuntimeConfig(
                booleanValue(value(EVENT_ENABLED, source), DEFAULT_EVENT_ENABLED),
                intValue(value(ERROR_DETAILS_MAX_LENGTH, source), DEFAULT_ERROR_DETAILS_MAX_LENGTH,
                        1, MAX_ERROR_DETAILS_LENGTH),
                longValue(value(PAYLOAD_MAX_BYTES, source), DEFAULT_PAYLOAD_MAX_BYTES,
                        1L, MAX_PAYLOAD_BYTES),
                intValue(value(PREVIEW_FIELD_MAX_LENGTH, source), DEFAULT_PREVIEW_FIELD_MAX_LENGTH,
                        1, MAX_ERROR_DETAILS_LENGTH),
                intValue(value(PREVIEW_MAX_DEPTH, source), DEFAULT_PREVIEW_MAX_DEPTH,
                        0, MAX_PREVIEW_DEPTH),
                intValue(value(PREVIEW_MAX_ITEMS, source), DEFAULT_PREVIEW_MAX_ITEMS,
                        1, MAX_PREVIEW_ITEMS),
                intValue(value(RECOVERY_BATCH_MAX_SIZE, source), DEFAULT_RECOVERY_BATCH_MAX_SIZE,
                        1, MAX_RECOVERY_BATCH_SIZE),
                longValue(value(RECOVERY_EVENT_TIMEOUT_SECONDS, source),
                        DEFAULT_RECOVERY_EVENT_TIMEOUT_SECONDS, 1L, MAX_TIMEOUT_SECONDS),
                longValue(value(RECOVERY_BATCH_TIMEOUT_SECONDS, source),
                        DEFAULT_RECOVERY_BATCH_TIMEOUT_SECONDS, 1L, MAX_TIMEOUT_SECONDS),
                booleanValue(value(RECOVERY_CONTINUE_ON_EVENT_FAILURE, source),
                        DEFAULT_RECOVERY_CONTINUE_ON_EVENT_FAILURE),
                longValue(value(UNKNOWN_GUARD_WINDOW_SECONDS, source),
                        DEFAULT_UNKNOWN_GUARD_WINDOW_SECONDS, 1L, MAX_TIMEOUT_SECONDS),
                longValue(value(UNKNOWN_GUARD_MAX_EVENTS, source),
                        DEFAULT_UNKNOWN_GUARD_MAX_EVENTS, 1L, MAX_GUARD_EVENTS),
                doubleValue(value(UNKNOWN_GUARD_MAX_BATCH_RATIO, source),
                        DEFAULT_UNKNOWN_GUARD_MAX_BATCH_RATIO, 0d, 1d),
                decisionValue(value(UNKNOWN_GUARD_DECISION, source), DEFAULT_UNKNOWN_GUARD_DECISION));
    }

    private static String value(String key, ValueSource settings) {
        String value = systemProperty(key);
        if (isBlank(value)) {
            value = environment(key);
        }
        if (isBlank(value)) {
            value = settings.get(key);
        }
        return isBlank(value) ? null : value.trim();
    }

    private static String systemProperty(String key) {
        try {
            return System.getProperty(key);
        } catch (SecurityException ignored) {
            return null;
        }
    }

    private static String environment(String key) {
        String normalized = key.toUpperCase(Locale.ROOT).replace('.', '_');
        try {
            String exact = System.getenv(key);
            return isBlank(exact) ? System.getenv(normalized) : exact;
        } catch (SecurityException ignored) {
            return null;
        }
    }

    private static boolean booleanValue(String value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        return fallback;
    }

    private static int intValue(String value, int fallback, int minimum, int maximum) {
        if (value == null) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            return parsed >= minimum && parsed <= maximum ? (int) parsed : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static long longValue(String value, long fallback, long minimum, long maximum) {
        if (value == null) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            return parsed >= minimum && parsed <= maximum ? parsed : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static double doubleValue(String value, double fallback, double minimum, double maximum) {
        if (value == null) {
            return fallback;
        }
        try {
            double parsed = Double.parseDouble(value);
            return !Double.isNaN(parsed) && !Double.isInfinite(parsed)
                    && parsed >= minimum && parsed <= maximum ? parsed : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static String decisionValue(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String normalized = value.toUpperCase(Locale.ROOT);
        return "TASK_RETRY".equals(normalized) || "TASK_ERROR".equals(normalized)
                ? normalized : fallback;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public boolean isEventEnabled() {
        return eventEnabled;
    }

    public int getErrorDetailsMaxLength() {
        return errorDetailsMaxLength;
    }

    public long getPayloadMaxBytes() {
        return payloadMaxBytes;
    }

    public int getPreviewFieldMaxLength() {
        return previewFieldMaxLength;
    }

    public int getPreviewMaxDepth() {
        return previewMaxDepth;
    }

    public int getPreviewMaxItems() {
        return previewMaxItems;
    }

    public int getRecoveryBatchMaxSize() {
        return recoveryBatchMaxSize;
    }

    public long getRecoveryEventTimeoutSeconds() {
        return recoveryEventTimeoutSeconds;
    }

    public long getRecoveryBatchTimeoutSeconds() {
        return recoveryBatchTimeoutSeconds;
    }

    public boolean isRecoveryContinueOnEventFailure() {
        return recoveryContinueOnEventFailure;
    }

    public long getUnknownGuardWindowSeconds() {
        return unknownGuardWindowSeconds;
    }

    public long getUnknownGuardMaxEvents() {
        return unknownGuardMaxEvents;
    }

    public double getUnknownGuardMaxBatchRatio() {
        return unknownGuardMaxBatchRatio;
    }

    public String getUnknownGuardDecision() {
        return unknownGuardDecision;
    }
}
