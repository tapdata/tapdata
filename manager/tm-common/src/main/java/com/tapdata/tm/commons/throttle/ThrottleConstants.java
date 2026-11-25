package com.tapdata.tm.commons.throttle;

/**
 * Shared constants for task-level throttling and continuous metering across TM, engine, and enterprise modules.
 */
public final class ThrottleConstants {

    private ThrottleConstants() {
    }

    public static final String METER_VERSION = "event-v1-full-first-cdc-table-dml-v1";
    public static final long BYTES_PER_MB = 1_000_000L;

    public static final String SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL = "cdcSampleRefreshIntervalSeconds";
    public static final int DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS = 30;
    public static final int MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS = 5;
    public static final int MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS = 300;

    public static final long DEFAULT_MAX_ATOMIC_SECONDS = 5L;
    public static final double HIGH_WATERMARK_RATIO = 0.80d;
    public static final double LOW_WATERMARK_RATIO = 0.60d;

    public static final long REALTIME_WAIT_REFRESH_SECONDS = 5L;
    public static final long REALTIME_WAIT_UNKNOWN_SECONDS = 15L;
    public static final long CLOCK_DRIFT_WARN_SECONDS = 2L;

    public static final long MUTATION_PROBE_INTERVAL_SECONDS = 5L;
    public static final int MAX_SAMPLES_PER_GROUP = 4;

    public static final String ERR_INVALID_RATE = "INVALID_RATE";
    public static final String ERR_UNSUPPORTED_TASK_KIND = "UNSUPPORTED_TASK_KIND";
    public static final String ERR_UNSUPPORTED_PATH = "UNSUPPORTED_PATH";
    public static final String ERR_VERSION_CONFLICT = "VERSION_CONFLICT";
    public static final String ERR_STOP_EVIDENCE_REQUIRED = "STOP_EVIDENCE_REQUIRED";
    public static final String ERR_STALE_RUN_EPOCH = "STALE_RUN_EPOCH";
}
