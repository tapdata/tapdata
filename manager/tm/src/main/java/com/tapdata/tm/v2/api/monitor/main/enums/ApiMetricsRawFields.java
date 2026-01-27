package com.tapdata.tm.v2.api.monitor.main.enums;

import com.tapdata.tm.base.field.CollectionField;

public enum ApiMetricsRawFields implements CollectionField {
    PROCESS_ID("processId"),
    API_ID("apiId"),
    METRIC_TYPE("metricType"),
    TIME_GRANULARITY("timeGranularity"),
    TIME_START("timeStart"),
    REQ_COUNT("reqCount"),
    ERROR_COUNT("errorCount"),
    RPS("rps"),
    BYTES("bytes"),
    DELAY("delay"),
    DB_COST("dbCost"),
    SUB_METRICS("subMetrics"),
    P50("p50"),
    P95("p95"),
    P99("p99"),
    MAX_DELAY("maxDelay"),
    MIN_DELAY("minDelay"),
    WORKER_INFO_MAP("workerInfoMap"),
    LAST_CALL_ID("lastCallId"),
    TTL_KEY("ttlKey"),

    ;
    final String fieldName;

    ApiMetricsRawFields(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String field() {
        return this.fieldName;
    }
}
