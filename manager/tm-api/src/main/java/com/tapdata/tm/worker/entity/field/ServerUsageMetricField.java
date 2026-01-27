package com.tapdata.tm.worker.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see ServerUsageMetricField
 * */
public enum ServerUsageMetricField implements CollectionField {
    TIME_GRANULARITY("timeGranularity"),
    MAX_CPU_USAGE("maxCpuUsage"),
    MIN_CPU_USAGE("minCpuUsage"),
    MIN_HEAP_MEMORY_USAGE("minHeapMemoryUsage"),
    MAX_HEAP_MEMORY_USAGE("maxHeapMemoryUsage");

    final String field;
    ServerUsageMetricField(String field) {
        this.field = field;
    }
    @Override
    public String field() {
        return this.field;
    }
}
