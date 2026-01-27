package com.tapdata.tm.worker.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see com.tapdata.tm.worker.entity.ServerUsage
 */
public enum ServerUsageField implements CollectionField {
    TYPE("type"),
    PROCESS_TYPE("processType"),
    PROCESS_ID("processId"),
    WORK_OID("workOid"),
    CPU_USAGE("cpuUsage"),
    SELF_CPU_USAGE("selfCpuUsage"),
    HEAP_MEMORY_MAX("heapMemoryMax"),
    HEAP_MEMORY_USAGE("heapMemoryUsage"),
    LAST_UPDATE_TIME("lastUpdateTime"),
    TTL_KEY("ttlKey");

    final String field;

    ServerUsageField(String field) {
        this.field = field;
    }

    @Override
    public String field() {
        return this.field;
    }
}
