package com.tapdata.tm.worker.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see com.tapdata.tm.worker.entity.ConnectionPoolEntity
 */
public enum ConnectionPoolField implements CollectionField {
    CONNECTION_ID("connectionId"),
    TIME_GRANULARITY("timeGranularity"),
    PROCESS_ID("processId"),
    LAST_UPDATE_TIME("lastUpdateTime"),
    DATA_TYPE("dataType"),
    MAX_CONNECTIONS("maxConnections"),
    USED_CONNECTIONS("usedConnections"),
    AVAILABLE("available"),
    QUEUE_SIZE("queueSize"),
    TTL_KEY("ttlKey");

    final String field;

    ConnectionPoolField(String field) {
        this.field = field;
    }

    @Override
    public String field() {
        return this.field;
    }
}
