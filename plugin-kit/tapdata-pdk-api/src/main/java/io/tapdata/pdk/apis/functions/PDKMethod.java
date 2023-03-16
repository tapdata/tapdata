package io.tapdata.pdk.apis.functions;

import java.util.concurrent.TimeUnit;

public enum PDKMethod {
    REGISTER_CAPABILITIES,

    PROCESSOR_FUNCTIONS,

    DISCOVER_SCHEMA,
    CONNECTION_TEST,
//    DESTROY,
    RELEASE_EXTERNAL,
    CONTROL,
    INIT,
    BATCH_OFFSET,
    STREAM_OFFSET,
    TABLE_COUNT,
    SOURCE_CONNECTION_TEST,
    TARGET_CONNECTION_TEST,

    SOURCE_BATCH_COUNT,

    SOURCE_BATCH_READ,
    SOURCE_GET_READ_PARTITIONS,
    SOURCE_BATCH_OFFSET,
    SOURCE_QUERY_BY_FILTER,
    SOURCE_QUERY_BY_ADVANCE_FILTER,

    TARGET_INSERT,

    TARGET_WRITE_RECORD,

    PROCESSOR_PROCESS_RECORD,
    TARGET_DROP_TABLE,
    TARGET_CLEAR_TABLE,
    TARGET_CREATE_TABLE,
    TARGET_ALTER_TABLE,
    SOURCE_STREAM_READ,
    TARGET_CREATE_INDEX,
    STOP,
    TIMESTAMP_TO_STREAM_OFFSET,
    NEW_FIELD,
    ALTER_FIELD_NAME,
    ALTER_FIELD_ATTRIBUTES,
    GET_TABLE_NAMES,
    CONNECTION_CHECK,
    COMMAND_CALLBACK,
    GET_TABLE_INFO,
    CHECK_TABLE_NAME,
    DROP_FIELD,
    MEMORY_FETCHER,
    COUNT_BY_PARTITION_FILTER,
    QUERY_FIELD_MIN_MAX_VALUE,
    RUN_RAW_COMMAND,
    RAW_DATA_CALLBACK_FILTER;
    PDKMethod() {

    }
    PDKMethod(Long warnMilliseconds) {
        this.warnMilliseconds = warnMilliseconds;
    }
    private Long warnMilliseconds;
}
