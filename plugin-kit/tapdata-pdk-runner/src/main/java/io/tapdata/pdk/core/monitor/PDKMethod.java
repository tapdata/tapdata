package io.tapdata.pdk.core.monitor;

import java.util.concurrent.TimeUnit;

public enum PDKMethod {
    REGISTER_CAPABILITIES(TimeUnit.SECONDS.toMillis(3)),

    PROCESSOR_FUNCTIONS(TimeUnit.SECONDS.toMillis(3)),

    DISCOVER_SCHEMA,
    CONNECTION_TEST,
//    DESTROY,
    RELEASE_EXTERNAL,
    CONTROL,
    INIT,
    BATCH_OFFSET,
    STREAM_OFFSET,
    TABLE_COUNT,
    SOURCE_CONNECTION_TEST(TimeUnit.SECONDS.toMillis(10)),
    TARGET_CONNECTION_TEST(TimeUnit.SECONDS.toMillis(10)),

    SOURCE_BATCH_COUNT(TimeUnit.SECONDS.toMillis(30)),

    SOURCE_BATCH_READ,
    SOURCE_BATCH_OFFSET,
    SOURCE_QUERY_BY_FILTER,
    SOURCE_QUERY_BY_ADVANCE_FILTER,

    TARGET_INSERT(TimeUnit.SECONDS.toMillis(10)),

    TARGET_WRITE_RECORD(TimeUnit.SECONDS.toMillis(10)),

    PROCESSOR_PROCESS_RECORD(TimeUnit.SECONDS.toMillis(10)),
    TARGET_DROP_TABLE,
    TARGET_CLEAR_TABLE,
    TARGET_CREATE_TABLE,
    TARGET_ALTER_TABLE,
    SOURCE_STREAM_READ,
    TARGET_CREATE_INDEX,
    STOP,
    TIMESTAMP_TO_STREAM_OFFSET,
    ;

    PDKMethod() {

    }
    PDKMethod(Long warnMilliseconds) {
        this.warnMilliseconds = warnMilliseconds;
    }
    private Long warnMilliseconds;
}
