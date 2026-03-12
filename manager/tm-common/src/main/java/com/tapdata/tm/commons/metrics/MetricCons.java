package com.tapdata.tm.commons.metrics;

/**
 * 指标常量类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/11/27 22:17 Create
 */
public final class MetricCons {

    public enum QpsType {
        COUNT(0),
        MEMORY(1),
        ;
        private final int code;

        QpsType(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    public enum SampleType {
        ENGINE("engine"),
        TASK("task"),
        NODE("node"),
        TABLE("table"),
        ;
        private final String code;

        SampleType(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

        public boolean check(String type) {
            return code().equals(type);
        }
    }

    // fields basic
    public static final String F_ID = "_id";
    public static final String F_GRANULARITY = "grnty";
    public static final String F_DATE = "date";
    public static final String F_SIZE = "size";
    public static final String F_FIRST = "first";
    public static final String F_LAST = "last";
    public static final String F_TAGS = "tags";
    public static final String F_SAMPLES = "ss";
    public static final String F_DIGEST = "digest";

    // fields tags
    public static final class Tags {
        public static final String F_TYPE = "type";
        public static final String F_ENGINE_ID = "engineId";
        public static final String F_TASK_ID = "taskId";
        public static final String F_TASK_RECORD_ID = "taskRecordId";
        public static final String F_NODE_ID = "nodeId";
        public static final String F_TABLE = "table";

        public static String path(String field) {
            return F_TAGS + "." + field;
        }
    }

    public static final class SS {
        public static final String F_DATE = "date";
        public static final String F_VS = "vs";

        public static final class VS {

            public static final String F_QPS_TYPE = "qpsType";
            public static final String F_TIME_COST_AVG = "timeCostAvg";
            public static final String F_REPLICATE_LAG = "replicateLag";
            public static final String F_CURR_EVENT_TS = "currentEventTimestamp";
            public static final String F_TABLE_TOTAL = "tableTotal";
            public static final String F_CREATE_TABLE_TOTAL = "createTableTotal";

            // input
            public static final String F_INPUT_QPS = "inputQps";
            public static final String F_INPUT_SIZE_QPS = "inputSizeQps";
            public static final String F_INPUT_DDL_TOTAL = "inputDdlTotal";
            public static final String F_INPUT_INSERT_TOTAL = "inputInsertTotal";
            public static final String F_INPUT_UPDATE_TOTAL = "inputUpdateTotal";
            public static final String F_INPUT_DELETE_TOTAL = "inputDeleteTotal";
            public static final String F_INPUT_OTHERS_TOTAL = "inputOthersTotal";
            public static final String F_MAX_INPUT_QPS = "maxInputQps";
            public static final String F_MAX_INPUT_SIZE_QPS = "maxInputSizeQps";
            public static final String F_95TH_INPUT_QPS = "inputQps95th";
            public static final String F_95TH_INPUT_SIZE_QPS = "inputSizeQps95th";
            public static final String F_99TH_INPUT_QPS = "inputQps99th";
            public static final String F_99TH_INPUT_SIZE_QPS = "inputSizeQps99th";

            // output
            public static final String F_OUTPUT_QPS = "outputQps";
            public static final String F_OUTPUT_QPS_MAX = "outputQpsMax";
            public static final String F_OUTPUT_QPS_AVG = "outputQpsAvg";
            public static final String F_OUTPUT_SIZE_QPS = "outputSizeQps";
            public static final String F_OUTPUT_SIZE_QPS_MAX = "outputSizeQpsMax";
            public static final String F_OUTPUT_SIZE_QPS_AVG = "outputSizeQpsAvg";
            public static final String F_OUTPUT_DDL_TOTAL = "outputDdlTotal";
            public static final String F_OUTPUT_INSERT_TOTAL = "outputInsertTotal";
            public static final String F_OUTPUT_UPDATE_TOTAL = "outputUpdateTotal";
            public static final String F_OUTPUT_DELETE_TOTAL = "outputDeleteTotal";
            public static final String F_OUTPUT_OTHERS_TOTAL = "outputOthersTotal";
            public static final String F_MAX_OUTPUT_QPS = "maxOutputQps";
            public static final String F_MAX_OUTPUT_SIZE_QPS = "maxOutputSizeQps";
            public static final String F_BATCH_READ_SIZE = "batchReadSize"; // 源节点事件读取批大小
            public static final String F_BATCH_READ_TIMEOUT_MS = "batchReadTimeoutMs"; // 源节点事件攒批超时时长
            public static final String F_95TH_OUTPUT_QPS = "outputQps95th";
            public static final String F_95TH_OUTPUT_SIZE_QPS = "outputSizeQps95th";
            public static final String F_99TH_OUTPUT_QPS = "outputQps99th";
            public static final String F_99TH_OUTPUT_SIZE_QPS = "outputSizeQps99th";

            //replicateLag
            public static final String REPLICATE_LAG = "replicateLag";
            public static final String F_95TH_REPLICATE_LAG = "replicateLag95th";
            public static final String F_99TH_REPLICATE_LAG = "replicateLag99th";

            // snapshot
            public static final String F_SNAPSHOT_START_AT = "snapshotStartAt";
            public static final String F_SNAPSHOT_DONE_AT = "snapshotDoneAt";
            public static final String F_SNAPSHOT_DONE_COST = "snapshotDoneCost";
            public static final String F_SNAPSHOT_TABLE_TOTAL = "snapshotTableTotal";
            public static final String F_SNAPSHOT_ROW_TOTAL = "snapshotRowTotal";
            public static final String F_SNAPSHOT_INSERT_ROW_TOTAL = "snapshotInsertRowTotal";
            public static final String F_SNAPSHOT_SYNC_RATE = "snapshotSyncRate";
            public static final String F_SNAPSHOT_ERROR_SKIPPED = "snapshotErrorSkipped";
            public static final String F_CURR_SNAPSHOT_TABLE = "currentSnapshotTable";
            public static final String F_CURR_SNAPSHOT_TABLE_ROW_TOTAL = "currentSnapshotTableRowTotal";
            public static final String F_CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL = "currentSnapshotTableInsertRowTotal";

            // node
            public static final String F_SNAPSHOT_SOURCE_READ_TIME_COST_AVG = "snapshotSourceReadTimeCostAvg";
            public static final String F_INCR_SOURCE_READ_TIME_COST_AVG = "incrementalSourceReadTimeCostAvg";
            public static final String F_TARGET_WRITE_TIME_COST_AVG = "targetWriteTimeCostAvg";

            // monitor
            public static final String F_CPU_USAGE = "cpuUsage";
            public static final String F_MEMORY_USAGE = "memoryUsage";

            public static String path(String field) {
                return F_SAMPLES + "." + F_VS + "." + field;
            }
        }

        public static String path(String field) {
            return F_SAMPLES + "." + field;
        }
    }
}
