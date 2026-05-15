package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.metrics.MetricCons;

/**
 * 替代方案 {@link MetricCons.SS.VS}（统一管理指标属性）
 * @deprecated since release-v4.12.0
 * @author Dexter
 */
@Deprecated(since = "release-v4.12.0", forRemoval = true)
public class Constants {
    static final String INPUT_DDL_TOTAL     = "inputDdlTotal";
    static final String INPUT_INSERT_TOTAL  = "inputInsertTotal";
    static final String INPUT_UPDATE_TOTAL  = "inputUpdateTotal";
    static final String INPUT_DELETE_TOTAL  = "inputDeleteTotal";
    static final String INPUT_OTHERS_TOTAL  = "inputOthersTotal";

    static final String OUTPUT_DDL_TOTAL    = "outputDdlTotal";
    static final String OUTPUT_INSERT_TOTAL = "outputInsertTotal";
    static final String OUTPUT_UPDATE_TOTAL = "outputUpdateTotal";
    static final String OUTPUT_DELETE_TOTAL = "outputDeleteTotal";
    static final String OUTPUT_OTHERS_TOTAL = "outputOthersTotal";

    static final String INPUT_QPS           = "inputQps";
    static final String OUTPUT_QPS          = "outputQps";
    static final String TIME_COST_AVG       = "timeCostAvg";
    static final String REPLICATE_LAG       = "replicateLag";
    static final String CURR_EVENT_TS       = "currentEventTimestamp";

    static final String QPS_TYPE            = "qpsType";
    static final int QPS_TYPE_MEMORY        = 1;
    static final int QPS_TYPE_COUNT         = 0;
    static final String INPUT_SIZE_QPS      = "inputSizeQps";
    static final String OUTPUT_SIZE_QPS     = "outputSizeQps";
    static final String OUTPUT_SIZE_QPS_MAX = "outputSizeQpsMax";
    static final String OUTPUT_SIZE_QPS_AVG = "outputSizeQpsAvg";

    static final String CPU_USAGE           = "cpuUsage";
    static final String MEMORY_USAGE        = "memoryUsage";

    static final String BATCH_READ_SIZE     = "batchReadSize";
    static final String INTERVAL_MS        = "intervalMs";
}
