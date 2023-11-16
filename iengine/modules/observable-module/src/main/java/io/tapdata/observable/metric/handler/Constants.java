package io.tapdata.observable.metric.handler;

/**
 * @author Dexter
 */
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
}
