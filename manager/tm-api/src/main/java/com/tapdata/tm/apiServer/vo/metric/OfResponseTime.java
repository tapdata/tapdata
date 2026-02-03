package com.tapdata.tm.apiServer.vo.metric;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;

@Data
public class OfResponseTime extends MetricDataBase {
    /**
     * percentile 50, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    Double p50;

    /**
     * percentile 95, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    Double p95;

    /**
     * percentile 99, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    Double p99;
}