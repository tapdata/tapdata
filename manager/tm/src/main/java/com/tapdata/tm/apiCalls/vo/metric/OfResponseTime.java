package com.tapdata.tm.apiCalls.vo.metric;

import lombok.Data;

@Data
public class OfResponseTime extends MetricDataBase {
    /**
     * percentile 50, millisecond
     * */
    Long p50;

    /**
     * percentile 95, millisecond
     * */
    Long p95;

    /**
     * percentile 99, millisecond
     * */
    Long p99;
}