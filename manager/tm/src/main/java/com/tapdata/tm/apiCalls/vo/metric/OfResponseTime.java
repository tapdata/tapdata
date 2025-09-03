package com.tapdata.tm.apiCalls.vo.metric;

import lombok.Data;

@Data
public class OfResponseTime extends MetricDataBase {
    Long p50;
    Long p95;
    Long p99;
}