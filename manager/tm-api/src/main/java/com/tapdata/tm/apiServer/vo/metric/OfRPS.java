package com.tapdata.tm.apiServer.vo.metric;

import lombok.Data;

@Data
public class OfRPS extends MetricDataBase {
    /**
     * Requests per second (RPS) , such as 100.0(unit: 1s)
     * */
    Double rps;
}