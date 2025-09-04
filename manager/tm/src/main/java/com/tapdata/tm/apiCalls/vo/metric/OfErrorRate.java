package com.tapdata.tm.apiCalls.vo.metric;

import lombok.Data;

@Data
public class OfErrorRate extends MetricDataBase {
    /**
     * error rate, such as 0.1 means 10%
     * */
    Double errorRate;
}
