package com.tapdata.tm.apiServer.vo.metric;

import lombok.Data;

@Data
public class OfErrorRate extends MetricDataBase {
    /**
     * error rate, such as 0.1 means 10%
     * */
    Double errorRate;

    @Override
    public Object[] values() {
        return new Object[] {errorRate};
    }
}
