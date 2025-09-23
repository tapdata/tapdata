package com.tapdata.tm.apiServer.vo.metric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OfErrorRateTest {
    @Test
    void test() {
        OfErrorRate ofErrorRate = new OfErrorRate();
        ofErrorRate.setErrorRate(0.1d);
        Object[] values = ofErrorRate.values();
        Assertions.assertEquals(1, values.length);
        Assertions.assertEquals(0.1d, values[0]);
    }
}