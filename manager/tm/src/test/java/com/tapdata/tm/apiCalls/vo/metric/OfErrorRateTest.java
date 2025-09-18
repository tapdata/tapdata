package com.tapdata.tm.apiCalls.vo.metric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OfErrorRateTest {
    @Test
    void test() {
        OfErrorRate ofErrorRate = new OfErrorRate();
        ofErrorRate.setErrorRate(0.1d);
        Object[] values = ofErrorRate.values();
        assertEquals(1, values.length);
        assertEquals(0.1d, values[0]);
    }
}