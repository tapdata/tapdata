package com.tapdata.tm.v2.api.monitor.main.dto;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApiItemTest {

    @Test
    void testNormal() {
        ApiItem item = ApiItem.create();
        Assertions.assertEquals(0L, item.getRequestCount());
        Assertions.assertEquals(0.0D, item.getErrorRate());
        Assertions.assertEquals(0L, item.getErrorCount());
        Assertions.assertEquals(0.0D, item.getResponseTimeAvg());
    }
}