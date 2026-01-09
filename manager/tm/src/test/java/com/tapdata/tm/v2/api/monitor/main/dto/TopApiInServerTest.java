package com.tapdata.tm.v2.api.monitor.main.dto;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopApiInServerTest {
    @org.junit.jupiter.api.Test
    void testCreate() {
        TopApiInServer item = TopApiInServer.create();
        assertNotNull(item.getRequestCount());
        assertNotNull(item.getErrorRate());
        assertNotNull(item.getAvg());
    }
}