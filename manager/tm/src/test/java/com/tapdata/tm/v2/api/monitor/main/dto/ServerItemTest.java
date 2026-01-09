package com.tapdata.tm.v2.api.monitor.main.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ServerItemTest {

    @Test
    void testCreate() {
        ServerItem item = ServerItem.create();
        assertNotNull(item);
        assertNotNull(item.getCpuUsage());
        assertNotNull(item.getMemoryUsage());
        assertNotNull(item.getTs());
    }
}