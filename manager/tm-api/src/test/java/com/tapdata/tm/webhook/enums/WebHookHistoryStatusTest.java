package com.tapdata.tm.webhook.enums;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WebHookHistoryStatusTest {
    @Test
    void testParams() {
        WebHookHistoryStatus[] values = WebHookHistoryStatus.values();
        Assertions.assertEquals(3, values.length);
    }
    @Test
    void testFAILED() {
        Assertions.assertEquals("FAILED", WebHookHistoryStatus.FAILED.name());
    }
    @Test
    void testING() {
        Assertions.assertEquals("ING", WebHookHistoryStatus.ING.name());
    }
    @Test
    void testSUCCEED() {
        Assertions.assertEquals("SUCCEED", WebHookHistoryStatus.SUCCEED.name());
    }
}