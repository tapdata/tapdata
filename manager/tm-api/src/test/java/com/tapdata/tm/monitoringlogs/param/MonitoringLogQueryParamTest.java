package com.tapdata.tm.monitoringlogs.param;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class MonitoringLogQueryParamTest {
    @Test
    void test_getFullLevels() {
        MonitoringLogQueryParam param = new MonitoringLogQueryParam();
        List<String> expect = Arrays.asList(
                "DEBUG", "INFO", "WARN", "ERROR","TRACE"
        );
        Assertions.assertEquals(expect, param.getFullLevels());
    }
}
