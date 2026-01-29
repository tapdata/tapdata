package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiMetricsRawMergeServiceTest {
    ApiMetricsRawMergeService service;
    @BeforeEach
    void init() {
        service = mock(ApiMetricsRawMergeService.class);
        when(service.errorCountGetter(anyList(), any(LongConsumer.class))).thenCallRealMethod();
    }

    @Nested
    class ErrorCountGetterTest {
        @Test
        void testErrorCountGetter() {
            ApiMetricsRaw raw1 = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server1", 200L, 20L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw1, raw2);

            List<Long> requestCounts = new ArrayList<>();
            long errorCount = service.errorCountGetter(raws, requestCounts::add);

            assertEquals(30L, errorCount);
            assertEquals(2, requestCounts.size());
            assertTrue(requestCounts.contains(100L));
            assertTrue(requestCounts.contains(200L));
        }

        @Test
        void testErrorCountGetterWithNull() {
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", null, null);
            List<ApiMetricsRaw> raws = Arrays.asList(null, raw);

            List<Long> requestCounts = new ArrayList<>();
            long errorCount = service.errorCountGetter(raws, requestCounts::add);

            assertEquals(0L, errorCount);
            assertEquals(1, requestCounts.size());
            assertTrue(requestCounts.contains(0L));
        }
    }

    // Helper methods
    private ApiMetricsRaw createApiMetricsRaw(String apiId, String processId, Long reqCount, Long errorCount) {
        ApiMetricsRaw raw = new ApiMetricsRaw();
        raw.setApiId(apiId);
        raw.setProcessId(processId);
        raw.setReqCount(reqCount);
        raw.setErrorCount(errorCount);
        raw.setTimeStart(System.currentTimeMillis());
        raw.setDelay(new ArrayList<>());
        raw.setBytes(new ArrayList<>());
        raw.setRps(10.0);
        return raw;
    }
}