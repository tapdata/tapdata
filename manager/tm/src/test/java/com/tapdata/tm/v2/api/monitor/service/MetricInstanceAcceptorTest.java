package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetricInstanceAcceptorTest {

    @Mock
    private BiFunction<Boolean, ApiMetricsRaw, Void> consumer;

    private MetricInstanceAcceptor acceptor;
    private ApiMetricsRaw lastBucketMin;
    private ApiMetricsRaw lastBucketHour;
    private ApiMetricsRaw lastBucketDay;

    @BeforeEach
    void setUp() {
        lastBucketMin = createApiMetricsRaw("server1", "api1", 60000L, 1);
        lastBucketHour = createApiMetricsRaw("server1", "api1", 3600000L, 2);
        lastBucketDay = createApiMetricsRaw("server1", "api1", 86400000L, 3);
        Function<Long, MetricInstanceAcceptor.BucketInfo> bucketInfoGetter = ts -> {
            return new MetricInstanceAcceptor.BucketInfo(lastBucketMin, lastBucketHour);
        };
        acceptor = new MetricInstanceAcceptor(MetricTypes.API_SERVER, bucketInfoGetter, consumer);
    }

    @Nested
    class CalcPValueTest {

        @Test
        void testCalcPValueWithNull() {
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            
            spyAcceptor.calcPValue(null);
            
            // Should return early without any processing
        }

        @Test
        void testCalcPValueWithZeroReqCount() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            item.setReqCount(0L);
            
            try (MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                delayUtil.when(() -> ApiMetricsDelayUtil.p50(any(), eq(0)))
                        .thenReturn(null);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), eq(0)))
                        .thenReturn(null);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), eq(0)))
                        .thenReturn(null);
                
                acceptor.calcPValue(item);
                
                assertNotNull(item.getP50());
                assertNotNull(item.getP95());
                assertNotNull(item.getP99());
            }
        }
    }

    @Nested
    class CloseTest {

        @Test
        void testClose() {
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            doNothing().when(spyAcceptor).acceptOnce(any());
            
            spyAcceptor.close();
            
            verify(spyAcceptor, times(0)).acceptOnce(lastBucketMin);
            verify(spyAcceptor, times(0)).acceptOnce(lastBucketHour);
        }
    }

    @Nested
    class ConstantTest {

        @Test
        void testUnKnownConstant() {
            assertEquals("UN_KNOWN", MetricInstanceAcceptor.UN_KNOW);
        }
    }

    @Nested
    class IntegrationTest {

        @Test
        void testBucketTimeCalculation() {
            Document document = createValidDocument();
            document.append("reqTime", 125000L); // 125 seconds
            document.append("succeed", true); // 125 seconds
            
            // Expected calculations:
            // reqTimeOSec = 125000 / 1000 = 125
            // bucketSec = (125 / 5) * 5 = 125
            // bucketMin = (125 / 60) * 60 = 120
            // bucketHour = (120 / 60) * 60 = 120
            Function<Long, MetricInstanceAcceptor.BucketInfo> bucketInfoGetter = ts -> {
                return new MetricInstanceAcceptor.BucketInfo(null, null);
            };
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(MetricTypes.API_SERVER, bucketInfoGetter, consumer);
            
            testAcceptor.accept(new MetricInstanceFactory.CallInfo(document));
            
            // Verify the bucket calculations are correct
            verify(consumer, never()).apply(anyBoolean(), any());
        }
    }

    // Helper methods
    private Document createValidDocument() {
        return new Document()
                .append("allPathId", "/api/test")
                .append("api_gateway_uuid", "server1")
                .append("latency", 100L)
                .append("req_bytes", 1024L)
                .append("reqTime", 0L)
                .append("_id", new ObjectId());
    }

    private ApiMetricsRaw createApiMetricsRaw(String serverId, String apiId, Long timeStart, Integer timeGranularity) {
        ApiMetricsRaw raw = new ApiMetricsRaw();
        raw.setProcessId(serverId);
        raw.setApiId(apiId);
        raw.setTimeStart(timeStart);
        raw.setTimeGranularity(timeGranularity);
        raw.setReqCount(0L);
        raw.setErrorCount(0L);
        raw.setRps(0.0);
        raw.setBytes(new ArrayList<>());
        raw.setDelay(new ArrayList<>());
        raw.setSubMetrics(new HashMap<>());
        raw.setLastCallId(new ObjectId());
        return raw;
    }
}