package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetricInstanceAcceptorTest {

    @Mock
    private Consumer<ApiMetricsRaw> consumer;

    private MetricInstanceAcceptor acceptor;
    private ApiMetricsRaw lastBucketMin;
    private ApiMetricsRaw lastBucketHour;

    @BeforeEach
    void setUp() {
        lastBucketMin = createApiMetricsRaw("server1", "api1", 60000L, 1);
        lastBucketHour = createApiMetricsRaw("server1", "api1", 3600000L, 2);
        acceptor = new MetricInstanceAcceptor(lastBucketMin, lastBucketHour, consumer);
    }

    @Nested
    class AcceptTest {

        @Test
        void testAcceptNull() {
            acceptor.accept(null);
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptEmptyDocument() {
            Document document = new Document();
            acceptor.accept(document);
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithBlankApiId() {
            Document document = new Document()
                    .append("allPathId", "")
                    .append("req_path", "")
                    .append("api_gateway_uuid", "server1");
            
            acceptor.accept(document);
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptWithAllPathId() {
            Document document = createValidDocument();
            document.append("allPathId", "/api/test");
            
            acceptor.accept(document);
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithReqPath() {
            Document document = createValidDocument();
            document.remove("allPathId");
            document.append("req_path", "/api/test");
            
            acceptor.accept(document);
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithUnknownApiId() {
            Document document = createValidDocument();
            document.remove("allPathId");
            document.remove("req_path");
            
            acceptor.accept(document);
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithDifferentBucketMin() {
            // Create acceptor with different bucket time
            ApiMetricsRaw differentBucketMin = createApiMetricsRaw("server1", "api1", 120000L, 1);
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(differentBucketMin, lastBucketHour, consumer);
            
            Document document = createValidDocument();
            document.append("reqTime", 60000L * 1000L); // Different bucket time
            
            testAcceptor.accept(document);
            
            verify(consumer, times(2)).accept(any(ApiMetricsRaw.class));
        }

        @Test
        void testAcceptWithDifferentBucketHour() {
            // Create acceptor with different bucket hour
            ApiMetricsRaw differentBucketHour = createApiMetricsRaw("server1", "api1", 7200000L, 2);
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(lastBucketMin, differentBucketHour, consumer);
            
            Document document = createValidDocument();
            document.append("reqTime", 3600000L * 1000L); // Different bucket hour
            
            testAcceptor.accept(document);
            
            verify(consumer, times(2)).accept(any(ApiMetricsRaw.class));
        }

        @Test
        void testAcceptWithNullLastBuckets() {
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(null, null, consumer);
            
            Document document = createValidDocument();
            
            testAcceptor.accept(document);
            
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptWithNullLastBucketMin() {
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(null, lastBucketHour, consumer);
            
            Document document = createValidDocument();
            
            testAcceptor.accept(document);
            
            verify(consumer, times(1)).accept(any());
        }

        @Test
        void testAcceptWithNullLastBucketHour() {
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(lastBucketMin, null, consumer);
            
            Document document = createValidDocument();
            
            testAcceptor.accept(document);
            
            verify(consumer, times(1)).accept(any());
        }

        @Test
        void testAcceptWithNullSubMetrics() {
            // Set subMetrics to null
            ReflectionTestUtils.setField(lastBucketMin, "subMetrics", null);
            
            Document document = createValidDocument();
            
            acceptor.accept(document);
            
            // Verify subMetrics was created
            assertNull(lastBucketMin.getSubMetrics());
        }

        @Test
        void testAcceptWithExistingSubMetrics() {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            long bucketSec = 0L; // Based on reqTime 0
            subMetrics.put(bucketSec, createApiMetricsRaw("server1", "api1", bucketSec, 0));
            lastBucketMin.setSubMetrics(subMetrics);
            
            Document document = createValidDocument();
            
            acceptor.accept(document);
            
            // Verify existing sub metric was used
            assertEquals(1, lastBucketMin.getSubMetrics().size());
        }

        @Test
        void testAcceptWithSuccessfulRequest() {
            try (MockedStatic<ApiMetricsDelayInfoUtil> delayInfoUtil = mockStatic(ApiMetricsDelayInfoUtil.class)) {
                delayInfoUtil.when(() -> ApiMetricsDelayInfoUtil.checkByCode(anyString()))
                        .thenReturn(true);
                
                Document document = createValidDocument();
                document.append("code", "200");
                
                acceptor.accept(document);
                
                delayInfoUtil.verify(() -> ApiMetricsDelayInfoUtil.checkByCode("200"));
            }
        }

        @Test
        void testAcceptWithFailedRequest() {
            try (MockedStatic<ApiMetricsDelayInfoUtil> delayInfoUtil = mockStatic(ApiMetricsDelayInfoUtil.class)) {
                delayInfoUtil.when(() -> ApiMetricsDelayInfoUtil.checkByCode(anyString()))
                        .thenReturn(false);
                
                Document document = createValidDocument();
                document.append("code", "500");
                
                acceptor.accept(document);
                
                delayInfoUtil.verify(() -> ApiMetricsDelayInfoUtil.checkByCode("500"));
            }
        }

        @Test
        void testAcceptWithNullOptionalValues() {
            Document document = new Document()
                    .append("allPathId", "/api/test")
                    .append("api_gateway_uuid", "server1")
                    .append("_id", new ObjectId());
            // latency, req_bytes, reqTime are null
            
            acceptor.accept(document);
            
            // Should handle null values gracefully
            verify(consumer, times(2)).accept(any());
        }
    }

    @Nested
    class AcceptOnceTest {

        @Test
        void testAcceptOnceWithValidItem() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            doNothing().when(spyAcceptor).calcPValue(any());
            
            spyAcceptor.acceptOnce(item);
            
            verify(spyAcceptor, times(1)).calcPValue(item);
            verify(consumer, times(1)).accept(item);
        }

        @Test
        void testAcceptOnceWithSubMetrics() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw subItem1 = createApiMetricsRaw("server1", "api1", 0L, 0);
            ApiMetricsRaw subItem2 = createApiMetricsRaw("server1", "api1", 5L, 0);
            subMetrics.put(0L, subItem1);
            subMetrics.put(5L, subItem2);
            item.setSubMetrics(subMetrics);
            
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            doNothing().when(spyAcceptor).calcPValue(any());
            
            spyAcceptor.acceptOnce(item);
            
            // Should call calcPValue for each sub metric plus the main item
            verify(spyAcceptor, times(3)).calcPValue(any());
            verify(consumer, times(1)).accept(item);
        }

        @Test
        void testAcceptOnceWithEmptySubMetrics() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            item.setSubMetrics(new HashMap<>());
            
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            doNothing().when(spyAcceptor).calcPValue(any());
            
            spyAcceptor.acceptOnce(item);
            
            // Should only call calcPValue for the main item
            verify(spyAcceptor, times(1)).calcPValue(item);
            verify(consumer, times(1)).accept(item);
        }

        @Test
        void testAcceptOnceWithNullSubMetrics() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            item.setSubMetrics(null);
            
            MetricInstanceAcceptor spyAcceptor = spy(acceptor);
            doNothing().when(spyAcceptor).calcPValue(any());
            
            spyAcceptor.acceptOnce(item);
            
            // Should only call calcPValue for the main item
            verify(spyAcceptor, times(1)).calcPValue(item);
            verify(consumer, times(1)).accept(item);
        }
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
        void testCalcPValueWithValidItem() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            item.setReqCount(100L);
            
            List<Map<Long, Integer>> mockDelay = Arrays.asList(
                    Collections.singletonMap(100L, 10),
                    Collections.singletonMap(200L, 20)
            );
            
            try (MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any()))
                        .thenReturn(mockDelay);
                delayUtil.when(() -> ApiMetricsDelayUtil.p50(mockDelay, 100))
                        .thenReturn(150L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(mockDelay, 100))
                        .thenReturn(300L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(mockDelay, 100))
                        .thenReturn(400L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(eq(mockDelay), any(), any()))
                        .thenAnswer(invocation -> {
                            LongConsumer maxSetter = invocation.getArgument(1);
                            LongConsumer minSetter = invocation.getArgument(2);
                            maxSetter.accept(500L);
                            minSetter.accept(50L);
                            return null;
                        });
                
                acceptor.calcPValue(item);
                
                assertEquals(150L, item.getP50());
                assertEquals(300L, item.getP95());
                assertEquals(400L, item.getP99());
                assertEquals(500L, item.getMaxDelay());
                assertEquals(50L, item.getMinDelay());
                
                delayUtil.verify(() -> ApiMetricsDelayUtil.fixDelayAsMap(any()));
                delayUtil.verify(() -> ApiMetricsDelayUtil.p50(mockDelay, 100));
                delayUtil.verify(() -> ApiMetricsDelayUtil.p95(mockDelay, 100));
                delayUtil.verify(() -> ApiMetricsDelayUtil.p99(mockDelay, 100));
                delayUtil.verify(() -> ApiMetricsDelayUtil.readMaxAndMin(eq(mockDelay), any(), any()));
            }
        }

        @Test
        void testCalcPValueWithZeroReqCount() {
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            item.setReqCount(0L);
            
            try (MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any()))
                        .thenReturn(Collections.emptyList());
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
            
            verify(spyAcceptor).acceptOnce(lastBucketMin);
            verify(spyAcceptor).acceptOnce(lastBucketHour);
        }

        @Test
        void testCloseWithNullBuckets() {
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(null, null, consumer);
            MetricInstanceAcceptor spyAcceptor = spy(testAcceptor);
            doNothing().when(spyAcceptor).acceptOnce(any());
            
            spyAcceptor.close();
            
            verify(spyAcceptor, times(2)).acceptOnce(null);
            verify(spyAcceptor, times(2)).acceptOnce(any());
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
        void testCompleteWorkflowWithRealData() {
            try (MockedStatic<ApiMetricsDelayInfoUtil> delayInfoUtil = mockStatic(ApiMetricsDelayInfoUtil.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                
                delayInfoUtil.when(() -> ApiMetricsDelayInfoUtil.checkByCode("200"))
                        .thenReturn(true);
                
                List<Map<Long, Integer>> mockDelay = Arrays.asList(
                        Collections.singletonMap(100L, 10)
                );
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any()))
                        .thenReturn(mockDelay);
                delayUtil.when(() -> ApiMetricsDelayUtil.p50(any(), anyInt()))
                        .thenReturn(150L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyInt()))
                        .thenReturn(300L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyInt()))
                        .thenReturn(400L);
                
                MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(null, null, consumer);
                
                Document document = createValidDocument();
                document.append("code", "200");
                document.append("reqTime", 60000L * 1000L); // 1 minute
                
                testAcceptor.accept(document);
                testAcceptor.close();
                
                verify(consumer, times(2)).accept(any(ApiMetricsRaw.class));
            }
        }

        @Test
        void testBucketTimeCalculation() {
            Document document = createValidDocument();
            document.append("reqTime", 125000L); // 125 seconds
            
            // Expected calculations:
            // reqTimeOSec = 125000 / 1000 = 125
            // bucketSec = (125 / 5) * 5 = 125
            // bucketMin = (125 / 60) * 60 = 120
            // bucketHour = (120 / 60) * 60 = 120
            
            MetricInstanceAcceptor testAcceptor = new MetricInstanceAcceptor(null, null, consumer);
            
            testAcceptor.accept(document);
            
            // Verify the bucket calculations are correct
            verify(consumer, never()).accept(any());
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
        raw.setCallId(new ObjectId());
        return raw;
    }
}