package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ServerUsageMetricInstanceAcceptorTest {

    @Mock
    private Consumer<ServerUsageMetric> consumer;

    private ServerUsageMetricInstanceAcceptor acceptor;
    private ServerUsageMetric lastBucketMin;
    private ServerUsageMetric lastBucketHour;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        lastBucketMin = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
        lastBucketHour = ServerUsageMetric.instance(2, 3600000L, "server1", "work1", 0);
        acceptor = new ServerUsageMetricInstanceAcceptor(lastBucketMin, lastBucketHour, consumer);
    }

    @Nested
    class AcceptTest {
        @Test
        void testAcceptWithNullEntity() {
            acceptor.accept(null);
            
            verifyNoInteractions(consumer);
        }

        @Test
        void testAcceptWithValidEntity() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 120000L); // 2 minutes
            
            acceptor.accept(entity);
            
            verify(consumer, times(2)).accept(any(ServerUsageMetric.class));
        }

        @Test
        void testAcceptWithDifferentBucketMin() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 180000L); // 3 minutes, different bucket
            
            acceptor.accept(entity);
            
            verify(consumer, times(1)).accept(eq(lastBucketMin));
        }

        @Test
        void testAcceptWithDifferentBucketHour() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 7200000L); // 2 hours, different bucket
            
            acceptor.accept(entity);
            
            verify(consumer, times(1)).accept(eq(lastBucketHour));
        }

        @Test
        void testAcceptWithNullWorkOid() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", null)
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should create new metrics for API_SERVER type
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithStringWorkOid() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work123")
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should handle string workOid
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithNonStringWorkOid() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", 12345)
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should convert to string
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithNullLastUpdateTime() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", null);
            
            acceptor.accept(entity);
            
            // Should handle null lastUpdateTime (defaults to 0)
            verify(consumer, times(2)).accept(any());
        }

        @Test
        void testAcceptWithNullBuckets() {
            acceptor = new ServerUsageMetricInstanceAcceptor(null, null, consumer);
            
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should create new buckets
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptWithOnlyNullBucketMin() {
            acceptor = new ServerUsageMetricInstanceAcceptor(null, lastBucketHour, consumer);
            
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should create new bucket min
            verify(consumer, times(1)).accept(any());
        }

        @Test
        void testAcceptWithOnlyNullBucketHour() {
            acceptor = new ServerUsageMetricInstanceAcceptor(lastBucketMin, null, consumer);
            
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1")
                    .append("lastUpdateTime", 120000L);
            
            acceptor.accept(entity);
            
            // Should create new bucket hour
            verify(consumer, times(1)).accept(any());
        }
    }

    @Nested
    class AcceptOnceTest {
        @Test
        void testAcceptOnceWithValidItem() {
            ServerUsageMetric metric = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);

            ReflectionTestUtils.setField(acceptor, "lastBucketHour", metric);
            acceptor.acceptHour();
            
            verify(consumer, times(1)).accept(metric);
        }

        @Test
        void testAcceptOnceWithNullItem() {
            ReflectionTestUtils.setField(acceptor, "lastBucketHour", null);
            
            verifyNoInteractions(consumer);
        }
    }

    @Nested
    class acceptTest {
        @Test
        void testNormal() {
            ServerUsageMetric metric = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            List<Long> memory = new ArrayList<>();
            memory.add(1L);
            List<Long> memoryMax = new ArrayList<>();
            memoryMax.add(1L);
            List<Double> cpu = new ArrayList<>();
            cpu.add(1D);
            acceptor.accept(metric, memory, memoryMax, cpu);
            Assertions.assertEquals(1L, metric.getHeapMemoryMax());
            Assertions.assertEquals(1L, metric.getHeapMemoryUsage());
            Assertions.assertEquals(1D, metric.getCpuUsage());
            Assertions.assertEquals(1L, metric.getMaxHeapMemoryUsage());
            Assertions.assertEquals(1L, metric.getMinHeapMemoryUsage());
            Assertions.assertEquals(1D, metric.getMaxCpuUsage());
            Assertions.assertEquals(1D, metric.getMinCpuUsage());
        }
        @Test
        void testNormal2() {
            ServerUsageMetric metric = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            List<Long> memory = new ArrayList<>();
            memory.add(1L);
            memory.add(7L);
            List<Long> memoryMax = new ArrayList<>();
            memoryMax.add(1L);
            memoryMax.add(9L);
            List<Double> cpu = new ArrayList<>();
            cpu.add(1D);
            cpu.add(3D);
            acceptor.accept(metric, memory, memoryMax, cpu);
            Assertions.assertEquals(4L, metric.getHeapMemoryMax());
            Assertions.assertEquals(5L, metric.getHeapMemoryUsage());
            Assertions.assertEquals(2D, metric.getCpuUsage());
            Assertions.assertEquals(7L, metric.getMaxHeapMemoryUsage());
            Assertions.assertEquals(1L, metric.getMinHeapMemoryUsage());
            Assertions.assertEquals(3D, metric.getMaxCpuUsage());
            Assertions.assertEquals(1D, metric.getMinCpuUsage());
        }
    }

    @Nested
    class CloseTest {
        @Test
        void testClose() {
            acceptor.close();
            
            verify(consumer, times(2)).accept(any(ServerUsageMetric.class));
        }

        @Test
        void testCloseWithNullBuckets() {
            acceptor = new ServerUsageMetricInstanceAcceptor(null, null, consumer);
            
            acceptor.close();
            
            verifyNoInteractions(consumer);
        }

        @Test
        void testCloseWithOnlyOneBucket() {
            acceptor = new ServerUsageMetricInstanceAcceptor(lastBucketMin, null, consumer);
            
            acceptor.close();
            
            verify(consumer, times(1)).accept(lastBucketMin);
        }
    }
}