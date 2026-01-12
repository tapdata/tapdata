package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
            
            acceptor.acceptOnce(metric);
            
            verify(consumer, times(1)).accept(metric);
        }

        @Test
        void testAcceptOnceWithNullItem() {
            acceptor.acceptOnce(null);
            
            verifyNoInteractions(consumer);
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