package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServerUsageMetricInstanceFactoryTest {

    @Mock
    private Consumer<List<ServerUsageMetric>> consumer;

    @Mock
    private Function<Query, ServerUsageMetric> findOne;

    private ServerUsageMetricInstanceFactory factory;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        factory = new ServerUsageMetricInstanceFactory(consumer, findOne);
    }

    @Nested
    class ConstructorTest {
        @Test
        void testConstructor() {
            assertNotNull(factory.apiMetricsRaws);
            assertNotNull(factory.instanceMap);
            assertEquals(consumer, factory.consumer);
            assertEquals(findOne, factory.findOne);
            assertFalse(factory.needUpdateTag);
        }
    }

    @Nested
    class NeedUpdateTest {
        @Test
        void testNeedUpdate() {
            assertFalse(factory.needUpdate());
            
            factory.needUpdate(true);
            assertTrue(factory.needUpdate());
            
            factory.needUpdate(false);
            assertFalse(factory.needUpdate());
        }
    }

    @Nested
    class AcceptTest {
        @Test
        void testAcceptWithNullEntity() {
            factory.accept(null);
            
            assertFalse(factory.needUpdate());
            assertTrue(factory.instanceMap.isEmpty());
        }

        @Test
        void testAcceptWithValidEntity() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            when(findOne.apply(any(Query.class))).thenReturn(null);
            
            factory.accept(entity);
            
            assertTrue(factory.needUpdate());
            assertEquals(1, factory.instanceMap.size());
            assertTrue(factory.instanceMap.containsKey("server1:work1"));
        }

        @Test
        void testAcceptWithExistingKey() {
            Document entity1 = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            Document entity2 = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            when(findOne.apply(any(Query.class))).thenReturn(null);
            
            factory.accept(entity1);
            factory.accept(entity2);
            
            assertTrue(factory.needUpdate());
            assertEquals(1, factory.instanceMap.size());
        }

        @Test
        void testAcceptWithDifferentKeys() {
            Document entity1 = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            Document entity2 = new Document()
                    .append("processId", "server2")
                    .append("workOid", "work2");
            
            when(findOne.apply(any(Query.class))).thenReturn(null);
            
            factory.accept(entity1);
            factory.accept(entity2);
            
            assertTrue(factory.needUpdate());
            assertEquals(2, factory.instanceMap.size());
            assertTrue(factory.instanceMap.containsKey("server1:work1"));
            assertTrue(factory.instanceMap.containsKey("server2:work2"));
        }

        @Test
        void testAcceptWithBatchSizeReached() {
            when(findOne.apply(any(Query.class))).thenReturn(null);
            
            // Add enough entities to trigger flush
            for (int i = 0; i < 500; i++) {
                Document entity = new Document()
                        .append("processId", "server" + i)
                        .append("workOid", "work" + i);
                factory.accept(entity);
            }
            
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptWithExistingLastMin() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            ServerUsageMetric lastMin = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            when(findOne.apply(any(Query.class)))
                    .thenReturn(lastMin)
                    .thenReturn(null);
            
            factory.accept(entity);
            
            assertTrue(factory.needUpdate());
            assertEquals(1, factory.instanceMap.size());
        }

        @Test
        void testAcceptWithExistingLastMinAndHour() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            ServerUsageMetric lastMin = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            ServerUsageMetric lastHour = ServerUsageMetric.instance(2, 3600000L, "server1", "work1", 0);
            when(findOne.apply(any(Query.class)))
                    .thenReturn(lastMin)
                    .thenReturn(lastHour);
            
            factory.accept(entity);
            
            assertTrue(factory.needUpdate());
            assertEquals(1, factory.instanceMap.size());
        }
    }

    @Nested
    class LastOneTest {
        @Test
        void testLastOneWithTimeStart() {
            ServerUsageMetric expected = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            when(findOne.apply(any(Query.class))).thenReturn(expected);
            
            ServerUsageMetric result = factory.lastOne("server1", 1, 60000L);
            
            assertEquals(expected, result);
            verify(findOne, times(1)).apply(any(Query.class));
        }

        @Test
        void testLastOneWithoutTimeStart() {
            ServerUsageMetric expected = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            when(findOne.apply(any(Query.class))).thenReturn(expected);
            
            ServerUsageMetric result = factory.lastOne("server1", 1, null);
            
            assertEquals(expected, result);
            verify(findOne, times(1)).apply(any(Query.class));
        }

        @Test
        void testLastOneReturnsNull() {
            when(findOne.apply(any(Query.class))).thenReturn(null);
            
            ServerUsageMetric result = factory.lastOne("server1", 1, null);
            
            assertNull(result);
            verify(findOne, times(1)).apply(any(Query.class));
        }
    }

    @Nested
    class FlushTest {
        @Test
        void testFlushWithEmptyList() {
            factory.flush();
            
            verifyNoInteractions(consumer);
        }

        @Test
        void testFlushWithNonEmptyList() {
            // Add some items to the list
            factory.apiMetricsRaws.add(ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0));
            factory.apiMetricsRaws.add(ServerUsageMetric.instance(1, 120000L, "server2", "work2", 0));
            
            factory.flush();
            
            verify(consumer, times(1)).accept(any());
            assertTrue(factory.apiMetricsRaws.isEmpty());
        }
    }

    @Nested
    class CloseTest {
        @Test
        void testCloseWithEmptyInstanceMap() {
            factory.close();
            
            verifyNoInteractions(consumer);
        }

        @Test
        void testCloseWithInstanceMapAndNoUpdate() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            when(findOne.apply(any(Query.class))).thenReturn(null);
            factory.accept(entity);
            factory.needUpdate(false); // Set to false
            
            factory.close();
            
            verifyNoInteractions(consumer);
        }

        @Test
        void testCloseWithInstanceMapAndUpdate() {
            Document entity = new Document()
                    .append("processId", "server1")
                    .append("workOid", "work1");
            
            when(findOne.apply(any(Query.class))).thenReturn(null);
            factory.accept(entity);
            
            factory.close();
            
            verify(consumer, atLeastOnce()).accept(any());
        }

        @Test
        void testCloseWithNonEmptyApiMetricsRaws() {
            factory.apiMetricsRaws.add(ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0));
            factory.needUpdate(true);
            
            factory.close();
            
            verify(consumer, times(1)).accept(any());
            assertTrue(factory.apiMetricsRaws.isEmpty());
        }
    }
}