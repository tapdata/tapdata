package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricInstanceFactoryTest {

    @Mock
    private Consumer<List<ApiMetricsRaw>> consumer;

    @Mock
    private Function<Query, ApiMetricsRaw> findOne;

    @Mock
    private MetricInstanceAcceptor acceptor;

    private MetricInstanceFactory factory;

    @BeforeEach
    void setUp() {
        factory = new MetricInstanceFactory(consumer, findOne);
    }

    @Nested
    class ConstructorTest {

        @Test
        void testConstructor() {
            MetricInstanceFactory testFactory = new MetricInstanceFactory(consumer, findOne);

            assertNotNull(testFactory);
            assertEquals(consumer, ReflectionTestUtils.getField(testFactory, "consumer"));
            assertEquals(findOne, ReflectionTestUtils.getField(testFactory, "findOne"));
            assertNotNull(ReflectionTestUtils.getField(testFactory, "apiMetricsRaws"));
            assertNotNull(ReflectionTestUtils.getField(testFactory, "instanceMap"));
            assertFalse((Boolean) ReflectionTestUtils.getField(testFactory, "needUpdateTag"));
        }
    }

    @Nested
    class NeedUpdateTest {

        @Test
        void testNeedUpdateGetter() {
            assertFalse(factory.needUpdate());

            ReflectionTestUtils.setField(factory, "needUpdateTag", true);
            assertTrue(factory.needUpdate());
        }

        @Test
        void testNeedUpdateSetter() {
            factory.needUpdate(true);
            assertTrue(factory.needUpdate());

            factory.needUpdate(false);
            assertFalse(factory.needUpdate());
        }
    }

    @Nested
    class AcceptTest {

        @Test
        void testAcceptNull() {
            factory.accept(null);

            assertFalse(factory.needUpdate());
            verify(consumer, never()).accept(any());
        }

        @Test
        void testAcceptWithIgnoredPath() {
            Document document = new Document("req_path", "/openapi-readOnly.json");
            document.append("succeed", true);
            factory.accept(document);

            assertFalse(factory.needUpdate());
            verify(consumer, never()).accept(any());
        }
    }

    @Nested
    class LastOneTest {

        @Test
        void testLastOneWithoutTimeStart() {
            ApiMetricsRaw expected = createApiMetricsRaw("server1", "api1", 60000L, 1);
            when(findOne.apply(any(Query.class))).thenReturn(expected);

            ApiMetricsRaw result = factory.lastOne("api1", "server1", MetricTypes.API_SERVER, TimeGranularity.MINUTE, null);

            assertEquals(expected, result);
            verify(findOne).apply(argThat(query -> {
                Document queryObject = query.getQueryObject();
                return queryObject.get("reqPath").equals("api1") &&
                        queryObject.get("processId").equals("server1") &&
                        queryObject.get("timeGranularity").equals(1) &&
                        !queryObject.containsKey("timeStart");
            }));
        }

        @Test
        void testLastOneWithTimeStart() {
            ApiMetricsRaw expected = createApiMetricsRaw("server1", "api1", 60000L, 1);
            when(findOne.apply(any(Query.class))).thenReturn(expected);

            ApiMetricsRaw result = factory.lastOne("api1", "server1", MetricTypes.API_SERVER, TimeGranularity.MINUTE, 60000L);

            assertEquals(expected, result);
            verify(findOne).apply(argThat(query -> {
                Document queryObject = query.getQueryObject();
                return queryObject.get("reqPath").equals("api1") &&
                        queryObject.get("processId").equals("server1") &&
                        queryObject.get("timeGranularity").equals(1) &&
                        queryObject.get("timeStart").equals(60000L);
            }));
        }

        @Test
        void testLastOneReturnsNull() {
            when(findOne.apply(any(Query.class))).thenReturn(null);

            ApiMetricsRaw result = factory.lastOne("api1", "server1", MetricTypes.API_SERVER, TimeGranularity.MINUTE, null);

            assertNull(result);
            verify(findOne).apply(any(Query.class));
        }
    }

    @Nested
    class FlushTest {

        @Test
        void testFlushWithEmptyList() {
            MetricInstanceFactory spyFactory = spy(factory);

            spyFactory.flush();

            verify(consumer, never()).accept(any());
        }

        @Test
        void testFlushWithNonEmptyList() {
            List<ApiMetricsRaw> apiMetricsRaws = (List<ApiMetricsRaw>) ReflectionTestUtils.getField(factory, "apiMetricsRaws");
            ApiMetricsRaw item = createApiMetricsRaw("server1", "api1", 60000L, 1);
            apiMetricsRaws.add(item);
            factory.flush();
            assertTrue(apiMetricsRaws.isEmpty());
        }

        @Test
        void testFlushClearsList() {
            List<ApiMetricsRaw> apiMetricsRaws = (List<ApiMetricsRaw>) ReflectionTestUtils.getField(factory, "apiMetricsRaws");
            apiMetricsRaws.add(createApiMetricsRaw("server1", "api1", 60000L, 1));
            apiMetricsRaws.add(createApiMetricsRaw("server2", "api2", 120000L, 1));

            assertEquals(2, apiMetricsRaws.size());

            factory.flush();

            assertTrue(apiMetricsRaws.isEmpty());
            verify(consumer).accept(any());
        }
    }

    @Nested
    class CloseTest {

        @Test
        void testCloseWithEmptyInstanceMap() {
            factory.close();

            verify(consumer, never()).accept(any());
        }

        @Test
        void testCloseWithInstanceMapButNoUpdate() {
            Map<String, MetricInstanceAcceptor> instanceMap =
                    (Map<String, MetricInstanceAcceptor>) ReflectionTestUtils.getField(factory, "instanceMap");
            instanceMap.put("key1", acceptor);

            // needUpdateTag is false by default
            factory.close();

            verify(acceptor).close();
            verify(consumer, never()).accept(any());
        }

        @Test
        void testCloseWithInstanceMapAndUpdate() {
            Map<String, MetricInstanceAcceptor> instanceMap =
                    (Map<String, MetricInstanceAcceptor>) ReflectionTestUtils.getField(factory, "instanceMap");
            instanceMap.put("key1", acceptor);

            factory.needUpdate(true);

            MetricInstanceFactory spyFactory = spy(factory);
            doNothing().when(spyFactory).flush();

            spyFactory.close();

            verify(acceptor).close();
            verify(spyFactory).flush();
        }

        @Test
        void testCloseWithMultipleAcceptors() {
            MetricInstanceAcceptor acceptor2 = mock(MetricInstanceAcceptor.class);
            Map<String, MetricInstanceAcceptor> instanceMap =
                    (Map<String, MetricInstanceAcceptor>) ReflectionTestUtils.getField(factory, "instanceMap");
            instanceMap.put("key1", acceptor);
            instanceMap.put("key2", acceptor2);

            factory.close();

            verify(acceptor).close();
            verify(acceptor2).close();
        }

        @Test
        void testCloseWithNullInstanceMap() {
            ReflectionTestUtils.setField(factory, "instanceMap", null);

            assertDoesNotThrow(() -> factory.close());
        }
    }

    @Nested
    class ConstantTest {

        @Test
        void testBatchSizeConstant() {
            assertEquals(100, ReflectionTestUtils.getField(MetricInstanceFactory.class, "BATCH_SIZE"));
        }

        @Test
        void testIgnorePathConstant() {
            List<String> ignorePath = (List<String>) ReflectionTestUtils.getField(MetricInstanceFactory.class, "IGNORE_PATH");
            assertNotNull(ignorePath);
            assertTrue(ignorePath.contains("/openapi-readOnly.json"));
        }
    }

    // Helper methods
    private Document createValidDocument() {
        return new Document()
                .append("_id", new ObjectId())
                .append("latency", 100L)
                .append("req_bytes", 1024L)
                .append("succeed", true)
                .append("reqTime", System.currentTimeMillis());
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
        return raw;
    }
}