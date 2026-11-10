package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.SessionCallback;
import org.springframework.data.mongodb.core.SessionScoped;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApiMetricsRawServiceTest {

    ApiMetricsRawService service;
    ApiMetricsRepository repository;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawService.class);
        repository = mock(ApiMetricsRepository.class);
        ReflectionTestUtils.setField(service, "repository", repository);
    }

    @Nested
    class FindTest {
        @org.junit.jupiter.api.Test
        void testFind() {
            when(repository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Assertions.assertDoesNotThrow(() -> service.find(new Query()));
        }
    }

    @Nested
    class HasAttributableDataTest {
        @Test
        void treatsNullPhysicalNodeIdAsUnattributable() {
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            ApiMetricsRawService actual = new ApiMetricsRawService();
            ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);
            when(mongoTemplate.count(any(Query.class), eq(ApiMetricsRaw.class))).thenReturn(0L);
            org.mockito.ArgumentCaptor<Query> captor = org.mockito.ArgumentCaptor.forClass(Query.class);
            when(mongoTemplate.count(captor.capture(), eq(ApiCallEntity.class),
                    org.mockito.ArgumentMatchers.anyString())).thenReturn(1L);

            Assertions.assertTrue(actual.hasAttributableData(1000L, 2000L));

            Document query = captor.getValue().getQueryObject();
            List<Document> clauses = query.getList("$and", Document.class);
            Document nodeClause = clauses.stream()
                    .filter(clause -> clause.containsKey("physicalNodeId"))
                    .findFirst()
                    .orElseThrow();
            Document nodeCriteria = (Document) nodeClause.get("physicalNodeId");
            Assertions.assertTrue(nodeCriteria.containsKey("$nin"));
            Assertions.assertFalse(nodeCriteria.containsKey("$ne"));
            Assertions.assertEquals(java.util.Arrays.asList(null, ""), nodeCriteria.get("$nin"));
        }
    }

    @Nested
    class DashboardRequestCountsTest {
        private ClientSession snapshotSession;

        @Test
        void usesFullHourBucketsAndExactMinuteEdges() {
            ApiMetricsRepository rawRepository = mock(ApiMetricsRepository.class);
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            ApiMetricsRawService actual = new ApiMetricsRawService();
            ReflectionTestUtils.setField(actual, "repository", rawRepository);
            ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);
            MongoOperations snapshot = mockSnapshot(mongoTemplate);

            List<ApiMetricsRaw> rows = List.of(
                    rawMetric(1, 1800L, 999L),
                    rawMetric(1, 1860L, 20L),
                    rawMetric(1, 3540L, 30L),
                    rawMetric(1, 3600L, 888L),
                    rawMetric(2, 3600L, 50L),
                    rawMetric(2, 7200L, 999L));
            when(snapshot.find(any(Query.class), eq(ApiMetricsRaw.class))).thenAnswer(invocation -> {
                Query query = invocation.getArgument(0);
                Document object = query.getQueryObject();
                int granularity = ((Number) object.get("timeGranularity")).intValue();
                Document time = (Document) object.get("timeStart");
                long start = ((Number) time.get("$gte")).longValue();
                long end = ((Number) time.get("$lt")).longValue();
                return rows.stream().filter(row -> row.getTimeGranularity() == granularity
                        && row.getTimeStart() >= start && row.getTimeStart() < end).toList();
            });
            ApiCallEntity edgeCall = new ApiCallEntity();
            edgeCall.setReqTime(1835L * 1000L);
            ApiCallEntity endCall = new ApiCallEntity();
            endCall.setReqTime(7200L * 1000L);
            when(snapshot.stream(any(Query.class), eq(ApiCallEntity.class), any(String.class)))
                    .thenAnswer(invocation -> {
                        Query query = invocation.getArgument(0);
                        Document time = (Document) query.getQueryObject().get("reqTime");
                        long start = ((Number) time.get("$gte")).longValue();
                        long end = ((Number) time.get("$lt")).longValue();
                        return List.of(edgeCall, endCall).stream()
                                .filter(call -> call.getReqTime() >= start && call.getReqTime() < end);
                    });

            Map<Long, Long> counts = actual.dashboardRequestCounts(1830L, 7200L,
                    TimeGranularity.HOUR, MetricTypes.ALL, null);

            assertEquals(51L, counts.get(0L));
            assertEquals(50L, counts.get(3600L));
            Assertions.assertFalse(counts.containsKey(7200L));
            verify(snapshotSession).commitTransaction();
        }

        @Test
        void overlaysOnlyUnaggregatedApiCallsOnRecentRawBuckets() {
            ApiMetricsRepository rawRepository = mock(ApiMetricsRepository.class);
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            ApiMetricsRawService actual = new ApiMetricsRawService();
            ReflectionTestUtils.setField(actual, "repository", rawRepository);
            ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);

            ApiMetricsRaw minute = rawMetric(TimeGranularity.MINUTE.getType(), 0L, 0L);
            ApiMetricsRaw firstBucket = rawMetric(TimeGranularity.SECOND_FIVE.getType(), 0L, 1L);
            ApiMetricsRaw secondBucket = rawMetric(TimeGranularity.SECOND_FIVE.getType(), 5L, 1L);
            minute.setSubMetrics(Map.of(0L, firstBucket, 5L, secondBucket));
            MongoOperations snapshot = mockSnapshot(mongoTemplate);
            when(snapshot.find(any(Query.class), eq(ApiMetricsRaw.class))).thenReturn(List.of(minute));

            ApiCallEntity processed = apiCall(1_000L, true);
            ApiCallEntity pending = apiCall(6_000L, false);
            when(snapshot.stream(any(Query.class), eq(ApiCallEntity.class), any(String.class)))
                    .thenAnswer(invocation -> {
                        Query query = invocation.getArgument(0);
                        Document queryObject = query.getQueryObject();
                        boolean pendingOnly = queryObject.containsKey("hasMetric");
                        return List.of(processed, pending).stream()
                                .filter(call -> !pendingOnly || !call.isHasMetric())
                                .filter(call -> call.getReqTime() >= 0L && call.getReqTime() < 10_000L);
                    });

            Map<Long, Long> counts = actual.dashboardRequestCounts(0L, 10L,
                    TimeGranularity.SECOND_FIVE, MetricTypes.ALL, null);

            assertEquals(1L, counts.get(0L));
            assertEquals(2L, counts.get(5L));
            verify(mongoTemplate).withSession(any(Supplier.class));
            verify(snapshot).find(any(Query.class), eq(ApiMetricsRaw.class));
            verify(snapshot).stream(any(Query.class), eq(ApiCallEntity.class), any(String.class));
            verify(mongoTemplate, never()).find(any(Query.class), eq(ApiCallEntity.class), any(String.class));
            verify(snapshotSession).commitTransaction();
        }

        @Test
        void abortsSnapshotWhenAReadFails() {
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            ApiMetricsRawService actual = new ApiMetricsRawService();
            ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);
            MongoOperations snapshot = mockSnapshot(mongoTemplate);
            when(snapshot.find(any(Query.class), eq(ApiMetricsRaw.class))).thenThrow(new IllegalStateException("read failed"));
            when(snapshotSession.hasActiveTransaction()).thenReturn(true);

            Assertions.assertThrows(IllegalStateException.class, () -> actual.dashboardRequestCounts(0L, 5L,
                    TimeGranularity.SECOND_FIVE, MetricTypes.ALL, null));

            verify(snapshotSession).startTransaction(any(TransactionOptions.class));
            verify(snapshotSession).abortTransaction();
        }

        private ApiCallEntity apiCall(long reqTime, boolean hasMetric) {
            ApiCallEntity call = new ApiCallEntity();
            call.setReqTime(reqTime);
            call.setHasMetric(hasMetric);
            call.setDelete(false);
            call.setApi_gateway_uuid("server-1");
            return call;
        }

        @SuppressWarnings("unchecked")
        private MongoOperations mockSnapshot(MongoTemplate mongoTemplate) {
            MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
            ClientSession session = mock(ClientSession.class);
            snapshotSession = session;
            SessionScoped sessionScoped = mock(SessionScoped.class);
            MongoOperations snapshot = mock(MongoOperations.class);
            when(mongoTemplate.getMongoDatabaseFactory()).thenReturn(databaseFactory);
            when(databaseFactory.getSession(any(ClientSessionOptions.class))).thenReturn(session);
            when(mongoTemplate.withSession(any(Supplier.class))).thenReturn(sessionScoped);
            when(sessionScoped.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
                SessionCallback<?> callback = invocation.getArgument(0);
                return callback.doInSession(snapshot);
            });
            return snapshot;
        }

        private ApiMetricsRaw rawMetric(int granularity, long timeStart, long count) {
            ApiMetricsRaw raw = new ApiMetricsRaw();
            raw.setMetricType(MetricTypes.ALL.getType());
            raw.setTimeGranularity(granularity);
            raw.setTimeStart(timeStart);
            raw.setReqCount(count);
            return raw;
        }
    }
}
