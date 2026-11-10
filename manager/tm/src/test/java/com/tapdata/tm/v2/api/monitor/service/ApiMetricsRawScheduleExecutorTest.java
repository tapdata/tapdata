package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SessionCallback;
import org.springframework.data.mongodb.core.SessionScoped;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApiMetricsRawScheduleExecutorTest {
    private MongoTemplate mongoTemplate;
    private MetricInstanceFactory metricInstanceFactory;
    private ApiMetricsRawScheduleExecutor executor;

    @BeforeEach
    void setUp() {
        mongoTemplate = mock(MongoTemplate.class);
        metricInstanceFactory = mock(MetricInstanceFactory.class);


        executor = mock(ApiMetricsRawScheduleExecutor.class);
        ReflectionTestUtils.setField(executor, "mongoTemplate", mongoTemplate);

        when(executor.create(0L)).thenReturn(metricInstanceFactory);
        doCallRealMethod().when(executor).aggregateApiCall();
        doCallRealMethod().when(executor).saveApiMetricsRaw(anyList());
        doCallRealMethod().when(executor).bulkUpsert(anyList());
        doCallRealMethod().when(executor).bulkUpsert(anyList(), any(Function.class), any(Function.class));
        doCallRealMethod().when(executor).bulkUpsert(any(MongoOperations.class), anyList(), any(Function.class), any(Function.class));
        when(executor.buildDefaultQuery(any(ApiMetricsRaw.class))).thenCallRealMethod();
        when(executor.buildDefaultUpdate(any(ApiMetricsRaw.class))).thenCallRealMethod();
        when(executor.findMetricStart(any(Query.class))).thenCallRealMethod();
    }

    @Nested
    class saveApiMetricsRawTest {
        @Test
        void testSaveApiMetricsRaw() {
            List<ApiMetricsRaw> apiMetricsRawList = Arrays.asList(createApiMetricsRaw());
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            executor.saveApiMetricsRaw(apiMetricsRawList);
            verify(executor).bulkUpsert(apiMetricsRawList);
        }
        @Test
        void testSaveApiMetricsRawEmpty() {
            List<ApiMetricsRaw> apiMetricsRawList = new ArrayList<>();
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            executor.saveApiMetricsRaw(apiMetricsRawList);
            verify(executor, times(0)).bulkUpsert(apiMetricsRawList);
        }
        @Test
        void testSaveApiMetricsRawTruncatesOversizedHistogramsInsteadOfFailing() {
            ApiMetricsRaw raw = createApiMetricsRaw();
            List<java.util.Map<String, Number>> oversized = new ArrayList<>();
            for (int i = 0; i < ApiMetricsRawScheduleExecutor.RAW_SAMPLE_CAP + 10; i++) {
                oversized.add(java.util.Map.of("k", i));
            }
            raw.setDelay(oversized);
            raw.setDbCost(new ArrayList<>(oversized));
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);

            executor.saveApiMetricsRaw(List.of(raw));

            assertEquals(ApiMetricsRawScheduleExecutor.RAW_SAMPLE_CAP, raw.getDelay().size());
            assertEquals(ApiMetricsRawScheduleExecutor.RAW_SAMPLE_CAP, raw.getDbCost().size());
            verify(bulkOps).execute();
        }

        @Test
        void testSaveApiMetricsRawException() {
            List<ApiMetricsRaw> apiMetricsRawList = new ArrayList<>();
            apiMetricsRawList.add(createApiMetricsRaw());
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenAnswer(a -> {throw new Exception("e");});
            Assertions.assertThrows(Exception.class, () -> executor.saveApiMetricsRaw(apiMetricsRawList));
            verify(executor).bulkUpsert(apiMetricsRawList);
        }
    }

    @Nested
    class findMetricStartTest {
        @Test
        void testFindMetricStart() {
            ApiMetricsRaw apiMetricsRaw = createApiMetricsRaw();
            when(mongoTemplate.findOne(any(Query.class), eq(ApiMetricsRaw.class))).thenReturn(apiMetricsRaw);
            ApiMetricsRaw result = executor.findMetricStart(new Query());
            assertEquals(apiMetricsRaw, result);
        }

        @Test
        void upsertIdentityIncludesPhysicalNodeId() {
            ApiMetricsRaw apiMetricsRaw = createApiMetricsRaw();
            apiMetricsRaw.setPhysicalNodeId("node-a");

            Query query = executor.buildDefaultQuery(apiMetricsRaw);
            Update update = executor.buildDefaultUpdate(apiMetricsRaw);

            assertEquals("node-a", query.getQueryObject().get("physicalNodeId"));
            assertEquals("node-a", ((org.bson.Document) update.getUpdateObject().get("$set")).get("physicalNodeId"));
        }
    }

    @Test
    void aggregateUsesSessionOperationsAndMarksOnlyReadIdsAfterTheFactoryFlushes() {
        ApiMetricsRawScheduleExecutor actual = new ApiMetricsRawScheduleExecutor();
        ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        ClientSession session = mock(ClientSession.class);
        SessionScoped sessionScoped = mock(SessionScoped.class);
        MongoOperations sessionOperations = mock(MongoOperations.class);
        when(mongoTemplate.getMongoDatabaseFactory()).thenReturn(databaseFactory);
        when(databaseFactory.getSession(any(ClientSessionOptions.class))).thenReturn(session);
        when(mongoTemplate.withSession(any(Supplier.class))).thenReturn(sessionScoped);
        when(session.withTransaction(any(TransactionBody.class), any(TransactionOptions.class)))
                .thenAnswer(invocation -> ((TransactionBody<?>) invocation.getArgument(0)).execute());
        when(sessionScoped.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<?> callback = invocation.getArgument(0);
            return callback.doInSession(sessionOperations);
        });

        String callCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        ObjectId callId = new ObjectId();
        long now = System.currentTimeMillis();
        Document pendingCall = apiCall(callId, now - 60_000L, new Date(now - 60_000L));
        when(sessionOperations.stream(any(Query.class), org.mockito.ArgumentMatchers.eq(Document.class), eq(callCollection)))
                .thenReturn(Stream.of(pendingCall));
        when(sessionOperations.findOne(any(Query.class), eq(ApiMetricsRaw.class))).thenReturn(null);
        BulkOperations sessionBulkOps = mock(BulkOperations.class);
        when(sessionOperations.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(sessionBulkOps);
        when(sessionBulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(sessionBulkOps);

        actual.aggregateApiCall();

        org.mockito.ArgumentCaptor<Query> readQuery = org.mockito.ArgumentCaptor.forClass(Query.class);
        verify(sessionOperations).stream(readQuery.capture(), org.mockito.ArgumentMatchers.eq(Document.class), eq(callCollection));
        assertEquals(ApiMetricsRawScheduleExecutor.BATCH_READ_SIZE, readQuery.getValue().getLimit());
        assertEquals(new Document(ApiCallField.REQ_TIME.field(), 1).append(BaseEntityFields._ID.field(), 1),
                readQuery.getValue().getSortObject());
        Document readFilter = readQuery.getValue().getQueryObject();
        assertEquals(false, readFilter.get(ApiCallField.HAS_METRIC.field()));
        assertTrue(((Document) readFilter.get(ApiCallField.DELETE.field())).containsKey("$ne"));
        assertTrue(((Document) readFilter.get(BaseEntityFields.CREATE_TIME.field())).containsKey("$lt"));

        org.mockito.ArgumentCaptor<Query> markQuery = org.mockito.ArgumentCaptor.forClass(Query.class);
        org.mockito.ArgumentCaptor<Update> markUpdate = org.mockito.ArgumentCaptor.forClass(Update.class);
        verify(sessionOperations).updateMulti(markQuery.capture(), markUpdate.capture(), eq(callCollection));
        Document markFilter = markQuery.getValue().getQueryObject();
        assertEquals(List.of(callId), ((Document) markFilter.get(BaseEntityFields._ID.field())).get("$in"));
        assertEquals(false, markFilter.get(ApiCallField.HAS_METRIC.field()));
        assertEquals(true, ((Document) markUpdate.getValue().getUpdateObject().get("$set"))
                .get(ApiCallField.HAS_METRIC.field()));

        org.mockito.InOrder writeOrder = inOrder(sessionOperations, sessionBulkOps);
        writeOrder.verify(sessionBulkOps).execute();
        writeOrder.verify(sessionOperations).updateMulti(any(Query.class), any(Update.class), eq(callCollection));
        verify(mongoTemplate, never()).getCollection(any(String.class));
        verify(mongoTemplate, never()).updateMulti(any(Query.class), any(Update.class), any(String.class));
        verify(mongoTemplate, never()).bulkOps(any(BulkOperations.BulkMode.class), any(Class.class));
        verify(session).withTransaction(any(TransactionBody.class), any(TransactionOptions.class));
    }

    @Test
    void unwrapsSpringWrappedTransientTransactionLabelsForDriverRetry() {
        ApiMetricsRawScheduleExecutor actual = new ApiMetricsRawScheduleExecutor();
        ReflectionTestUtils.setField(actual, "mongoTemplate", mongoTemplate);
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        ClientSession session = mock(ClientSession.class);
        SessionScoped sessionScoped = mock(SessionScoped.class);
        MongoOperations sessionOperations = mock(MongoOperations.class);
        when(mongoTemplate.getMongoDatabaseFactory()).thenReturn(databaseFactory);
        when(databaseFactory.getSession(any(ClientSessionOptions.class))).thenReturn(session);
        when(mongoTemplate.withSession(any(Supplier.class))).thenReturn(sessionScoped);
        AtomicInteger transactionAttempts = new AtomicInteger();
        when(session.withTransaction(any(TransactionBody.class), any(TransactionOptions.class))).thenAnswer(invocation -> {
            TransactionBody<?> body = invocation.getArgument(0);
            try {
                return body.execute();
            } catch (MongoException retryable) {
                Assertions.assertTrue(retryable.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
                transactionAttempts.incrementAndGet();
                return body.execute();
            }
        });
        when(sessionScoped.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<?> callback = invocation.getArgument(0);
            return callback.doInSession(sessionOperations);
        });
        String callCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        MongoException transientMongoError = new MongoException("transient transaction failure");
        transientMongoError.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
        when(sessionOperations.stream(any(Query.class), org.mockito.ArgumentMatchers.eq(Document.class), eq(callCollection)))
                .thenThrow(new DataAccessResourceFailureException("translated by Spring", transientMongoError))
                .thenReturn(Stream.empty());

        actual.aggregateApiCall();

        assertEquals(1, transactionAttempts.get());
        verify(sessionOperations, times(2)).stream(any(Query.class), org.mockito.ArgumentMatchers.eq(Document.class), eq(callCollection));
    }

    @Test
    void oversizedRawMetricIsTruncatedAndStillWritten() {
        ApiMetricsRaw oversized = createApiMetricsRaw();
        oversized.setDelay(new ArrayList<>(java.util.Collections.nCopies(50_001, new HashMap<>())));
        MongoOperations operations = mock(MongoOperations.class);
        BulkOperations bulkOperations = mock(BulkOperations.class);
        when(operations.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOperations);
        ApiMetricsRawScheduleExecutor actual = new ApiMetricsRawScheduleExecutor();

        actual.bulkUpsert(operations, List.of(oversized), actual::buildDefaultQuery, actual::buildDefaultUpdate);

        assertEquals(ApiMetricsRawScheduleExecutor.RAW_SAMPLE_CAP, oversized.getDelay().size());
        verify(bulkOperations).execute();
    }

    // Helper method to create ApiMetricsRaw instance
    private ApiMetricsRaw createApiMetricsRaw() {
        ApiMetricsRaw raw = new ApiMetricsRaw();
        raw.setId(new ObjectId());
        raw.setApiId("api1");
        raw.setProcessId("process1");
        raw.setTimeStart(System.currentTimeMillis());
        raw.setTimeGranularity(2);
        raw.setReqCount(100L);
        raw.setErrorCount(10L);
        raw.setRps(5.0);
        raw.setBytes(0L);
        raw.setDelay(new ArrayList<>());
        raw.setSubMetrics(new HashMap<>());
        raw.setDbCost(new ArrayList<>());
        raw.setP50(150D);
        raw.setP95(300D);
        raw.setP99(400D);
        return raw;
    }

    private Document apiCall(ObjectId id, long reqTime, Date createTime) {
        return new Document(BaseEntityFields._ID.field(), id)
                .append(BaseEntityFields.CREATE_TIME.field(), createTime)
                .append(ApiCallField.ALL_PATH_ID.field(), "api-unit")
                .append(ApiCallField.REQ_PATH.field(), "/unit")
                .append(ApiCallField.API_GATEWAY_UUID.field(), "server-unit")
                .append(ApiCallField.PHYSICAL_NODE_ID.field(), "node-unit")
                .append(ApiCallField.LATENCY.field(), 10D)
                .append(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), 1D)
                .append(ApiCallField.REQ_BYTES.field(), 8L)
                .append(ApiCallField.REQ_TIME.field(), reqTime)
                .append(ApiCallField.CODE.field(), 200)
                .append(ApiCallField.HTTP_STATUS.field(), 200)
                .append(ApiCallField.DELETE.field(), false)
                .append(ApiCallField.HAS_METRIC.field(), false)
                .append(ApiCallField.SUPPLEMENT.field(), false)
                .append(ApiCallField.SUCCEED.field(), true)
                .append(ApiCallField.WORK_O_ID.field(), "worker-unit");
    }
}
