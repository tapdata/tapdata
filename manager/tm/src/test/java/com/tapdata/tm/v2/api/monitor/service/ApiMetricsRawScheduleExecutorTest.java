package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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

        when(executor.create()).thenReturn(metricInstanceFactory);
        doCallRealMethod().when(executor).aggregateApiCall();
        when(executor.lastOne()).thenCallRealMethod();
        doCallRealMethod().when(executor).saveApiMetricsRaw(anyList());
        doCallRealMethod().when(executor).bulkUpsert(anyList());
        doCallRealMethod().when(executor).bulkUpsert(anyList(), any(Function.class), any(Function.class));
        when(executor.buildDefaultQuery(any(ApiMetricsRaw.class))).thenCallRealMethod();
        when(executor.buildDefaultUpdate(any(ApiMetricsRaw.class))).thenCallRealMethod();
        when(executor.findMetricStart(any(Query.class))).thenCallRealMethod();
    }

    @Nested
    class AggregateApiCallTest {
        @Test
        void testAggregateApiCall() {
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongoTemplate.getCollection(anyString())).thenReturn(collection);
            when(mongoTemplate.findOne(any(Query.class), any(Class.class))).thenReturn(createApiMetricsRaw());
            FindIterable<Document> iterable = mock(FindIterable.class);
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(collection.find(any(Document.class), any(Class.class))).thenReturn(iterable);
            when(iterable.sort(any(Bson.class))).thenReturn(iterable);
            when(iterable.batchSize(anyInt())).thenReturn(iterable);
            when(iterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(new Document());
            doNothing().when(metricInstanceFactory).accept(any(Document.class));
            Assertions.assertDoesNotThrow(executor::aggregateApiCall);
            verify(executor).create();
        }
        @Test
        void testAggregateApiCallNotHaveLastOne() {
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongoTemplate.getCollection(anyString())).thenReturn(collection);
            when(mongoTemplate.findOne(any(Query.class), any(Class.class))).thenReturn(null);
            FindIterable<Document> iterable = mock(FindIterable.class);
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(collection.find(any(Document.class), any(Class.class))).thenReturn(iterable);
            when(iterable.sort(any(Bson.class))).thenReturn(iterable);
            when(iterable.batchSize(anyInt())).thenReturn(iterable);
            when(iterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(new Document());
            doNothing().when(metricInstanceFactory).accept(any(Document.class));
            Assertions.assertDoesNotThrow(executor::aggregateApiCall);
            verify(executor).create();
        }
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
        void testSaveApiMetricsRawException() {
            List<ApiMetricsRaw> apiMetricsRawList = new ArrayList<>();
            apiMetricsRawList.add(createApiMetricsRaw());
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenAnswer(a -> {throw new Exception("e");});
            Assertions.assertDoesNotThrow(() -> executor.saveApiMetricsRaw(apiMetricsRawList));
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
        raw.setBytes(new ArrayList<>());
        raw.setDelay(new ArrayList<>());
        raw.setSubMetrics(new HashMap<>());
        raw.setP50(150L);
        raw.setP95(300L);
        raw.setP99(400L);
        raw.setCallId(new ObjectId());
        return raw;
    }
}