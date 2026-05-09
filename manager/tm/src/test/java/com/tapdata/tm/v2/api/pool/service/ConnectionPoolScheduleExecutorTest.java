package com.tapdata.tm.v2.api.pool.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ConnectionPoolScheduleExecutorTest {

    private ConnectionPoolScheduleExecutor executor;
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        executor = spy(new ConnectionPoolScheduleExecutor());
        mongoTemplate = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(executor, "mongoTemplate", mongoTemplate);
    }

    @Test
    void testLastOneNullAndNotNull() {
        when(mongoTemplate.findOne(any(Query.class), eq(ConnectionPoolEntity.class))).thenReturn(null);
        assertNull(executor.lastOne());

        ConnectionPoolEntity entity = new ConnectionPoolEntity();
        entity.setLastUpdateTime(123L);
        when(mongoTemplate.findOne(any(Query.class), eq(ConnectionPoolEntity.class))).thenReturn(entity);
        assertEquals(123L, executor.lastOne());
    }

    @Test
    void testSaveApiMetricsRawEmptyAndNonEmpty() {
        doNothing().when(executor).bulkUpsert(anyList());
        executor.saveApiMetricsRaw(new ArrayList<>());
        verify(executor, times(0)).bulkUpsert(anyList());

        List<ConnectionPoolEntity> list = new ArrayList<>();
        ConnectionPoolEntity e = new ConnectionPoolEntity();
        e.setLastUpdateTime(1000L);
        list.add(e);
        executor.saveApiMetricsRaw(list);
        verify(executor, times(1)).bulkUpsert(anyList());
    }

    @Test
    void testBulkUpsertNullAndEmpty() {
        executor.bulkUpsert(null);
        executor.bulkUpsert(new ArrayList<>());
        verifyNoInteractions(mongoTemplate);
    }

    @Test
    void testBulkUpsertNormalAndUpdateFields() {
        BulkOperations bulkOps = mock(BulkOperations.class);
        when(mongoTemplate.bulkOps(eq(BulkOperations.BulkMode.ORDERED), eq(ConnectionPoolEntity.class))).thenReturn(bulkOps);
        when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(bulkOps);

        ConnectionPoolEntity e1 = new ConnectionPoolEntity();
        e1.setProcessId("p1");
        e1.setConnectionId("c1");
        e1.setTimeGranularity(TimeGranularity.MINUTE.getType());
        e1.setLastUpdateTime(120_000L);
        e1.setMaxConnections(10);
        e1.setUsedConnections(5);
        e1.setAvailable(3);
        e1.setQueueSize(2);

        ConnectionPoolEntity e2 = new ConnectionPoolEntity();
        e2.setProcessId("p2");
        e2.setConnectionId("c2");
        e2.setTimeGranularity(TimeGranularity.HOUR.getType());
        e2.setLastUpdateTime(3_600_000L);

        List<ConnectionPoolEntity> list = new ArrayList<>();
        list.add(e1);
        list.add(e2);

        executor.bulkUpsert(list);

        assertNotNull(e1.getTtlKey());
        assertNotNull(e2.getTtlKey());
        assertEquals(new Date(120_000L), e1.getTtlKey());
        assertEquals(new Date(3_600_000L), e2.getTtlKey());

        verify(bulkOps, times(2)).upsert(any(Query.class), any(Update.class));
        verify(bulkOps, times(1)).execute();

        AtomicReference<Query> capturedQuery = new AtomicReference<>();
        AtomicReference<Update> capturedUpdate = new AtomicReference<>();
        Mockito.reset(bulkOps);
        when(bulkOps.upsert(any(Query.class), any(Update.class))).thenAnswer(invocation -> {
            capturedQuery.set(invocation.getArgument(0));
            capturedUpdate.set(invocation.getArgument(1));
            return bulkOps;
        });
        when(bulkOps.execute()).thenReturn(null);

        executor.bulkUpsert(List.of(e1));

        Document q = capturedQuery.get().getQueryObject();
        assertEquals("p1", q.getString(ConnectionPoolField.PROCESS_ID.field()));
        assertEquals("c1", q.getString(ConnectionPoolField.CONNECTION_ID.field()));
        assertEquals(TimeGranularity.MINUTE.getType(), ((Number) q.get(ConnectionPoolField.TIME_GRANULARITY.field())).intValue());
        assertEquals(120_000L, ((Number) q.get(ConnectionPoolField.LAST_UPDATE_TIME.field())).longValue());

        Document u = capturedUpdate.get().getUpdateObject();
        Document set = u.get("$set", Document.class);
        assertEquals(10, set.get(ConnectionPoolField.MAX_CONNECTIONS.field()));
        assertEquals(5, set.get(ConnectionPoolField.USED_CONNECTIONS.field()));
        assertEquals(3, set.get(ConnectionPoolField.AVAILABLE.field()));
        assertEquals(2, set.get(ConnectionPoolField.QUEUE_SIZE.field()));
        assertNotNull(set.get(ConnectionPoolField.TTL_KEY.field()));
        assertNotNull(u.get("$currentDate", Document.class));
    }

    @Test
    void testBulkUpsertExceptionFromNullLastUpdateTime() {
        BulkOperations bulkOps = mock(BulkOperations.class);
        when(mongoTemplate.bulkOps(eq(BulkOperations.BulkMode.ORDERED), eq(ConnectionPoolEntity.class))).thenReturn(bulkOps);

        ConnectionPoolEntity e = new ConnectionPoolEntity();
        e.setProcessId("p1");
        e.setConnectionId("c1");
        e.setTimeGranularity(TimeGranularity.MINUTE.getType());
        e.setLastUpdateTime(null);

        Assertions.assertDoesNotThrow(() -> executor.bulkUpsert(List.of(e), x -> new Query(), x -> new Update()));
    }

    @Test
    void testBulkUpsertExceptionFromExecute() {
        BulkOperations bulkOps = mock(BulkOperations.class);
        when(mongoTemplate.bulkOps(eq(BulkOperations.BulkMode.ORDERED), eq(ConnectionPoolEntity.class))).thenReturn(bulkOps);
        when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(bulkOps);
        when(bulkOps.execute()).thenThrow(new RuntimeException("boom"));

        ConnectionPoolEntity e = new ConnectionPoolEntity();
        e.setProcessId("p1");
        e.setConnectionId("c1");
        e.setTimeGranularity(TimeGranularity.MINUTE.getType());
        e.setLastUpdateTime(120_000L);

        Assertions.assertDoesNotThrow(() -> executor.bulkUpsert(List.of(e)));
    }

    @Test
    void testFindMetricStart() {
        Query q = new Query();
        ConnectionPoolEntity entity = new ConnectionPoolEntity();
        when(mongoTemplate.findOne(eq(q), eq(ConnectionPoolEntity.class))).thenReturn(entity);
        assertEquals(entity, executor.findMetricStart(q));
    }

    @Test
    void testAggregateUsageWithoutLastOneAndNoData() {
        doReturn(null).when(executor).lastOne();

        MongoCollection<Document> collection = mock(MongoCollection.class);
        FindIterable<Document> iterable = mock(FindIterable.class);
        MongoCursor<Document> cursor = mock(MongoCursor.class);

        when(mongoTemplate.getCollection(eq("ConnectionPool"))).thenReturn(collection);
        when(collection.find(any(Document.class), eq(Document.class))).thenReturn(iterable);
        when(iterable.sort(any())).thenReturn(iterable);
        when(iterable.batchSize(anyInt())).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(false);

        try (MockedStatic<MongoUtils> mongoUtilsMockedStatic = Mockito.mockStatic(MongoUtils.class);
             MockedConstruction<ConnectionPoolInstanceFactory> construction = Mockito.mockConstruction(ConnectionPoolInstanceFactory.class,
                     (mock, context) -> {
                         doNothing().when(mock).accept(any(Document.class));
                         doNothing().when(mock).close();
                     })) {
            mongoUtilsMockedStatic.when(() -> MongoUtils.getCollectionNameIgnore(ConnectionPoolEntity.class)).thenReturn("ConnectionPool");
            executor.aggregateUsage();

            assertEquals(1, construction.constructed().size());
            verify(construction.constructed().get(0), times(1)).close();

            AtomicReference<Document> filter = new AtomicReference<>();
            Mockito.reset(collection);
            when(collection.find(any(Document.class), eq(Document.class))).thenAnswer(invocation -> {
                filter.set(invocation.getArgument(0));
                return iterable;
            });
            when(iterable.sort(any())).thenReturn(iterable);
            when(iterable.batchSize(anyInt())).thenReturn(iterable);
            when(iterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(false);

            executor.aggregateUsage();
            assertNotNull(filter.get());
            assertEquals(TimeGranularity.SECOND_FIVE.getType(), ((Number) filter.get().get(ConnectionPoolField.TIME_GRANULARITY.field())).intValue());
            assertNull(filter.get().get(ConnectionPoolField.LAST_UPDATE_TIME.field()));
        }
    }

    @Test
    void testAggregateUsageWithLastOneAndTwoDocs() {
        doReturn(1000L).when(executor).lastOne();

        MongoCollection<Document> collection = mock(MongoCollection.class);
        FindIterable<Document> iterable = mock(FindIterable.class);
        MongoCursor<Document> cursor = mock(MongoCursor.class);

        when(mongoTemplate.getCollection(eq("ConnectionPool"))).thenReturn(collection);
        when(collection.find(any(Document.class), eq(Document.class))).thenReturn(iterable);
        when(iterable.sort(any())).thenReturn(iterable);
        when(iterable.batchSize(anyInt())).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(cursor);

        Document d1 = new Document().append("a", 1);
        Document d2 = new Document().append("a", 2);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(d1, d2);

        AtomicReference<Document> filter = new AtomicReference<>();
        when(collection.find(any(Document.class), eq(Document.class))).thenAnswer(invocation -> {
            filter.set(invocation.getArgument(0));
            return iterable;
        });

        try (MockedStatic<MongoUtils> mongoUtilsMockedStatic = Mockito.mockStatic(MongoUtils.class);
             MockedConstruction<ConnectionPoolInstanceFactory> construction = Mockito.mockConstruction(ConnectionPoolInstanceFactory.class,
                     (mock, context) -> {
                         doNothing().when(mock).accept(any(Document.class));
                         doNothing().when(mock).close();
                     })) {
            mongoUtilsMockedStatic.when(() -> MongoUtils.getCollectionNameIgnore(ConnectionPoolEntity.class)).thenReturn("ConnectionPool");
            executor.aggregateUsage();

            assertNotNull(filter.get());
            Document lastUpdateTime = (Document) filter.get().get(ConnectionPoolField.LAST_UPDATE_TIME.field());
            assertNotNull(lastUpdateTime);
            assertEquals(1000L, ((Number) lastUpdateTime.get("$gte")).longValue());

            ConnectionPoolInstanceFactory factory = construction.constructed().get(0);
            verify(factory, times(2)).accept(any(Document.class));
            verify(factory, times(1)).close();
        }
    }
}

