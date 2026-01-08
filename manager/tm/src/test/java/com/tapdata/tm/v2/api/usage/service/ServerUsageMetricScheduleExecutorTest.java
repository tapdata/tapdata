package com.tapdata.tm.v2.api.usage.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServerUsageMetricScheduleExecutorTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoTemplate mongoOperations;

    @Mock
    private MongoCollection<Document> collection;

    @Mock
    private FindIterable<Document> findIterable;

    @Mock
    private MongoCursor<Document> cursor;

    @Mock
    private BulkOperations bulkOperations;

    private ServerUsageMetricScheduleExecutor executor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new ServerUsageMetricScheduleExecutor();
        executor.setMongoTemplate(mongoTemplate);
        executor.setMongoOperations(mongoOperations);
    }

    @Nested
    class AggregateUsageTest {
        @Test
        void testAggregateUsageWithBlankCollectionName() {
            try (MockedStatic<com.tapdata.tm.utils.MongoUtils> mongoUtils = 
                 mockStatic(com.tapdata.tm.utils.MongoUtils.class)) {
                mongoUtils.when(() -> com.tapdata.tm.utils.MongoUtils.getCollectionNameIgnore(any()))
                         .thenReturn("");
                
                executor.aggregateUsage();
                
                verifyNoInteractions(mongoTemplate);
            }
        }

        @Test
        void testAggregateUsageWithNullCollectionName() {
            try (MockedStatic<com.tapdata.tm.utils.MongoUtils> mongoUtils = 
                 mockStatic(com.tapdata.tm.utils.MongoUtils.class)) {
                mongoUtils.when(() -> com.tapdata.tm.utils.MongoUtils.getCollectionNameIgnore(any()))
                         .thenReturn(null);
                
                executor.aggregateUsage();
                
                verifyNoInteractions(mongoTemplate);
            }
        }

        @Test
        void testAggregateUsageWithValidCollectionName() {
            try (MockedStatic<com.tapdata.tm.utils.MongoUtils> mongoUtils = 
                 mockStatic(com.tapdata.tm.utils.MongoUtils.class)) {
                mongoUtils.when(() -> com.tapdata.tm.utils.MongoUtils.getCollectionNameIgnore(any()))
                         .thenReturn("serverUsage");
                
                when(mongoTemplate.getCollection("serverUsage")).thenReturn(collection);
                when(collection.find(any(Document.class), eq(Document.class))).thenReturn(findIterable);
                when(findIterable.sort(any())).thenReturn(findIterable);
                when(findIterable.batchSize(anyInt())).thenReturn(findIterable);
                when(findIterable.iterator()).thenReturn(cursor);
                when(cursor.hasNext()).thenReturn(false);
                
                executor.aggregateUsage();
                
                verify(mongoTemplate, times(1)).getCollection("serverUsage");
                verify(collection, times(1)).find(any(Document.class), eq(Document.class));
            }
        }

        @Test
        void testAggregateUsageWithLastUpdateTime() {
            try (MockedStatic<com.tapdata.tm.utils.MongoUtils> mongoUtils = 
                 mockStatic(com.tapdata.tm.utils.MongoUtils.class)) {
                mongoUtils.when(() -> com.tapdata.tm.utils.MongoUtils.getCollectionNameIgnore(any()))
                         .thenReturn("serverUsage");
                
                ServerUsageMetric lastMetric = ServerUsageMetric.instance(2, 3600000L, "server1", "work1", 0);
                when(mongoTemplate.findOne(any(Query.class), eq(ServerUsageMetric.class)))
                    .thenReturn(lastMetric);
                when(mongoTemplate.getCollection("serverUsage")).thenReturn(collection);
                when(collection.find(any(Document.class), eq(Document.class))).thenReturn(findIterable);
                when(findIterable.sort(any())).thenReturn(findIterable);
                when(findIterable.batchSize(anyInt())).thenReturn(findIterable);
                when(findIterable.iterator()).thenReturn(cursor);
                when(cursor.hasNext()).thenReturn(false);
                executor.aggregateUsage();
                verify(mongoTemplate, times(1)).findOne(any(Query.class), eq(ServerUsageMetric.class));
            }
        }

        @Test
        void testAggregateUsageWithDocuments() {
            try (MockedStatic<com.tapdata.tm.utils.MongoUtils> mongoUtils = 
                 mockStatic(com.tapdata.tm.utils.MongoUtils.class)) {
                mongoUtils.when(() -> com.tapdata.tm.utils.MongoUtils.getCollectionNameIgnore(any()))
                         .thenReturn("serverUsage");
                Document doc1 = new Document("processId", "server1").append("workOid", "work1");
                Document doc2 = new Document("processId", "server2").append("workOid", "work2");
                when(mongoTemplate.getCollection("serverUsage")).thenReturn(collection);
                when(collection.find(any(Document.class), eq(Document.class))).thenReturn(findIterable);
                when(findIterable.sort(any())).thenReturn(findIterable);
                when(findIterable.batchSize(anyInt())).thenReturn(findIterable);
                when(findIterable.iterator()).thenReturn(cursor);
                when(cursor.hasNext()).thenReturn(true, true, false);
                when(cursor.next()).thenReturn(doc1, doc2);
                
                executor.aggregateUsage();
                
                verify(cursor, times(2)).next();
            }
        }
    }

    @Nested
    class LastOneTest {
        @Test
        void testLastOneWithResult() {
            ServerUsageMetric expected = ServerUsageMetric.instance(2, 3600000L, "server1", "work1", 0);
            when(mongoTemplate.findOne(any(Query.class), eq(ServerUsageMetric.class)))
                .thenReturn(expected);
            
            Long result = executor.lastOne();
            
            assertEquals(3600000L, result);
            verify(mongoTemplate, times(1)).findOne(any(Query.class), eq(ServerUsageMetric.class));
        }

        @Test
        void testLastOneWithNullResult() {
            when(mongoTemplate.findOne(any(Query.class), eq(ServerUsageMetric.class)))
                .thenReturn(null);
            
            Long result = executor.lastOne();
            
            assertNull(result);
            verify(mongoTemplate, times(1)).findOne(any(Query.class), eq(ServerUsageMetric.class));
        }
    }

    @Nested
    class SaveApiMetricsRawTest {
        @Test
        void testSaveApiMetricsRawWithEmptyList() {
            executor.saveApiMetricsRaw(Collections.emptyList());
            
            verifyNoInteractions(mongoOperations);
        }

        @Test
        void testSaveApiMetricsRawWithNullList() {
            executor.saveApiMetricsRaw(null);
            
            verifyNoInteractions(mongoOperations);
        }

        @Test
        void testSaveApiMetricsRawWithValidList() {
            List<ServerUsageMetric> metrics = Arrays.asList(
                ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0),
                ServerUsageMetric.instance(1, 120000L, "server2", "work2", 0)
            );
            
            when(mongoOperations.bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class)))
                .thenReturn(bulkOperations);
            when(bulkOperations.upsert(any(Query.class), any(Update.class)))
                .thenReturn(bulkOperations);
            
            executor.saveApiMetricsRaw(metrics);
            
            verify(mongoOperations, times(1)).bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class));
            verify(bulkOperations, times(2)).upsert(any(Query.class), any(Update.class));
            verify(bulkOperations, times(1)).execute();
        }
    }

    @Nested
    class BulkUpsertTest {
        @Test
        void testBulkUpsertWithDefaultBuilders() {
            List<ServerUsageMetric> metrics = Arrays.asList(
                ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0)
            );
            
            when(mongoOperations.bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class)))
                .thenReturn(bulkOperations);
            when(bulkOperations.upsert(any(Query.class), any(Update.class)))
                .thenReturn(bulkOperations);
            
            executor.bulkUpsert(metrics);
            
            verify(mongoOperations, times(1)).bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class));
            verify(bulkOperations, times(1)).upsert(any(Query.class), any(Update.class));
            verify(bulkOperations, times(1)).execute();
        }

        @Test
        void testBulkUpsertWithCustomBuilders() {
            List<ServerUsageMetric> metrics = Arrays.asList(
                ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0)
            );
            
            Function<ServerUsageMetric, Query> queryBuilder = mock(Function.class);
            Function<ServerUsageMetric, Update> updateBuilder = mock(Function.class);
            Query mockQuery = mock(Query.class);
            Update mockUpdate = mock(Update.class);
            
            when(queryBuilder.apply(any(ServerUsageMetric.class))).thenReturn(mockQuery);
            when(updateBuilder.apply(any(ServerUsageMetric.class))).thenReturn(mockUpdate);
            when(mongoOperations.bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class)))
                .thenReturn(bulkOperations);
            when(bulkOperations.upsert(any(Query.class), any(Update.class)))
                .thenReturn(bulkOperations);
            
            executor.bulkUpsert(metrics, queryBuilder, updateBuilder);
            
            verify(queryBuilder, times(1)).apply(any(ServerUsageMetric.class));
            verify(updateBuilder, times(1)).apply(any(ServerUsageMetric.class));
            verify(bulkOperations, times(1)).upsert(mockQuery, mockUpdate);
            verify(bulkOperations, times(1)).execute();
        }

        @Test
        void testBulkUpsertWithNullEntities() {
            Function<ServerUsageMetric, Query> queryBuilder = mock(Function.class);
            Function<ServerUsageMetric, Update> updateBuilder = mock(Function.class);
            
            executor.bulkUpsert(null, queryBuilder, updateBuilder);
            
            verifyNoInteractions(mongoOperations);
            verifyNoInteractions(queryBuilder);
            verifyNoInteractions(updateBuilder);
        }

        @Test
        void testBulkUpsertWithEmptyEntities() {
            Function<ServerUsageMetric, Query> queryBuilder = mock(Function.class);
            Function<ServerUsageMetric, Update> updateBuilder = mock(Function.class);
            
            executor.bulkUpsert(Collections.emptyList(), queryBuilder, updateBuilder);
            
            verifyNoInteractions(mongoOperations);
            verifyNoInteractions(queryBuilder);
            verifyNoInteractions(updateBuilder);
        }

        @Test
        void testBulkUpsertWithException() {
            List<ServerUsageMetric> metrics = Arrays.asList(
                ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0)
            );
            
            when(mongoOperations.bulkOps(any(BulkOperations.BulkMode.class), eq(ServerUsageMetric.class)))
                .thenThrow(new RuntimeException("Database error"));
            
            // Should not throw exception, just log it
            assertDoesNotThrow(() -> executor.bulkUpsert(metrics));
        }
    }

    @Nested
    class FindMetricStartTest {
        @Test
        void testFindMetricStart() {
            Query query = mock(Query.class);
            ServerUsageMetric expected = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            
            when(mongoTemplate.findOne(query, ServerUsageMetric.class)).thenReturn(expected);
            
            ServerUsageMetric result = executor.findMetricStart(query);
            
            assertEquals(expected, result);
            verify(mongoTemplate, times(1)).findOne(query, ServerUsageMetric.class);
        }

        @Test
        void testFindMetricStartReturnsNull() {
            Query query = mock(Query.class);
            
            when(mongoTemplate.findOne(query, ServerUsageMetric.class)).thenReturn(null);
            
            ServerUsageMetric result = executor.findMetricStart(query);
            
            assertNull(result);
            verify(mongoTemplate, times(1)).findOne(query, ServerUsageMetric.class);
        }
    }

    @Nested
    class BuildDefaultQueryTest {
        @Test
        void testBuildDefaultQuery() throws Exception {
            ServerUsageMetric entity = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            
            // Use reflection to access private method
            java.lang.reflect.Method method = ServerUsageMetricScheduleExecutor.class
                .getDeclaredMethod("buildDefaultQuery", ServerUsageMetric.class);
            method.setAccessible(true);
            
            Query result = (Query) method.invoke(executor, entity);
            
            assertNotNull(result);
        }
    }

    @Nested
    class BuildDefaultUpdateTest {
        @Test
        void testBuildDefaultUpdate() throws Exception {
            ServerUsageMetric entity = ServerUsageMetric.instance(1, 60000L, "server1", "work1", 0);
            entity.setMaxCpuUsage(0.8);
            entity.setMinCpuUsage(0.2);
            entity.setMaxHeapMemoryUsage(1000L);
            entity.setMinHeapMemoryUsage(500L);
            entity.setCpuUsage(0.5);
            entity.setHeapMemoryUsage(750L);
            entity.setHeapMemoryMax(2000L);
            
            // Use reflection to access private method
            java.lang.reflect.Method method = ServerUsageMetricScheduleExecutor.class
                .getDeclaredMethod("buildDefaultUpdate", ServerUsageMetric.class);
            method.setAccessible(true);
            
            Update result = (Update) method.invoke(executor, entity);
            
            assertNotNull(result);
        }
    }
}