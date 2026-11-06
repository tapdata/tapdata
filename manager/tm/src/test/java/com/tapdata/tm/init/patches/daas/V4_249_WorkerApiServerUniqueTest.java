package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V4_249_WorkerApiServerUniqueTest {

    @Test
    void activeWorkerQueryUsesWorkerDeletedField() {
        Document query = V4_24_9_WorkerApiServerUnique.activeWorkersQuery().getQueryObject();

        assertTrue(query.containsKey("deleted"));
        assertFalse(query.containsKey("delete"));
    }

    @Test
    void runContinuesWhenIndexWasDroppedConcurrently() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        FindIterable<Document> workers = mock(FindIterable.class);
        when(mongoTemplate.getCollection("Workers")).thenReturn(mongoCollection);
        when(mongoCollection.listIndexes()).thenReturn(indexes);
        when(indexes.into(anyList())).thenAnswer(invocation -> {
            List<Document> result = invocation.getArgument(0);
            result.add(new Document("name", "update_worker_status_index")
                    .append("key", new Document("legacy", 1)));
            return result;
        });
        when(mongoCollection.find(any(Bson.class))).thenReturn(workers);
        when(workers.into(anyList())).thenAnswer(invocation -> invocation.getArgument(0));
        doThrow(new RuntimeException("index was already dropped"))
                .when(mongoCollection).dropIndex("update_worker_status_index");

        V4_24_9_WorkerApiServerUnique patch = new V4_24_9_WorkerApiServerUnique(null, null);
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);

            assertDoesNotThrow(patch::run);
        }

        verify(mongoCollection).createIndex(
                any(Document.class),
                argThat(options -> "update_worker_status_index".equals(options.getName()))
        );
    }

    @Test
    void apiServerIndexUsesDifferentKeyPatternWhenLegacyManagerIndexExists() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        FindIterable<Document> workers = mock(FindIterable.class);
        when(mongoTemplate.getCollection("Workers")).thenReturn(mongoCollection);
        when(mongoCollection.listIndexes()).thenReturn(indexes);
        when(indexes.into(anyList())).thenAnswer(invocation -> {
            List<Document> result = invocation.getArgument(0);
            result.add(new Document("name", "worker_type_1_process_id_1")
                    .append("key", new Document("process_id", 1).append("worker_type", 1))
                    .append("partialFilterExpression", new Document("worker_type", "tapdata-manager"))
                    .append("unique", true));
            return result;
        });
        when(mongoCollection.find(any(Bson.class))).thenReturn(workers);
        when(workers.into(anyList())).thenAnswer(invocation -> invocation.getArgument(0));

        V4_24_9_WorkerApiServerUnique patch = new V4_24_9_WorkerApiServerUnique(null, null);
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);

            patch.run();
        }

        verify(mongoCollection).createIndex(
                argThat((Document key) -> List.copyOf(key.keySet())
                        .equals(List.of("worker_type", "process_id"))
                        && Integer.valueOf(1).equals(key.get("worker_type"))
                        && Integer.valueOf(1).equals(key.get("process_id"))),
                argThat((IndexOptions options) -> V4_24_9_WorkerApiServerUnique.INDEX_NAME.equals(options.getName())
                        && options.isUnique()
                        && new Document("worker_type", "api-server")
                        .append("deleted", false)
                        .append("process_id", new Document("$gt", ""))
                        .equals(options.getPartialFilterExpression()))
        );
    }

    @Test
    void canonicalWorkerPrefersFreshnessBeforeStatus() {
        ObjectId staleId = new ObjectId("000000000000000000000001");
        ObjectId currentId = new ObjectId("000000000000000000000002");
        Document stale = new Document("_id", staleId)
                .append("process_id", "api-server-1")
                .append("ping_time", 200L);
        Document current = new Document("_id", currentId)
                .append("process_id", "api-server-1")
                .append("ping_time", 100L)
                .append("worker_status", new Document("status", "UP"));

        assertEquals(staleId, V4_24_9_WorkerApiServerUnique.selectCanonical(List.of(stale, current)).getObjectId("_id"));
    }
}
