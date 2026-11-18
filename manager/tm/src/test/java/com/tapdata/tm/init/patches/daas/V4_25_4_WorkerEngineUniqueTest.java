package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V4_25_4_WorkerEngineUniqueTest {

    private static final String PROCESS_ID = "06eefee4-8154-44a1-afc6-a5bd68d0c8c8";

    @Test
    void activeWorkerQueryCoversOnlyGivenTypeAndNotDeletedRecords() {
        Document query = V4_25_4_WorkerEngineUnique.activeWorkersQuery("connector").getQueryObject();

        assertEquals("connector", query.get("worker_type"));
        assertEquals(new Document("$ne", true), query.get("deleted"));
        assertEquals(new Document("$gt", ""), query.get("process_id"));
    }

    @Test
    void indexKeysOnlyCoverEngineTypesAndAvoidExistingKeyPatterns() {
        Map<String, Document> keys = V4_25_4_WorkerEngineUnique.indexKeys();
        assertEquals(List.of("connector", "transformer"), List.copyOf(keys.keySet()));

        Set<List<String>> patterns = new HashSet<>();
        // Existing Workers indexes: tapdata-manager unique, api-server unique, update_worker_status_index.
        patterns.add(List.of("process_id", "worker_type"));
        patterns.add(List.of("worker_type", "process_id"));
        patterns.add(List.of("process_id", "worker_type", "deleted"));
        keys.values().forEach(key -> assertTrue(patterns.add(List.copyOf(key.keySet())),
                "duplicated key pattern " + key.keySet()));
    }

    @Test
    void patchVersionMatchesIdaasVersionFile() throws Exception {
        PatchAnnotation annotation = V4_25_4_WorkerEngineUnique.class.getAnnotation(PatchAnnotation.class);
        String fileVersion;
        try (InputStream in = new ClassPathResource("init/idaas/version").getInputStream()) {
            fileVersion = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        assertEquals("4.25-4", annotation.version());
        assertEquals(0, PatchVersion.valueOf(fileVersion).compareTo(PatchVersion.valueOf(annotation.version())));
    }

    @Test
    void runKeepsLiveConnectorAndRemovesStaleDuplicateThenCreatesUniqueIndexes() {
        ObjectId liveId = new ObjectId("6ae9795c885a7214937a8710");
        ObjectId staleId = new ObjectId("6aebff69885a721493517b42");
        Document live = new Document("_id", liveId).append("process_id", PROCESS_ID)
                .append("worker_type", "connector").append("ping_time", 1795000000000L);
        Document stale = new Document("_id", staleId).append("process_id", PROCESS_ID)
                .append("worker_type", "connector").append("deleted", false).append("ping_time", 1794300000000L);

        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mockCollection(mongoTemplate, List.of(live, stale), List.of());

        runPatch(mongoTemplate);

        ArgumentCaptor<Query> removed = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate, times(1)).remove(removed.capture(), eq("Workers"));
        assertEquals(staleId, removed.getValue().getQueryObject().get("_id"));
        verify(mongoTemplate, times(2)).updateMulti(any(Query.class),
                argThat((Update update) -> new Document("deleted", false).equals(update.getUpdateObject().get("$set"))),
                eq("Workers"));
        verify(collection).createIndex(
                argThat((Document key) -> key.equals(V4_25_4_WorkerEngineUnique.indexKeys().get("connector"))),
                argThat((IndexOptions options) -> V4_25_4_WorkerEngineUnique.CONNECTOR_INDEX_NAME.equals(options.getName())
                        && options.isUnique()
                        && new Document("worker_type", "connector").append("deleted", false)
                        .append("process_id", new Document("$gt", "")).equals(options.getPartialFilterExpression())));
        verify(collection).createIndex(
                argThat((Document key) -> key.equals(V4_25_4_WorkerEngineUnique.indexKeys().get("transformer"))),
                argThat((IndexOptions options) -> V4_25_4_WorkerEngineUnique.TRANSFORMER_INDEX_NAME.equals(options.getName())
                        && options.isUnique()
                        && new Document("worker_type", "transformer").append("deleted", false)
                        .append("process_id", new Document("$gt", "")).equals(options.getPartialFilterExpression())));
    }

    @Test
    void runDoesNotRemoveDistinctProcessesAndSkipsMatchingIndex() {
        Document first = new Document("_id", new ObjectId()).append("process_id", "a").append("ping_time", 1L);
        Document second = new Document("_id", new ObjectId()).append("process_id", "b").append("ping_time", 2L);
        Document existingConnectorIndex = new Document("name", V4_25_4_WorkerEngineUnique.CONNECTOR_INDEX_NAME)
                .append("key", V4_25_4_WorkerEngineUnique.indexKeys().get("connector"))
                .append("unique", true)
                .append("partialFilterExpression", new Document("worker_type", "connector").append("deleted", false)
                        .append("process_id", new Document("$gt", "")));

        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mockCollection(mongoTemplate, List.of(first, second),
                List.of(existingConnectorIndex));

        runPatch(mongoTemplate);

        verify(mongoTemplate, never()).remove(any(Query.class), eq("Workers"));
        verify(collection, never()).dropIndex(V4_25_4_WorkerEngineUnique.CONNECTOR_INDEX_NAME);
        verify(collection, never()).createIndex(any(Document.class),
                argThat((IndexOptions options) -> V4_25_4_WorkerEngineUnique.CONNECTOR_INDEX_NAME.equals(options.getName())));
        verify(collection).createIndex(any(Document.class),
                argThat((IndexOptions options) -> V4_25_4_WorkerEngineUnique.TRANSFORMER_INDEX_NAME.equals(options.getName())));
    }

    @Test
    void runRecreatesIndexWithOutdatedDefinition() {
        Document outdated = new Document("name", V4_25_4_WorkerEngineUnique.TRANSFORMER_INDEX_NAME)
                .append("key", new Document("process_id", 1))
                .append("unique", true);
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mockCollection(mongoTemplate, List.of(), List.of(outdated));

        runPatch(mongoTemplate);

        verify(collection).dropIndex(V4_25_4_WorkerEngineUnique.TRANSFORMER_INDEX_NAME);
        verify(collection).createIndex(any(Document.class),
                argThat((IndexOptions options) -> V4_25_4_WorkerEngineUnique.TRANSFORMER_INDEX_NAME.equals(options.getName())));
    }

    @Test
    void canonicalSelectionKeepsLatestHeartbeat() {
        Document live = new Document("_id", new ObjectId("6ae9795c885a7214937a8710")).append("ping_time", 300L);
        Document stale = new Document("_id", new ObjectId("6aebff69885a721493517b42")).append("ping_time", 100L);

        Document canonical = V4_25_2_WorkerApiServerUnique.selectCanonical(List.of(stale, live));

        assertEquals(live.getObjectId("_id"), canonical.getObjectId("_id"));
        assertNotEquals(stale.getObjectId("_id"), canonical.getObjectId("_id"));
    }

    private static void runPatch(MongoTemplate mongoTemplate) {
        V4_25_4_WorkerEngineUnique patch = new V4_25_4_WorkerEngineUnique(null, null);
        try (MockedStatic<SpringContextHelper> helper = mockStatic(SpringContextHelper.class)) {
            helper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            patch.run();
        }
    }

    /** Returns {@code connectorWorkers} for the connector query and nothing for transformer. */
    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> mockCollection(MongoTemplate mongoTemplate, List<Document> connectorWorkers,
                                                            List<Document> indexes) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexIterable = mock(ListIndexesIterable.class);
        FindIterable<Document> connectorFind = mock(FindIterable.class);
        FindIterable<Document> emptyFind = mock(FindIterable.class);
        when(mongoTemplate.getCollection("Workers")).thenReturn(collection);
        when(collection.listIndexes()).thenReturn(indexIterable);
        when(indexIterable.into(anyList())).thenAnswer(invocation -> {
            List<Document> result = invocation.getArgument(0);
            result.addAll(indexes);
            return result;
        });
        when(collection.find(any(Bson.class))).thenAnswer(invocation -> {
            Document query = invocation.getArgument(0);
            return "connector".equals(query.get("worker_type")) ? connectorFind : emptyFind;
        });
        when(connectorFind.into(anyList())).thenAnswer(invocation -> {
            List<Document> result = invocation.getArgument(0);
            result.addAll(new ArrayList<>(connectorWorkers));
            return result;
        });
        when(emptyFind.into(anyList())).thenAnswer(invocation -> invocation.getArgument(0));
        return collection;
    }
}
