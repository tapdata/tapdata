package com.tapdata.tm.init.patches;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchTypeEnums;
import com.tapdata.tm.init.PatchVersion;
import io.tapdata.utils.AppType;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JsonFilePatchTest {

    @Test
    void shouldSkipEquivalentAuditIndexesEvenWhenVersionAndBackgroundDiffer() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("_id", 1)).append("name", "_id_"),
                new Document("v", 2).append("key", new Document("eventId", 1)).append("name", "eventId_1").append("sparse", true),
                new Document("v", 2).append("key", new Document("operationId", 1).append("timestamp", 1))
                        .append("name", "operationId_1_timestamp_1").append("sparse", true),
                new Document("v", 2).append("key", new Document("type", 1).append("timestamp", -1).append("eventId", -1))
                        .append("name", "type_1_timestamp_-1_eventId_-1")
        ));
        mockCollectionIndexes(mongoTemplate, "AuditOperations", List.of(
                new Document("v", 2).append("key", new Document("_id", 1)).append("name", "_id_"),
                new Document("v", 2).append("key", new Document("createdAt", 1)).append("name", "createdAt_ttl")
                        .append("expireAfterSeconds", 604800L)
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "eventId_1", "key": { "eventId": 1 }, "background": true, "sparse": true },
                      { "name": "operationId_1_timestamp_1", "key": { "operationId": 1, "timestamp": 1 }, "background": true, "sparse": true },
                      { "name": "type_1_timestamp_-1_eventId_-1", "key": { "type": 1, "timestamp": -1, "eventId": -1 }, "background": true }
                    ]
                  },
                  {
                    "createIndexes": "AuditOperations",
                    "indexes": [
                      { "name": "createdAt_ttl", "key": { "createdAt": 1 }, "expireAfterSeconds": 604800, "background": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        verify(mongoTemplate, never()).executeCommand(anyString());
    }

    @Test
    void shouldTreatDifferentNumericRepresentationsInKeyAsEquivalent() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("eventId", 1.0d).append("timestamp", -1.0d))
                        .append("name", "eventId_1_timestamp_-1")
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "eventId_1_timestamp_-1", "key": { "eventId": 1, "timestamp": -1 }, "background": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        verify(mongoTemplate, never()).executeCommand(anyString());
    }

    @Test
    void shouldDropAndRecreateWhenCompoundKeyOrderChanges() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("timestamp", 1).append("operationId", 1))
                        .append("name", "operationId_1_timestamp_1").append("sparse", true)
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "operationId_1_timestamp_1", "key": { "operationId": 1, "timestamp": 1 }, "background": true, "sparse": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        ArgumentCaptor<String> commands = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(2)).executeCommand(commands.capture());
        assertTrue(commands.getAllValues().get(0).contains("dropIndexes"));
        assertTrue(commands.getAllValues().get(1).contains("createIndexes"));
    }

    @Test
    void shouldDropAndRecreateWhenSemanticIndexOptionsChange() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "AuditOperations", List.of(
                new Document("v", 2).append("key", new Document("createdAt", 1)).append("name", "createdAt_ttl")
                        .append("expireAfterSeconds", 86400L)
        ));

        String script = """
                [
                  {
                    "createIndexes": "AuditOperations",
                    "indexes": [
                      { "name": "createdAt_ttl", "key": { "createdAt": 1 }, "expireAfterSeconds": 604800, "background": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        ArgumentCaptor<String> commands = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(2)).executeCommand(commands.capture());
        assertTrue(commands.getAllValues().get(0).contains("dropIndexes"));
        assertTrue(commands.getAllValues().get(1).contains("createIndexes"));
    }

    @Test
    void shouldSkipWhenEquivalentIndexExistsUnderDifferentName() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("eventId", 1))
                        .append("name", "legacy_event_idx").append("sparse", true)
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "eventId_1", "key": { "eventId": 1 }, "background": true, "sparse": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        verify(mongoTemplate, never()).executeCommand(anyString());
    }

    @Test
    void shouldCreateWithoutDropWhenIndexDoesNotExist() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("_id", 1)).append("name", "_id_")
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "eventId_1", "key": { "eventId": 1 }, "background": true, "sparse": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        ArgumentCaptor<String> commands = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(1)).executeCommand(commands.capture());
        String cmd = commands.getValue();
        assertFalse(cmd.contains("dropIndexes"));
        assertTrue(cmd.contains("createIndexes"));
        assertTrue(cmd.contains("eventId_1"));
    }

    @Test
    void shouldCreateNewIndexMatchingOldDefinitionOfPlannedDropIndexWhenListedAfter() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("_id", 1)).append("name", "_id_"),
                new Document("v", 2).append("key", new Document("x", 1).append("y", 1)).append("name", "a")
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "a", "key": { "x": 1 }, "background": true },
                      { "name": "b", "key": { "x": 1, "y": 1 }, "background": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        ArgumentCaptor<String> commands = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(2)).executeCommand(commands.capture());
        String dropCmd = commands.getAllValues().get(0);
        String createCmd = commands.getAllValues().get(1);
        assertTrue(dropCmd.contains("dropIndexes") && dropCmd.contains("\"a\""));
        assertTrue(createCmd.contains("createIndexes") && createCmd.contains("\"a\"") && createCmd.contains("\"b\""));
    }

    @Test
    void shouldCreateNewIndexMatchingOldDefinitionOfPlannedDropIndexWhenListedBefore() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockCollectionIndexes(mongoTemplate, "UserLogs", List.of(
                new Document("v", 2).append("key", new Document("_id", 1)).append("name", "_id_"),
                new Document("v", 2).append("key", new Document("x", 1).append("y", 1)).append("name", "a")
        ));

        String script = """
                [
                  {
                    "createIndexes": "UserLogs",
                    "indexes": [
                      { "name": "b", "key": { "x": 1, "y": 1 }, "background": true },
                      { "name": "a", "key": { "x": 1 }, "background": true }
                    ]
                  }
                ]
                """;

        newPatch(mongoTemplate, script).run();

        ArgumentCaptor<String> commands = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(2)).executeCommand(commands.capture());
        String dropCmd = commands.getAllValues().get(0);
        String createCmd = commands.getAllValues().get(1);
        assertTrue(dropCmd.contains("dropIndexes") && dropCmd.contains("\"a\""));
        assertTrue(createCmd.contains("createIndexes") && createCmd.contains("\"b\"") && createCmd.contains("\"a\""));
    }

    private JsonFilePatch newPatch(MongoTemplate mongoTemplate, String script) {
        return new JsonFilePatch(
                new PatchType(AppType.DAAS, PatchTypeEnums.Script),
                PatchVersion.valueOf("4.25-4"),
                "4.25-4.json",
                script,
                Collections.emptyMap(),
                mongoTemplate
        );
    }

    @SuppressWarnings("unchecked")
    private void mockCollectionIndexes(MongoTemplate mongoTemplate, String collectionName, List<Document> indexDocs) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ListIndexesIterable<Document> iterable = mock(ListIndexesIterable.class);
        when(mongoTemplate.getCollection(collectionName)).thenReturn(collection);
        when(collection.listIndexes()).thenReturn(iterable);
        when(iterable.iterator()).thenAnswer(invocation -> {
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            var iterator = indexDocs.iterator();
            when(cursor.hasNext()).thenAnswer(i -> iterator.hasNext());
            when(cursor.next()).thenAnswer(i -> iterator.next());
            return cursor;
        });
    }
}
