package com.tapdata.tm.init;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.service.VersionService;
import io.tapdata.utils.UnitTestUtils;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.boot.ApplicationArguments;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlTtlIndexPatchTest {

    @Test
    @DisplayName("DAAS 4.22-7 upgrade installs both DLQ TTL indexes and advances the script version")
    void upgradeFromPreviousVersionInstallsDqlTtlIndexes() throws Exception {
        PatchesRunner runner = new PatchesRunner();
        VersionService versionService = mock(VersionService.class);
        MongoTemplate mongoTemplate = mongoTemplateWithoutIndexes();
        when(versionService.findOne(any(Query.class))).thenReturn(new VersionDto(
                VersionDto.DAAS_SCRIPT_VERSION_KEY, "4.22-6"));
        when(mongoTemplate.executeCommand(anyString())).thenReturn(new Document("ok", 1));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "versionService", versionService);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "productList", List.of("idaas"));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "mongodbUri", "mongodb://localhost/tapdata");

        try (MockedStatic<SpringContextHelper> springContext = mockStatic(SpringContextHelper.class);
             MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
            springContext.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);

            runner.run(mock(ApplicationArguments.class));

            initLogMap.verify(() -> InitLogMap.complete(PatchesRunner.class));
        }

        ArgumentCaptor<String> commandCaptor = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, atLeastOnce()).executeCommand(commandCaptor.capture());
        List<Document> commands = commandCaptor.getAllValues().stream().map(Document::parse).toList();
        assertTtlIndex(commands, "dql_events", "idx_dql_event_ttl");
        assertTtlIndex(commands, "dql_recovery_batches", "idx_dql_batch_ttl");

        ArgumentCaptor<VersionDto> versionCaptor = ArgumentCaptor.forClass(VersionDto.class);
        verify(versionService, atLeastOnce()).upsert(any(Query.class), versionCaptor.capture());
        VersionDto finalVersion = versionCaptor.getAllValues().get(versionCaptor.getAllValues().size() - 1);
        assertEquals(VersionDto.DAAS_SCRIPT_VERSION_KEY, finalVersion.getType());
        assertTrue(PatchVersion.valueOf(finalVersion.getVersion()).compareTo(PatchVersion.valueOf("4.22-7")) >= 0);
    }

    @Test
    @DisplayName("re-executing 4.22-7 does not recreate matching DLQ TTL indexes")
    void rerunningPatchWithMatchingIndexesIsIdempotent() throws Exception {
        PatchesRunner runner = new PatchesRunner();
        VersionService versionService = mock(VersionService.class);
        MongoTemplate mongoTemplate = mongoTemplateWithDqlIndexes();
        when(versionService.findOne(any(Query.class))).thenReturn(new VersionDto(
                VersionDto.DAAS_SCRIPT_VERSION_KEY, "4.22-6"));
        when(mongoTemplate.executeCommand(anyString())).thenReturn(new Document("ok", 1));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "versionService", versionService);
        UnitTestUtils.injectField(PatchesRunner.class, runner, "productList", List.of("idaas"));
        UnitTestUtils.injectField(PatchesRunner.class, runner, "mongodbUri", "mongodb://localhost/tapdata");

        try (MockedStatic<SpringContextHelper> springContext = mockStatic(SpringContextHelper.class);
             MockedStatic<InitLogMap> initLogMap = mockStatic(InitLogMap.class)) {
            springContext.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            runner.run(mock(ApplicationArguments.class));
            initLogMap.verify(() -> InitLogMap.complete(PatchesRunner.class));
        }

        ArgumentCaptor<String> commandCaptor = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, atLeastOnce()).executeCommand(commandCaptor.capture());
        assertTrue(commandCaptor.getAllValues().stream()
                .map(Document::parse)
                .noneMatch(command -> command.containsKey("createIndexes")
                        || command.containsKey("dropIndexes")));
    }

    private void assertTtlIndex(List<Document> commands, String collectionName, String indexName) {
        Document command = commands.stream()
                .filter(candidate -> collectionName.equals(candidate.getString("createIndexes")))
                .findFirst()
                .orElse(null);
        assertNotNull(command);
        List<Document> indexes = command.getList("indexes", Document.class);
        assertEquals(1, indexes.size());
        Document index = indexes.get(0);
        assertEquals(new Document("ttl_at", 1), index.get("key"));
        assertEquals(1_209_600, ((Number) index.get("expireAfterSeconds")).intValue());
        assertEquals(indexName, index.getString("name"));
    }

    @SuppressWarnings("unchecked")
    private MongoTemplate mongoTemplateWithoutIndexes() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(mongoTemplate.getCollection(anyString())).thenReturn(collection);
        when(collection.listIndexes()).thenReturn(indexes);
        when(indexes.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(false);
        return mongoTemplate;
    }

    @SuppressWarnings("unchecked")
    private MongoTemplate mongoTemplateWithDqlIndexes() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        mockIndexes(mongoTemplate, "dql_events", "idx_dql_event_ttl");
        mockIndexes(mongoTemplate, "dql_recovery_batches", "idx_dql_batch_ttl");
        return mongoTemplate;
    }

    private void mockIndexes(MongoTemplate mongoTemplate, String collectionName, String indexName) {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(mongoTemplate.getCollection(collectionName)).thenReturn(collection);
        when(collection.listIndexes()).thenReturn(indexes);
        when(indexes.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(new Document()
                .append("key", new Document("ttl_at", 1))
                .append("expireAfterSeconds", 1_209_600L)
                .append("name", indexName)
                .append("background", true));
    }
}
