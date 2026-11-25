package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.proxyprocess.service.impl.OracleLogParserAgentProcessProjectionService;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class V4_26_1_OracleLogParserAgentProcessSyncTest {

    private static final String INDEX_NAME =
            V4_26_1_OracleLogParserAgentProcessSync.UNIQUE_AGENT_ID_AGENT_TYPE_INDEX;

    private static final Document EXPECTED_KEY = new Document("agentId", 1).append("agentType", 1);

    private static MongoCollection<Document> collectionWithIndexes(List<Document> existingIndexes) {
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        when(mongoCollection.listIndexes()).thenReturn(indexes);
        when(indexes.into(anyList())).thenAnswer(invocation -> {
            List<Document> result = invocation.getArgument(0);
            result.addAll(existingIndexes);
            return result;
        });
        return mongoCollection;
    }

    private static MongoTemplate mongoTemplateWith(MongoCollection<Document> mongoCollection) {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        when(mongoTemplate.getCollection(V4_26_1_OracleLogParserAgentProcessSync.COLLECTION_NAME))
                .thenReturn(mongoCollection);
        return mongoTemplate;
    }

    @Test
    void createsCompoundUniqueIndexOnAgentIdAndAgentType() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of());
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);

        V4_26_1_OracleLogParserAgentProcessSync.createAgentProcessIndexes(mongoTemplate);

        verify(mongoCollection).createIndex(
                argThat((Document key) -> EXPECTED_KEY.equals(key)),
                argThat((IndexOptions options) -> INDEX_NAME.equals(options.getName()) && options.isUnique()));
        // Exactly one index: no single-column agentId / status / statusReportTime indexes.
        assertEquals(1, mockCreateCalls(mongoCollection));
    }

    @Test
    void replacesCompoundIndexThatWasNotUnique() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of(
                new Document("name", INDEX_NAME).append("key", EXPECTED_KEY)));
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);

        V4_26_1_OracleLogParserAgentProcessSync.createAgentProcessIndexes(mongoTemplate);

        InOrder order = inOrder(mongoCollection);
        order.verify(mongoCollection).dropIndex(INDEX_NAME);
        order.verify(mongoCollection).createIndex(
                any(Document.class),
                argThat((IndexOptions options) -> INDEX_NAME.equals(options.getName()) && options.isUnique()));
    }

    @Test
    void replacesCompoundIndexWithTheWrongKeyOrder() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of(
                new Document("name", INDEX_NAME)
                        .append("key", new Document("agentType", 1).append("agentId", 1))
                        .append("unique", true)));
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);

        V4_26_1_OracleLogParserAgentProcessSync.createAgentProcessIndexes(mongoTemplate);

        InOrder order = inOrder(mongoCollection);
        order.verify(mongoCollection).dropIndex(INDEX_NAME);
        order.verify(mongoCollection).createIndex(
                argThat((Document key) -> EXPECTED_KEY.equals(key)),
                any(IndexOptions.class));
    }

    @Test
    void keepsMatchingIndexUntouched() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of(
                new Document("name", INDEX_NAME).append("key", EXPECTED_KEY).append("unique", true)));
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);

        V4_26_1_OracleLogParserAgentProcessSync.createAgentProcessIndexes(mongoTemplate);

        verify(mongoCollection, never()).createIndex(any(Document.class), any(IndexOptions.class));
        verify(mongoCollection, never()).dropIndex(any(String.class));
    }

    @Test
    void indexIsCreatedBeforeBackfill() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of());
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);
        OracleLogParserAgentProcessProjectionService projectionService =
                mock(OracleLogParserAgentProcessProjectionService.class);
        when(projectionService.initializeLatestReports()).thenReturn(0);

        V4_26_1_OracleLogParserAgentProcessSync patch = new V4_26_1_OracleLogParserAgentProcessSync(null, null);
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            mockedHelper.when(() -> SpringContextHelper.getBean(OracleLogParserAgentProcessProjectionService.class))
                    .thenReturn(projectionService);

            patch.run();
        }

        InOrder order = inOrder(mongoCollection, projectionService);
        order.verify(mongoCollection).createIndex(any(Document.class), any(IndexOptions.class));
        order.verify(projectionService).initializeLatestReports();
    }

    @Test
    void uniqueIndexFailureStopsThePatch() {
        MongoCollection<Document> mongoCollection = collectionWithIndexes(List.of());
        when(mongoCollection.createIndex(any(Document.class), any(IndexOptions.class)))
                .thenThrow(new RuntimeException("E11000 duplicate key error collection: tapdata.AgentProcess"));
        MongoTemplate mongoTemplate = mongoTemplateWith(mongoCollection);

        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> V4_26_1_OracleLogParserAgentProcessSync.createAgentProcessIndexes(mongoTemplate));

        assertTrue(failure.getMessage().contains("E11000"));
    }

    private static int mockCreateCalls(MongoCollection<Document> mongoCollection) {
        return org.mockito.Mockito.mockingDetails(mongoCollection).getInvocations().stream()
                .filter(invocation -> "createIndex".equals(invocation.getMethod().getName()))
                .toList()
                .size();
    }
}
