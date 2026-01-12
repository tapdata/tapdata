package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class V4_12_11_AddApiCallTTLTest {

    @Mock
    private MongoTemplate mongoTemplate;
    
    @Mock
    private MongoCollection<Document> mongoCollection;
    
    @Mock
    private ListIndexesIterable<Document> listIndexesIterable;
    
    private V4_12_11_AddApiCallTTL patch;

    @BeforeEach
    void setUp() {
        patch = new V4_12_11_AddApiCallTTL(null, null);
    }

    @Test
    void testConstructor() {
        V4_12_11_AddApiCallTTL newPatch = new V4_12_11_AddApiCallTTL(null, null);
        assertNotNull(newPatch);
    }

    @Test
    void testRunWithNoExistingIndexes() {
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
            when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
            when(listIndexesIterable.into(anyList())).thenReturn(new ArrayList<>());

            patch.run();

            verify(mongoCollection, times(5)).createIndex(any(Document.class), any(IndexOptions.class));
        }
    }

    @Test
    void testRunWithExistingIndexes() {
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
            when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
            
            ArrayList<Document> existingIndexes = new ArrayList<>(Arrays.asList(
                new Document("name", "ApiCall_1_ttl"),
                new Document("name", "ApiCall_query-1"),
                new Document("name", "ApiCall_query-2"),
                new Document("name", "ApiCall_query_supplement-3"),
                new Document("name", "ApiCallInWorker_query-1")
            ));
            when(listIndexesIterable.into(anyList())).thenReturn(existingIndexes);

            patch.run();

            verify(mongoCollection, never()).createIndex(any(Document.class), any(IndexOptions.class));
        }
    }

    @Test
    void testCreateTTLIndexIfNeedWithTTLIndex() {
        ArrayList<Document> indexes = new ArrayList<>();
        Document indexKey = new Document("createTime", 1);
        Long expireAfterSeconds = 2592000L;

        when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);

        patch.createTTLIndexIfNeed(mongoTemplate, indexes, "ApiCall_1_ttl", indexKey, expireAfterSeconds);

        verify(mongoCollection).createIndex(eq(indexKey), argThat(options -> 
            "ApiCall_1_ttl".equals(options.getName()) && 
            options.getExpireAfter(TimeUnit.SECONDS).equals(expireAfterSeconds)
        ));
    }

    @Test
    void testCreateTTLIndexIfNeedWithoutTTL() {
        ArrayList<Document> indexes = new ArrayList<>();
        Document indexKey = new Document("workOid", 1).append("reqTime", 1);

        when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);

        patch.createTTLIndexIfNeed(mongoTemplate, indexes, "ApiCall_query-2", indexKey, null);

        verify(mongoCollection).createIndex(eq(indexKey), argThat(options -> 
            "ApiCall_query-2".equals(options.getName()) && 
            options.getExpireAfter(TimeUnit.SECONDS) == null
        ));
    }

    @Test
    void testCreateTTLIndexIfNeedIndexExists() {
        ArrayList<Document> indexes = new ArrayList<>(Arrays.asList(
            new Document("name", "ApiCall_1_ttl")
        ));
        Document indexKey = new Document("createTime", 1);

        patch.createTTLIndexIfNeed(mongoTemplate, indexes, "ApiCall_1_ttl", indexKey, 2592000L);

        verify(mongoTemplate, never()).getCollection(anyString());
        verify(mongoCollection, never()).createIndex(any(Document.class), any(IndexOptions.class));
    }

    @Test
    void testCreateTTLIndexIfNeedWithException() {
        ArrayList<Document> indexes = new ArrayList<>();
        Document indexKey = new Document("createTime", 1);

        when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
        when(mongoCollection.createIndex(any(Document.class), any(IndexOptions.class)))
            .thenThrow(new RuntimeException("Index creation failed"));

        assertDoesNotThrow(() -> 
            patch.createTTLIndexIfNeed(mongoTemplate, indexes, "ApiCall_1_ttl", indexKey, 2592000L)
        );

        verify(mongoCollection).createIndex(any(Document.class), any(IndexOptions.class));
    }

    @Test
    void testCreateTTLIndexIfNeedWithComplexIndexKey() {
        ArrayList<Document> indexes = new ArrayList<>();
        Document indexKey = new Document("supplement", 1)
            .append("createTime", 1)
            .append("allPathId", 1)
            .append("reqTime", 1);

        when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);

        patch.createTTLIndexIfNeed(mongoTemplate, indexes, "ApiCall_query_supplement-3", indexKey, null);

        verify(mongoCollection).createIndex(eq(indexKey), argThat(options -> 
            "ApiCall_query_supplement-3".equals(options.getName()) && 
            options.isBackground()
        ));
    }

    @Test
    void testCollectionNameConstant() {
        assertEquals("ApiCall", V4_12_11_AddApiCallTTL.COLLECTION_NAME);
    }
}