package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V4_23_2_SupplementApiCallTTLTest {

    static {
        V4_23_2_SupplementApiCallTTLTest.class.getClassLoader()
                .setClassAssertionStatus("com.tapdata.tm.init.patches.daas.V4_23_2_SupplementApiCallTTL", true);
    }

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoCollection<Document> mongoCollection;

    @Mock
    private ListIndexesIterable<Document> listIndexesIterable;

    private V4_23_2_SupplementApiCallTTL patch;

    @BeforeEach
    void setUp() {
        patch = new V4_23_2_SupplementApiCallTTL(null, null);
    }

    @Test
    void constructorCreatesPatchWithExpectedAnnotation() {
        V4_23_2_SupplementApiCallTTL newPatch = new V4_23_2_SupplementApiCallTTL(null, null);
        PatchAnnotation annotation = V4_23_2_SupplementApiCallTTL.class.getAnnotation(PatchAnnotation.class);

        assertNotNull(newPatch);
        assertNotNull(annotation);
        assertEquals(AppType.DAAS, annotation.appType());
        assertEquals("4.23-2", annotation.version());
    }

    @Test
    void runDropsLegacyIndexesAndCreatesMissingTtlIndex() {
        List<Document> indexes = Arrays.asList(
                new Document("name", "createTime_1"),
                new Document("name", "received_date_1")
        );
        mockIndexList(indexes);

        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);

            patch.run();

            verify(mongoCollection).dropIndex("createTime_1");
            verify(mongoCollection).dropIndex("received_date_1");
            verify(mongoCollection).createIndex(
                    eq(new Document(V4_14_21_AddApiCallTTL.CREATE_TIME, 1)),
                    anyTtlOptions("ApiCall_1_ttl", 2592000L)
            );
        }
    }

    @Test
    void runDoesNotCreateTtlIndexWhenItAlreadyExists() {
        List<Document> indexes = Arrays.asList(
                new Document("name", "createTime_1"),
                new Document("name", "received_date_1"),
                new Document("name", "ApiCall_1_ttl")
        );
        mockIndexList(indexes);

        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);

            patch.run();

            verify(mongoCollection).dropIndex("createTime_1");
            verify(mongoCollection).dropIndex("received_date_1");
            verify(mongoCollection, never()).createIndex(any(Document.class), any(IndexOptions.class));
        }
    }

    @Test
    void runThrowsAssertionErrorWhenMongoTemplateIsMissing() {
        try (MockedStatic<SpringContextHelper> mockedHelper = mockStatic(SpringContextHelper.class)) {
            mockedHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(null);

            assertThrows(AssertionError.class, patch::run);
        }
    }

    @Test
    void dropIndexIfNeedSkipsDropWhenIndexDoesNotExist() {
        mockIndexList(Arrays.asList(new Document("name", "ApiCall_1_ttl")));

        patch.dropIndexIfNeed(mongoTemplate, "ApiCall", "createTime_1");

        verify(mongoCollection, never()).dropIndex("createTime_1");
    }

    @Test
    void dropIndexIfNeedSwallowsDropException() {
        mockIndexList(Arrays.asList(new Document("name", "createTime_1")));
        doThrow(new RuntimeException("drop failed")).when(mongoCollection).dropIndex("createTime_1");

        assertDoesNotThrow(() -> patch.dropIndexIfNeed(mongoTemplate, "ApiCall", "createTime_1"));

        verify(mongoCollection).dropIndex("createTime_1");
    }

    private void mockIndexList(List<Document> indexes) {
        when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
        when(mongoCollection.listIndexes()).thenReturn(listIndexesIterable);
        when(listIndexesIterable.into(anyList())).thenAnswer(invocation -> {
            List<Document> target = invocation.getArgument(0);
            target.addAll(indexes);
            return target;
        });
    }

    private IndexOptions anyTtlOptions(String indexName, long expireAfterSeconds) {
        return argThat(options ->
                indexName.equals(options.getName())
                        && Boolean.TRUE.equals(options.isBackground())
                        && Long.valueOf(expireAfterSeconds).equals(options.getExpireAfter(TimeUnit.SECONDS))
        );
    }
}
