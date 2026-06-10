package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V4_17_6_ResetApiCallIndexTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoCollection<Document> mongoCollection;

    private V4_17_6_ResetApiCallIndex patch;

    @BeforeEach
    void setUp() {
        patch = spy(new V4_17_6_ResetApiCallIndex(null, null));
    }

    @Nested
    class RunTest {
        @Test
        void testRunNormal() {
            try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {

                springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
                mongoUtils.when(() -> MongoUtils.getCollectionName(any(Class.class))).thenReturn("ApiCall");
                when(mongoTemplate.getCollection(anyString())).thenReturn(mongoCollection);

                doNothing().when(mongoCollection).dropIndexes();
                doNothing().when(patch).loadingClientId(mongoCollection);
                doNothing().when(patch).createTTLIndexIfNeed(any(), anyString(), anyString(), any(), any());

                assertDoesNotThrow(() -> patch.run());

                verify(mongoCollection).dropIndexes();
                verify(patch).loadingClientId(mongoCollection);
                verify(patch, times(15)).createTTLIndexIfNeed(any(), anyString(), anyString(), any(), any());
            }
        }

        @Test
        void testRunDropIndexThrowsException() {
            try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {

                springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
                mongoUtils.when(() -> MongoUtils.getCollectionName(any(Class.class))).thenReturn("ApiCall");
                when(mongoTemplate.getCollection(anyString())).thenReturn(mongoCollection);

                doThrow(new RuntimeException("Drop index failed")).when(mongoCollection).dropIndexes();
                doNothing().when(patch).loadingClientId(mongoCollection);
                doNothing().when(patch).createTTLIndexIfNeed(any(), anyString(), anyString(), any(), any());

                assertDoesNotThrow(() -> patch.run());

                verify(mongoCollection).dropIndexes();
                verify(patch).loadingClientId(mongoCollection);
                verify(patch, times(15)).createTTLIndexIfNeed(any(), anyString(), anyString(), any(), any());
            }
        }
    }

    @Nested
    class LoadingClientIdTest {
        @Test
        void testLoadingClientIdNormal() {
            UpdateResult updateResult = mock(UpdateResult.class);
            when(mongoCollection.updateMany(any(Document.class), any(Document.class))).thenReturn(updateResult);

            assertDoesNotThrow(() -> patch.loadingClientId(mongoCollection));

            verify(mongoCollection).updateMany(any(Document.class), any(Document.class));
        }

        @Test
        void testLoadingClientIdThrowsException() {
            when(mongoCollection.updateMany(any(Document.class), any(Document.class)))
                    .thenThrow(new RuntimeException("Update failed"));

            assertDoesNotThrow(() -> patch.loadingClientId(mongoCollection));

            verify(mongoCollection).updateMany(any(Document.class), any(Document.class));
        }
    }
}
