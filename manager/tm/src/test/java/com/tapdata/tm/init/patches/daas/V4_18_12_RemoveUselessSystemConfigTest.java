package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class V4_18_12_RemoveUselessSystemConfigTest {

    private V4_18_12_RemoveUselessSystemConfig patch;
    @BeforeEach
    void setUp() {
        PatchType type = mock(PatchType.class);
        PatchVersion version = mock(PatchVersion.class);
        patch = new V4_18_12_RemoveUselessSystemConfig(type, version);
    }

    @Nested
    class RunTest {
        MongoTemplate mongoTemplate;
        MongoCollection<Document> collection;

        @BeforeEach
        void setUp() {
            mongoTemplate = mock(MongoTemplate.class);
            collection = mock(MongoCollection.class);
            when(mongoTemplate.getCollection("Settings")).thenReturn(collection);
        }

        @Test
        @DisplayName("test normal run")
        void testNormalRun() {
            try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)) {
                springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
                when(collection.deleteMany(any(Document.class))).thenReturn(mock(DeleteResult.class));

                Assertions.assertDoesNotThrow(() -> patch.run());

                verify(mongoTemplate, times(1)).getCollection("Settings");
                verify(collection, times(2)).deleteMany(any(Document.class));
            }
        }

        @Test
        @DisplayName("test run with exception")
        void testRunWithException() {
            try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)) {
                springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
                when(collection.deleteMany(any(Document.class))).thenThrow(new RuntimeException("Test Exception"));

                Assertions.assertDoesNotThrow(() -> patch.run());

                verify(mongoTemplate, times(1)).getCollection("Settings");
                verify(collection, times(1)).deleteMany(any(Document.class));
            }
        }
    }
}
