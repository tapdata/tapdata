package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V4_17_5_DropUselessIndexTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoCollection<Document> mongoCollection;

    private V4_17_5_DropUselessIndex patch;

    @BeforeEach
    void setUp() {
        patch = new V4_17_5_DropUselessIndex(null, null);
    }

    @Test
    void run_dropIndexSucceeded() {
        try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
             MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
            springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            mongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallEntity.class)).thenReturn("ApiCall");
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);

            assertDoesNotThrow(patch::run);

            verify(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");
        }
    }

    @Test
    void run_dropIndexThrowsShouldBeIgnored() {
        try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
             MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
            springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            mongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallEntity.class)).thenReturn("ApiCall");
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
            doThrow(new RuntimeException("not exists")).when(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");

            assertDoesNotThrow(patch::run);

            verify(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");
        }
    }
}
