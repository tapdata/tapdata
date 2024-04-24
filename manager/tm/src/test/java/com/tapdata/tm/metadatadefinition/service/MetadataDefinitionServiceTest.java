package com.tapdata.tm.metadatadefinition.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.utils.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MetadataDefinitionServiceTest {
    MetadataDefinitionService metadataDefinitionService;
    MongoTemplate mongoTemplate;

    @BeforeEach
    void init() {
        metadataDefinitionService = mock(MetadataDefinitionService.class);
        mongoTemplate = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(metadataDefinitionService, "mongoTemplate", mongoTemplate);
    }

    @Nested
    class BatchPushListTagsTest {
        BatchUpdateParam batchUpdateParam;
        @BeforeEach
        void init() {
            batchUpdateParam = mock(BatchUpdateParam.class);

            when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), any(Class.class))).thenReturn(mock(UpdateResult.class));
            when(batchUpdateParam.getId()).thenReturn(Lists.newArrayList());
            when(batchUpdateParam.getListtags()).thenReturn(Lists.newArrayList());
        }

        void doVerify(String tableName, int times) {
            when(metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam)).thenCallRealMethod();
            metadataDefinitionService.batchPushListTags(tableName, batchUpdateParam);

            verify(batchUpdateParam, times(1)).getId();
            verify(batchUpdateParam, times(1)).getListtags();
            verify(mongoTemplate, times(times)).updateMulti(any(Query.class), any(Update.class), any(Class.class));
        }

        @Test
        void testConnections() {
            doVerify("Connections", 1);
        }

        @Test
        void testTask() {
            doVerify("Task", 1);
        }

        @Test
        void testModules() {
            doVerify("Modules", 1);
        }

        @Test
        void testOther() {
            doVerify("", 0);
        }
    }
}
