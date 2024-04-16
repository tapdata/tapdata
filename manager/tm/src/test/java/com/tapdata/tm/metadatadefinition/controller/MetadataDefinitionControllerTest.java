package com.tapdata.tm.metadatadefinition.controller;

import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetadataDefinitionControllerTest {

    MetadataDefinitionController metadataDefinitionController;
    MetadataDefinitionService metadataDefinitionService;

    @BeforeEach
    void init() {
        metadataDefinitionController = mock(MetadataDefinitionController.class);
        metadataDefinitionService = mock(MetadataDefinitionService.class);
        ReflectionTestUtils.setField(metadataDefinitionController, "metadataDefinitionService", metadataDefinitionService);

        when(metadataDefinitionController.getLoginUser()).thenReturn(mock(UserDetail.class));
    }

    @Nested
    class BatchPushListTagsTest {
        BatchUpdateParam batchUpdateParam;

        @BeforeEach
        void init() {
            batchUpdateParam = mock(BatchUpdateParam.class);
            when(metadataDefinitionService.batchPushListTags(eq("Connections"), any(BatchUpdateParam.class))).thenReturn(Lists.newArrayList());
            when(metadataDefinitionController.batchPushListTags(eq("Connections"), any(BatchUpdateParam.class))).thenCallRealMethod();
            when(metadataDefinitionController.success(any(List.class))).thenReturn(mock(ResponseMessage.class));
        }

        /**
         * Test the normal behavior of the batchPushListTags method in the MetadataDefinitionController class.
         */
        @Test
        void testNormal() {
            // Assert that the batchPushListTags method does not throw any exception
            Assertions.assertDoesNotThrow(() -> metadataDefinitionController.batchPushListTags("Connections", batchUpdateParam));

            // Verify that the batchPushListTags method in the MetadataDefinitionService class is called once with the specified arguments
            verify(metadataDefinitionService, times(1)).batchPushListTags(eq("Connections"), any(BatchUpdateParam.class));
        }
    }

}