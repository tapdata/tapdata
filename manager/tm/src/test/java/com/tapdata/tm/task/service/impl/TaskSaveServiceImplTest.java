package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;

public class TaskSaveServiceImplTest {
    private TaskSaveServiceImpl taskSaveService;
    private MetadataInstancesService metadataInstancesService;

    @BeforeEach
    void beforeEach() {
        taskSaveService = mock(TaskSaveServiceImpl.class);
        metadataInstancesService = mock(MetadataInstancesService.class);
        ReflectionTestUtils.setField(taskSaveService, "metadataInstancesService", metadataInstancesService);
    }
    @Nested
    class syncTaskSettingTest {
        @Test
        void testSyncTaskSettingWhenTaskDtoIsNull() {
            UserDetail user = mock(UserDetail.class);
            doCallRealMethod().when(taskSaveService).syncTaskSetting(null, user);
            taskSaveService.syncTaskSetting(null, user);
            verify(metadataInstancesService, new Times(0)).findByTaskId(anyString(), any(UserDetail.class));
        }
    }
}
