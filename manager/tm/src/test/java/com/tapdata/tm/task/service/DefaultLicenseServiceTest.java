package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class DefaultLicenseServiceTest {
    private DefaultLicenseService defaultLicenseService;
    @BeforeEach
    void beforeEach(){
        defaultLicenseService = mock(DefaultLicenseService.class);
    }
    @Test
    void testCheckTaskPipelineLimit(){
        TaskDto taskDto = mock(TaskDto.class);
        UserDetail user = mock(UserDetail.class);
        doCallRealMethod().when(defaultLicenseService).checkTaskPipelineLimit(taskDto,user);
        boolean actual = defaultLicenseService.checkTaskPipelineLimit(taskDto, user);
        assertTrue(actual);
    }
}
