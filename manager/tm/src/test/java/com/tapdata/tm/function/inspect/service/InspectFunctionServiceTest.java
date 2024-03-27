package com.tapdata.tm.function.inspect.service;


import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.function.inspect.dto.InspectFunctionDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

class InspectFunctionServiceTest {
    @Test
    void testBeforeSaveNormal() {
        InspectFunctionDto functionDto = mock(InspectFunctionDto.class);
        UserDetail user = mock(UserDetail.class);
        InspectFunctionService service = mock(InspectFunctionService.class);
        doCallRealMethod().when(service).beforeSave(functionDto, user);
        Assertions.assertDoesNotThrow(() -> service.beforeSave(functionDto, user));
    }
}