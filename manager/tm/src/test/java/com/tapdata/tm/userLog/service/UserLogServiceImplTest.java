package com.tapdata.tm.userLog.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.constant.UserLogType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class UserLogServiceImplTest {
    private UserLogServiceImpl userLogService;
    @BeforeEach
    void beforeEach() {
        userLogService = mock(UserLogServiceImpl.class);
    }
    @Test
    void testAddUserLogWithSystemStart() {
        UserDetail userDetail = mock(UserDetail.class);
        doCallRealMethod().when(userLogService).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", true);
        userLogService.addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", true);
        verify(userLogService, times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, UserLogType.USER_OPERATION, "", null, false, true);
    }

}
