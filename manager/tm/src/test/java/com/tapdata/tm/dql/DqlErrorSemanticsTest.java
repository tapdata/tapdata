package com.tapdata.tm.dql;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.service.DqlEventPermissionService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class DqlErrorSemanticsTest {
    @Test
    void permissionFailureUsesStableBizErrorCode() {
        DqlEventPermissionService permissionService = new DqlEventPermissionService(mock(TaskService.class));

        BizException exception = assertThrows(BizException.class,
                () -> permissionService.checkTaskVisible("not-an-object-id", user()));

        assertEquals("NoPermission", exception.getErrorCode());
    }

    @Test
    void dqlMessagesAreAvailableForEnglishAndSimplifiedChinese() {
        assertEquals("Exception event not found: DQL-1",
                MessageUtil.getMessage(Locale.US, "DqlEvent.NotFound", "DQL-1"));
        assertEquals("异常事件不存在：DQL-1",
                MessageUtil.getMessage(Locale.CHINA, "DqlEvent.NotFound", "DQL-1"));
    }

    private UserDetail user() {
        return new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
    }
}
