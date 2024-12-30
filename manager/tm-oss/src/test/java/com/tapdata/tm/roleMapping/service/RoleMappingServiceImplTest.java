package com.tapdata.tm.roleMapping.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.*;
import java.util.List;
import java.util.ArrayList;
import com.tapdata.tm.config.security.UserDetail;

class RoleMappingServiceImplTest {
    RoleMappingServiceImpl service;
    @BeforeEach
    void init() {
        service = mock(RoleMappingServiceImpl.class);
    }

    @Nested
    class RemoveRoleFromUserTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(service).removeRoleFromUser(anyString());
            Assertions.assertDoesNotThrow(() -> service.removeRoleFromUser("Id"));
        }
    }
    @Test
    void testAddUserLogIfNeed() {
        doCallRealMethod().when(service).addUserLogIfNeed(anyList(), any(UserDetail.class));
        Assertions.assertDoesNotThrow(() -> service.addUserLogIfNeed(new ArrayList(), mock(UserDetail.class)));
    }
}