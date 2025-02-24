package com.tapdata.tm.Permission.service;

import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class PermissionServiceImplTest {
    PermissionService permissionService;
    @BeforeEach
    void beforeEach(){
        permissionService=mock(PermissionServiceImpl.class);

    }
    @Test
    void test1(){
        when(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, "testId")).thenCallRealMethod();
        boolean b = permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT, "testId");
        assertEquals(true,b);
    }

}
