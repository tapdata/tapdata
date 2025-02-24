package com.tapdata.tm.role.service;

import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.roleMapping.service.RoleMappingServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class RoleServiceImplTest {
    RoleMappingService roleMappingService;

    @BeforeEach
    void beforeEach() {
        roleMappingService = mock(RoleMappingServiceImpl.class);
        when(roleMappingService.checkHasPermission(any(PrincipleType.class), anyList(), anyString())).thenCallRealMethod();
    }
    @Test
    void test1(){
        List<ObjectId> idList=new ArrayList<>();
        boolean result = roleMappingService.checkHasPermission(PrincipleType.PERMISSION, idList, DataPermissionEnumsName.V2_CLUSTER_MANAGEMENT);
        assertEquals(true,result);
    }
}
