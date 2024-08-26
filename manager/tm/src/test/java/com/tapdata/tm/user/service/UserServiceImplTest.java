package com.tapdata.tm.user.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.user.service.UserServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class UserServiceImplTest {
    private UserServiceImpl userService;
    private RoleMappingService roleMappingService;
    @BeforeEach
    void beforeEach(){
        userService = mock(UserServiceImpl.class);
        roleMappingService = mock(RoleMappingService.class);
        ReflectionTestUtils.setField(userService,"roleMappingService",roleMappingService);
    }
    @Nested
    class updateRoleMapping{
        private String userId;
        private List<Object> roleusers;
        private UserDetail userDetail;
        @BeforeEach
        void beforeEach(){
            userId = "66c84372a5921a16459c2cef";
            roleusers = new ArrayList<>();
            userDetail = mock(UserDetail.class);
        }
        @Test
        void testForAdmin(){
            doCallRealMethod().when(userService).updateRoleMapping(userId,roleusers,userDetail);
            List<RoleMappingDto> actual = userService.updateRoleMapping(userId, roleusers, userDetail);
            verify(roleMappingService,new Times(0)).deleteAll(any(Query.class));
            assertNull(actual);
        }
        @Test
        void testForUser(){
            when(userDetail.getEmail()).thenReturn("test@tapdata.com");
            roleusers.add("5d31ae1ab953565ded04badd");
            doCallRealMethod().when(userService).updateRoleMapping(userId,roleusers,userDetail);
            List<RoleMappingDto> actual = userService.updateRoleMapping(userId, roleusers, userDetail);
            verify(roleMappingService,new Times(1)).deleteAll(any(Query.class));
            verify(roleMappingService,new Times(1)).updateUserRoleMapping(anyList(), any(UserDetail.class));
        }
    }
}
