package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RoleMappingServiceTest {
    RoleMappingService service;
    @BeforeEach
    void init() {
        service = mock(RoleMappingService.class);
    }

    @Nested
    class UpdateUserRoleMappingTest {
        List<RoleMappingDto> roleDto;
        UserDetail userDetail;
        RoleMappingDto dto ;
        @BeforeEach
        void init() {
            dto = mock(RoleMappingDto.class);
            userDetail = mock(UserDetail.class);
            roleDto = new ArrayList<>();
            roleDto.add(null);
            roleDto.add(dto);

            when(dto.getRoleId()).thenReturn(new ObjectId());
            when(dto.getPrincipalId()).thenReturn("id");
            when(dto.getPrincipalType()).thenReturn("type");
            when(service.upsert(any(Query.class), any(RoleMappingDto.class), any(UserDetail.class))).thenReturn(0L);
            when(service.updateUserRoleMapping(roleDto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            when(service.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Assertions.assertNotNull(service.updateUserRoleMapping(roleDto, userDetail));
            verify(service).upsert(any(Query.class), any(RoleMappingDto.class), any(UserDetail.class));
            verify(service).findAll(any(Query.class));
        }

        @Test
        void testEmpty() {
            roleDto.clear();
            when(service.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            List<RoleMappingDto> roleMappingDto = service.updateUserRoleMapping(roleDto, userDetail);
            Assertions.assertNotNull(roleMappingDto);
            Assertions.assertEquals(0, roleMappingDto.size());
            verify(service, times(0)).upsert(any(Query.class), any(RoleMappingDto.class), any(UserDetail.class));
            verify(service, times(0)).findAll(any(Query.class));
        }

        @Test
        void testResultIsEmpty() {
            when(service.findAll(any(Query.class))).thenReturn(null);
            List<RoleMappingDto> roleMappingDto = service.updateUserRoleMapping(roleDto, userDetail);
            Assertions.assertNotNull(roleMappingDto);
            Assertions.assertEquals(0, roleMappingDto.size());
            verify(service).upsert(any(Query.class), any(RoleMappingDto.class), any(UserDetail.class));
            verify(service).findAll(any(Query.class));
        }
    }
}