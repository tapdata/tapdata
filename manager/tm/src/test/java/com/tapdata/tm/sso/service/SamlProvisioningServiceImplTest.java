package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.user.dto.CreateUserRequest;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlProvisioningServiceImplTest {

    private SamlProvisioningServiceImpl service;

    @Mock
    private RoleService roleService;
    @Mock
    private UserService userService;
    @Mock
    private MongoTemplate mongoTemplate;

    private final UserDetail actor = org.mockito.Mockito.mock(UserDetail.class);

    @BeforeEach
    void setUp() {
        service = new SamlProvisioningServiceImpl();
        ReflectionTestUtils.setField(service, "roleService", roleService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
    }

    @Test
    @DisplayName("existing role is reused, no new role created")
    void reusesExistingRole() {
        RoleDto existing = new RoleDto();
        existing.setId(new ObjectId());
        existing.setName("Analyst");
        when(roleService.findOne(any(Query.class))).thenReturn(existing);

        String id = service.resolveOrCreateRoleId("Analyst", actor);

        assertEquals(existing.getId().toHexString(), id);
        verify(roleService, never()).save(any(RoleDto.class), any());
    }

    @Test
    @DisplayName("missing role is auto-created as an empty role with the SAML marker")
    void autoCreatesMissingRole() {
        when(roleService.findOne(any(Query.class))).thenReturn(null);
        RoleDto saved = new RoleDto();
        saved.setId(new ObjectId());
        ArgumentCaptor<RoleDto> captor = ArgumentCaptor.forClass(RoleDto.class);
        when(roleService.save(captor.capture(), any())).thenReturn(saved);

        String id = service.resolveOrCreateRoleId("Engineers", actor);

        assertEquals(saved.getId().toHexString(), id);
        assertEquals("Engineers", captor.getValue().getName());
        assertEquals(SamlProvisioningService.AUTO_ROLE_DESCRIPTION, captor.getValue().getDescription());
        assertEquals(Boolean.FALSE, captor.getValue().getRegisterUserDefault());
    }

    @Test
    @DisplayName("blank role name is rejected")
    void blankRoleRejected() {
        assertThrows(IllegalArgumentException.class, () -> service.resolveOrCreateRoleId("  ", actor));
    }

    @Test
    @DisplayName("provisions a password-less, active, verified SAML user with resolved roles")
    void provisionsUser() {
        RoleDto role = new RoleDto();
        role.setId(new ObjectId());
        when(roleService.findOne(any(Query.class))).thenReturn(role);
        User created = new User();
        created.setId(new ObjectId());
        when(mongoTemplate.findOne(any(Query.class), any())).thenReturn(created);

        ArgumentCaptor<CreateUserRequest> captor = ArgumentCaptor.forClass(CreateUserRequest.class);
        User result = service.provisionUser("john@corp.com", null, Arrays.asList("Analyst"), actor);

        verify(userService).save(captor.capture(), any());
        CreateUserRequest req = captor.getValue();
        assertEquals("john@corp.com", req.getEmail());
        assertEquals("john", req.getUsername());
        assertEquals(SamlProvisioningService.SOURCE_SAML, req.getSource());
        assertEquals(Integer.valueOf(1), req.getAccountStatus());
        assertEquals(Boolean.TRUE, req.getEmailVerified());
        assertTrue(req.getPassword() == null);
        assertEquals(1, req.getRoleusers().size());
        assertEquals(created.getId(), result.getId());
    }

    @Test
    @DisplayName("no role names -> user receives registered default roles")
    void provisionsUserWithoutRoles() {
        RoleDto defaultRole = new RoleDto();
        defaultRole.setId(new ObjectId());
        defaultRole.setRegisterUserDefault(true);
        when(roleService.findAll(any(Query.class))).thenReturn(List.of(defaultRole));
        User created = new User();
        created.setId(new ObjectId());
        when(mongoTemplate.findOne(any(Query.class), any())).thenReturn(created);

        service.provisionUser("nobody@corp.com", "nobody", null, actor);

        ArgumentCaptor<CreateUserRequest> captor = ArgumentCaptor.forClass(CreateUserRequest.class);
        verify(userService, times(1)).save(captor.capture(), any());
        assertEquals(List.of(defaultRole.getId().toHexString()), captor.getValue().getRoleusers());
    }

    @Test
    @DisplayName("blank email is rejected before any write")
    void blankEmailRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> service.provisionUser("  ", "x", List.of(), actor));
        verify(userService, never()).save(any(CreateUserRequest.class), any());
    }
}
