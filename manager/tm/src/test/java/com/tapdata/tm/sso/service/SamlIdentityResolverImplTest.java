package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.entity.SsoExternalIdentity;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlIdentityResolverImplTest {

    private SamlIdentityResolverImpl resolver;

    @Mock
    private SamlConfigService samlConfigService;
    @Mock
    private UserService userService;
    @Mock
    private SamlProvisioningService samlProvisioningService;
    @Mock
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        resolver = new SamlIdentityResolverImpl();
        ReflectionTestUtils.setField(resolver, "samlConfigService", samlConfigService);
        ReflectionTestUtils.setField(resolver, "userService", userService);
        ReflectionTestUtils.setField(resolver, "samlProvisioningService", samlProvisioningService);
        ReflectionTestUtils.setField(resolver, "mongoTemplate", mongoTemplate);
    }

    private SamlAuthenticatedSubject subject() {
        SamlAuthenticatedSubject s = new SamlAuthenticatedSubject();
        s.setNameId("user@corp.com");
        s.setIdpEntityId("https://idp/entity");
        return s;
    }

    private User activeUser() {
        User user = new User();
        user.setId(new ObjectId());
        user.setEmail("user@corp.com");
        user.setAccountStatus(1);
        return user;
    }

    @Test
    @DisplayName("existing binding resolves directly to the bound user")
    void resolvesViaBinding() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().build());
        User user = activeUser();
        SsoExternalIdentity binding = new SsoExternalIdentity();
        binding.setTapdataUserId(user.getId().toHexString());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(binding);
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(user);

        assertEquals(user, resolver.resolve(subject()));
    }

    @Test
    @DisplayName("binding to a logically deleted user is rejected")
    void deletedBoundUserRejected() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().build());
        User deletedUser = activeUser();
        deletedUser.setIsDeleted(true);
        SsoExternalIdentity binding = new SsoExternalIdentity();
        binding.setTapdataUserId(deletedUser.getId().toHexString());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(binding);
        // The user query contains the isDeleted filter, so a deleted account is not returned.
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(null);

        SamlLoginException exception = assertThrows(SamlLoginException.class,
                () -> resolver.resolve(subject()));

        assertEquals(SamlLoginError.USER_NOT_FOUND.getCode(), exception.getCode());
        verify(samlProvisioningService, never()).provisionUser(any(), any(), any(), any());
        verify(mongoTemplate, never()).insert(any(SsoExternalIdentity.class));
    }

    @Test
    @DisplayName("no binding: resolves by email and creates a binding")
    void resolvesByEmailAndBinds() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().build());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(null);
        User user = activeUser();
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(user);

        assertEquals(user, resolver.resolve(subject()));
        verify(mongoTemplate).insert(any(SsoExternalIdentity.class));
        verify(samlProvisioningService, never()).provisionUser(any(), any(), any(), any());
    }

    @Test
    @DisplayName("no user + JIT disabled -> rejected (secure default)")
    void jitDisabledRejects() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().jitProvisioningEnabled(false).build());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(null);
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(null);

        assertThrows(SamlValidationException.class, () -> resolver.resolve(subject()));
        verify(samlProvisioningService, never()).provisionUser(any(), any(), any(), any());
    }

    @Test
    @DisplayName("no user + JIT enabled -> provisions user with roles from claimGroups, then binds")
    void jitEnabledProvisions() {
        Map<String, List<String>> attrs = new HashMap<>();
        attrs.put("groups", Arrays.asList("Analyst", "Engineer"));
        SamlAuthenticatedSubject s = subject();
        s.setAttributes(attrs);

        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().jitProvisioningEnabled(true).claimGroups("groups").build());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(null);
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(null);
        UserDetail actor = org.mockito.Mockito.mock(UserDetail.class);
        when(userService.loadUserByUsername("admin@admin.com")).thenReturn(actor);
        User created = activeUser();
        when(samlProvisioningService.provisionUser(eq("user@corp.com"), any(),
                eq(Arrays.asList("Analyst", "Engineer")), eq(actor))).thenReturn(created);

        assertEquals(created, resolver.resolve(s));
        verify(mongoTemplate).insert(any(SsoExternalIdentity.class));
    }

    @Test
    @DisplayName("disabled user is rejected")
    void disabledUserRejected() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().build());
        when(mongoTemplate.findOne(any(Query.class), eq(SsoExternalIdentity.class))).thenReturn(null);
        User user = activeUser();
        user.setAccountStatus(0);
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(user);

        assertThrows(SamlValidationException.class, () -> resolver.resolve(subject()));
    }
}
