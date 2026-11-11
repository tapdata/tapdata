package com.tapdata.tm.audit;

import com.tapdata.tm.application.controller.ApplicationController;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.user.controller.UserController;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.mockito.MockedStatic;
import org.springframework.aop.aspectj.annotation.AnnotationAwareAspectJAutoProxyCreator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** L2: actual controller responses must not expose credentials outside audited reveal operations. */
class AuditCredentialExposureIT {
    @AfterEach
    void clearContext() {
        AuditWebContext.end();
    }

    @Test
    void ordinary_user_and_client_responses_hide_secrets_but_reveal_returns_them() throws Exception {
        String id = new ObjectId().toHexString();
        UserDetail login = mock(UserDetail.class);
        when(login.getUserId()).thenReturn(id);
        UserDto user = new UserDto();
        user.setUsername("test-user");
        user.setAccessCode("test-access-code");
        UserService users = mock(UserService.class);
        when(users.getUserDetail(id)).thenReturn(user);
        when(users.findById(new ObjectId(id))).thenReturn(user);
        when(users.refreshAccessCode(login)).thenReturn("new-test-access-code");
        UserController userController = spy(new UserController());
        ReflectionTestUtils.setField(userController, "userService", users);
        doReturn(login).when(userController).getLoginUser();
        MockMvc usersHttp = MockMvcBuilders.standaloneSetup(userController).build();
        String details = usersHttp.perform(get("/api/users/{id}", id))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertThat(details).contains("test-user").doesNotContain("test-access-code", "accessCode");
        String reveal = usersHttp.perform(post("/api/users/access-code/reveal"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertThat(reveal).contains("test-access-code");
        String refreshed = usersHttp.perform(post("/api/users/refreshAccessCode"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertThat(refreshed).doesNotContain("new-test-access-code");
        assertThat(UserController.class.getMethod("revealAccessCode").getAnnotation(Audited.class).value())
                .isEqualTo(AuditAction.USER_VIEW_ACCESS_CODE);

        ApplicationDto client = new ApplicationDto();
        client.setClientName("test-client");
        client.setClientSecret("test-client-secret");
        ApplicationService applications = mock(ApplicationService.class);
        when(applications.find(any(Filter.class), same(login))).thenReturn(Page.page(List.of(client), 1));
        when(applications.findById(eq(new ObjectId(id)), any(Field.class), same(login))).thenReturn(client);
        ApplicationController applicationController = spy(new ApplicationController());
        ReflectionTestUtils.setField(applicationController, "applicationService", applications);
        doReturn(login).when(applicationController).getLoginUser();
        try (MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class)) {
            permissions.when(() -> DataPermissionHelper.check(any(), any(), any(), any(), isNull(), any(), any()))
                    .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
            permissions.when(() -> DataPermissionHelper.checkOfQuery(any(), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
            MockMvc applicationsHttp = MockMvcBuilders.standaloneSetup(applicationController).build();
            String listing = applicationsHttp.perform(get("/api/Applications"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            assertThat(listing).contains("test-client", "\"clientSecretConfigured\":true")
                    .doesNotContain("test-client-secret", "\"clientSecret\":");
            String clientReveal = applicationsHttp.perform(post("/api/Applications/{id}/secret/reveal", id))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            assertThat(clientReveal).contains("test-client-secret");
            String userReveal = usersHttp.perform(post("/api/users/{id}/access-code/reveal", id))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            assertThat(userReveal).contains("test-access-code");
            clearInvocations(users);
            permissions.reset();
            permissions.when(() -> DataPermissionHelper.checkOfQuery(any(), any(), any(), any(), any(), any(), any()))
                    .thenThrow(new BizException("NotAuthorized"));
            assertThatThrownBy(() -> usersHttp.perform(post("/api/users/{id}/access-code/reveal", id)))
                    .hasRootCauseInstanceOf(BizException.class);
            verify(users, never()).findById(new ObjectId(id));
        }
        assertThat(ApplicationController.class.getMethod("revealSecret", jakarta.servlet.http.HttpServletRequest.class, String.class)
                .getAnnotation(Audited.class).value()).isEqualTo(AuditAction.API_CLIENT_VIEW_SECRET);
    }

    @Test
    void explicit_reveal_is_audited_without_recording_the_secret() throws Exception {
        String id = new ObjectId().toHexString();
        String otherId = new ObjectId().toHexString();
        UserDetail login = AuditItFixture.user(id, "auditor@example.test", "username_login");
        UserDto user = new UserDto();
        user.setAccessCode("test-private-value");
        UserService users = mock(UserService.class);
        when(users.findById(new ObjectId(id))).thenReturn(user);
        when(users.findById(new ObjectId(otherId))).thenReturn(user);
        UserController target = spy(new UserController());
        ReflectionTestUtils.setField(target, "userService", users);
        doReturn(login).when(target).getLoginUser();
        ApplicationDto client = new ApplicationDto();
        client.setClientSecret("test-client-private-value");
        ApplicationService applications = mock(ApplicationService.class);
        when(applications.findById(eq(new ObjectId(id)), any(Field.class), same(login))).thenReturn(client);
        ApplicationController appTarget = spy(new ApplicationController());
        ReflectionTestUtils.setField(appTarget, "applicationService", applications);
        doReturn(login).when(appTarget).getLoginUser();
        AuditItFixture fixture = new AuditItFixture();
        AuditWebContext.begin("", "127.0.0.1", null, login);
        try (GenericApplicationContext context = new GenericApplicationContext();
             MockedStatic<DataPermissionHelper> permissions = mockStatic(DataPermissionHelper.class)) {
            permissions.when(() -> DataPermissionHelper.checkOfQuery(any(), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(call -> ((Supplier<?>) call.getArgument(5)).get());
            context.registerBean(AnnotationAwareAspectJAutoProxyCreator.class);
            context.registerBean(AuditAspect.class,
                    () -> new AuditAspect(fixture.auditEventService, fixture.objectResolver, fixture.registry));
            context.registerBean(UserController.class, () -> target);
            context.registerBean(ApplicationController.class, () -> appTarget);
            context.refresh();
            assertThat(context.getBean(UserController.class).revealAccessCode().getData()).isEqualTo("test-private-value");
            assertThat(fixture.only(AuditAction.USER_VIEW_ACCESS_CODE)).satisfies(event -> {
                assertThat(event.getOutcome()).isEqualTo("SUCCESS");
                assertThat(event.getObjectId()).isEqualTo(id);
                assertThat(event.getObjectName()).isEqualTo("auditor@example.test");
            });
            assertThat(context.getBean(UserController.class).revealUserAccessCode(otherId).getData())
                    .isEqualTo("test-private-value");
            assertThat(fixture.events(AuditAction.USER_VIEW_ACCESS_CODE)).hasSize(2)
                    .extracting(event -> event.getObjectId()).containsExactly(id, otherId);
            assertThat(context.getBean(ApplicationController.class).revealSecret(null, id).getData())
                    .isEqualTo("test-client-private-value");
            assertThat(fixture.only(AuditAction.API_CLIENT_VIEW_SECRET).getOutcome()).isEqualTo("SUCCESS");
            assertThat(new ObjectMapper().writeValueAsString(fixture.events))
                    .doesNotContain("test-private-value", "test-client-private-value");
        }
    }

    @Test
    void credential_fields_remain_writable_for_explicit_create_or_update() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        assertThat(mapper.readValue("{\"accessCode\":\"new-code\"}", UserDto.class).getAccessCode())
                .isEqualTo("new-code");
        assertThat(mapper.readValue("{\"clientSecret\":\"new-secret\"}", ApplicationDto.class).getClientSecret())
                .isEqualTo("new-secret");
        assertThat(mapper.writeValueAsString(new ApplicationDto())).contains("\"clientSecretConfigured\":false");
    }
}
