package com.tapdata.tm.sso.controller;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.sso.dto.AuthnRequestResult;
import com.tapdata.tm.sso.dto.InboundLogout;
import com.tapdata.tm.sso.dto.LogoutRedirectResult;
import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.entity.SsoSession;
import com.tapdata.tm.sso.service.SamlAuthnRequestService;
import com.tapdata.tm.sso.service.SamlConfigService;
import com.tapdata.tm.sso.service.SamlIdentityResolver;
import com.tapdata.tm.sso.service.SamlLogoutService;
import com.tapdata.tm.sso.service.SamlResponseValidator;
import com.tapdata.tm.sso.service.SamlSessionService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.sso.service.SamlValidationException;
import com.tapdata.tm.user.entity.User;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SsoLoginControllerTest {

    private SsoLoginController controller;

    @Mock
    private SamlConfigService samlConfigService;
    @Mock
    private SamlAuthnRequestService samlAuthnRequestService;
    @Mock
    private SamlResponseValidator samlResponseValidator;
    @Mock
    private SamlIdentityResolver samlIdentityResolver;
    @Mock
    private AccessTokenService accessTokenService;
    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private SamlLogoutService samlLogoutService;
    @Mock
    private SamlSessionService samlSessionService;
    @Mock
    private UserService userService;

    @BeforeEach
    void setUp() {
        controller = new SsoLoginController();
        ReflectionTestUtils.setField(controller, "samlConfigService", samlConfigService);
        ReflectionTestUtils.setField(controller, "samlAuthnRequestService", samlAuthnRequestService);
        ReflectionTestUtils.setField(controller, "samlResponseValidator", samlResponseValidator);
        ReflectionTestUtils.setField(controller, "samlIdentityResolver", samlIdentityResolver);
        ReflectionTestUtils.setField(controller, "accessTokenService", accessTokenService);
        ReflectionTestUtils.setField(controller, "mongoTemplate", mongoTemplate);
        ReflectionTestUtils.setField(controller, "samlLogoutService", samlLogoutService);
        ReflectionTestUtils.setField(controller, "samlSessionService", samlSessionService);
        ReflectionTestUtils.setField(controller, "userService", userService);
    }

    @Test
    @DisplayName("login redirects to IdP and sets the request-id cookie")
    void loginRedirects() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().enabled(true).build());
        when(samlAuthnRequestService.buildRedirect(any(), any()))
                .thenReturn(new AuthnRequestResult("https://idp/sso?SAMLRequest=abc", "_req-1"));
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.login("state", response);

        assertEquals("https://idp/sso?SAMLRequest=abc", response.getRedirectedUrl());
        assertTrue(response.getCookie("TAPDATA_SAML_REQ") != null);
    }

    @Test
    @DisplayName("login returns 403 when SAML disabled")
    void loginDisabled() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().enabled(false).build());
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.login(null, response);
        assertEquals(HttpServletResponse.SC_FORBIDDEN, response.getStatus());
    }

    @Test
    @DisplayName("ACS validates, issues token, records session and redirects with token")
    void acsHappyPath() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).loginRedirectUrl("https://tapdata/app").build());
        SamlAuthenticatedSubject subject = new SamlAuthenticatedSubject();
        subject.setNameId("user@corp.com");
        when(samlResponseValidator.validate(any(), eq("RESP"), any())).thenReturn(subject);
        User user = new User();
        user.setId(new ObjectId());
        when(samlIdentityResolver.resolve(subject)).thenReturn(user);
        AccessTokenDto token = new AccessTokenDto();
        token.setId("tok-123");
        when(accessTokenService.save(eq(user), anyString())).thenReturn(token);

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.acs("RESP", null, request, response);

        verify(mongoTemplate).insert(any(com.tapdata.tm.sso.entity.SsoSession.class));
        assertTrue(response.getRedirectedUrl().startsWith("https://tapdata/app?access_token=tok-123"));
    }

    @Test
    @DisplayName("ACS returns 401 on validation failure without leaking details")
    void acsValidationFailure() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().enabled(true).build());
        when(samlResponseValidator.validate(any(), anyString(), any()))
                .thenThrow(new SamlValidationException("signature invalid"));

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.acs("RESP", null, request, response);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
    }

    @Test
    @DisplayName("ACS revokes existing SAML tokens before issuing a new one when single-session is enabled")
    void acsEnforcesSingleSession() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).loginRedirectUrl("https://tapdata/app").build());
        SamlAuthenticatedSubject subject = new SamlAuthenticatedSubject();
        subject.setNameId("user@corp.com");
        when(samlResponseValidator.validate(any(), eq("RESP"), any())).thenReturn(subject);
        User user = new User();
        ObjectId userId = new ObjectId();
        user.setId(userId);
        when(samlIdentityResolver.resolve(subject)).thenReturn(user);
        when(userService.checkLoginSingleSessionEnable()).thenReturn(true);
        AccessTokenDto token = new AccessTokenDto();
        token.setId("tok-123");
        when(accessTokenService.save(eq(user), anyString())).thenReturn(token);

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.acs("RESP", null, request, response);

        verify(accessTokenService).removeAccessTokenByAuthType(eq(userId), eq("saml_login"));
        assertTrue(response.getRedirectedUrl().startsWith("https://tapdata/app?access_token=tok-123"));
    }

    @Test
    @DisplayName("ACS does not revoke existing SAML tokens when single-session is disabled")
    void acsSkipsSingleSessionWhenDisabled() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).loginRedirectUrl("https://tapdata/app").build());
        SamlAuthenticatedSubject subject = new SamlAuthenticatedSubject();
        subject.setNameId("user@corp.com");
        when(samlResponseValidator.validate(any(), eq("RESP"), any())).thenReturn(subject);
        User user = new User();
        user.setId(new ObjectId());
        when(samlIdentityResolver.resolve(subject)).thenReturn(user);
        when(userService.checkLoginSingleSessionEnable()).thenReturn(false);
        AccessTokenDto token = new AccessTokenDto();
        token.setId("tok-123");
        when(accessTokenService.save(eq(user), anyString())).thenReturn(token);

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.acs("RESP", null, request, response);

        verify(accessTokenService, never()).removeAccessTokenByAuthType(any(), anyString());
    }

    private SsoSession session(String tokenId, String nameId, String sessionIndex) {
        SsoSession s = new SsoSession();
        s.setAccessTokenId(tokenId);
        s.setNameId(nameId);
        s.setSessionIndex(sessionIndex);
        return s;
    }

    @Test
    @DisplayName("SP-Initiated logout terminates the session and redirects the LogoutRequest to the IdP")
    void logoutRedirectsToIdp() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).idpSloUrl("https://idp/slo").build());
        when(mongoTemplate.findOne(any(), eq(SsoSession.class)))
                .thenReturn(session("tok-1", "user@x", "idx-1"));
        when(samlLogoutService.buildLogoutRequest(any(), eq("user@x"), eq("idx-1"), any()))
                .thenReturn(new LogoutRedirectResult("https://idp/slo?SAMLRequest=abc", "_lr-1"));
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.logout("tok-1", null, response);

        verify(samlSessionService).terminateByAccessToken("tok-1");
        assertEquals("https://idp/slo?SAMLRequest=abc", response.getRedirectedUrl());
    }

    @Test
    @DisplayName("SP-Initiated logout completes locally (partial) when no IdP SLO URL is configured")
    void logoutPartialWhenNoSloUrl() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).loginRedirectUrl("https://tapdata/app").build());
        when(mongoTemplate.findOne(any(), eq(SsoSession.class)))
                .thenReturn(session("tok-1", "user@x", "idx-1"));
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.logout("tok-1", null, response);

        verify(samlSessionService).terminateByAccessToken("tok-1");
        assertEquals("https://tapdata/app", response.getRedirectedUrl());
    }

    @Test
    @DisplayName("IdP-Initiated SLO validates the LogoutRequest, terminates sessions and redirects a LogoutResponse")
    void sloHandlesLogoutRequest() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).idpSloUrl("https://idp/slo").build());
        InboundLogout inbound = new InboundLogout();
        inbound.setRequestId("_lr-1");
        inbound.setNameId("user@x");
        inbound.setSessionIndex("idx-1");
        when(samlLogoutService.parseLogoutRequest(any(), eq("REQ"), any(), any(), any())).thenReturn(inbound);
        when(samlLogoutService.buildLogoutResponse(any(), eq("_lr-1"), any()))
                .thenReturn(new LogoutRedirectResult("https://idp/slo?SAMLResponse=xyz", null));

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.slo("REQ", null, null, null, null, request, response);

        verify(samlSessionService).terminate("user@x", "idx-1");
        assertEquals("https://idp/slo?SAMLResponse=xyz", response.getRedirectedUrl());
    }

    @Test
    @DisplayName("SLO returns 401 on an invalid LogoutRequest signature")
    void sloRejectsInvalidRequest() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).idpSloUrl("https://idp/slo").build());
        when(samlLogoutService.parseLogoutRequest(any(), anyString(), any(), any(), any()))
                .thenThrow(new SamlValidationException("bad signature"));

        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        controller.slo("REQ", null, null, null, null, request, response);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
    }

    @Test
    @DisplayName("SLO with a LogoutResponse (SP-initiated completion) redirects home without termination")
    void sloHandlesLogoutResponse() throws Exception {
        when(samlConfigService.getConfig()).thenReturn(
                SamlConfig.builder().enabled(true).loginRedirectUrl("https://tapdata/app").build());
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.slo(null, "RESP", null, null, null, request, response);

        assertEquals("https://tapdata/app", response.getRedirectedUrl());
    }
}
