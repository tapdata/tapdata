package com.tapdata.tm.sso.controller;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.annotation.IgnoreLogin;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.sso.dto.AuthnRequestResult;
import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.entity.SsoSession;
import com.tapdata.tm.sso.service.SamlAuthnRequestService;
import com.tapdata.tm.sso.service.SamlConfigService;
import com.tapdata.tm.sso.service.SamlIdentityResolver;
import com.tapdata.tm.sso.service.SamlResponseValidator;
import com.tapdata.tm.sso.service.SamlValidationException;
import com.tapdata.tm.user.entity.User;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * Browser-facing SAML SSO endpoints: SP-Initiated login (redirect to the IdP) and
 * the Assertion Consumer Service (ACS) callback. The ACS endpoint is
 * {@link IgnoreLogin} because the user is not yet authenticated when the IdP posts
 * the assertion back. On success it issues an AccessToken, records an
 * {@link SsoSession} and redirects the browser to the SPA with the token.
 */
@Slf4j
@RestController
@RequestMapping("/api/sso/saml")
@IgnoreLogin
@Setter(onMethod_ = {@Autowired})
public class SsoLoginController extends BaseController {

    private static final String REQUEST_ID_COOKIE = "TAPDATA_SAML_REQ";

    private SamlConfigService samlConfigService;
    private SamlAuthnRequestService samlAuthnRequestService;
    private SamlResponseValidator samlResponseValidator;
    private SamlIdentityResolver samlIdentityResolver;
    private AccessTokenService accessTokenService;
    private MongoTemplate mongoTemplate;

    @Operation(summary = "Start SP-Initiated SAML login (redirect to IdP)")
    @GetMapping("/login")
    public void login(@RequestParam(value = "relayState", required = false) String relayState,
                      HttpServletResponse response) throws IOException {
        SamlConfig config = samlConfigService.getConfig();
        if (!config.isEnabled()) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "SAML login is not enabled");
            return;
        }
        AuthnRequestResult result = samlAuthnRequestService.buildRedirect(config, relayState);
        response.addCookie(buildRequestIdCookie(result.getRequestId()));
        response.sendRedirect(result.getRedirectUrl());
    }

    @Operation(summary = "Assertion Consumer Service (IdP posts SAML response here)")
    @PostMapping("/acs")
    public void acs(@RequestParam("SAMLResponse") String samlResponse,
                    @RequestParam(value = "RelayState", required = false) String relayState,
                    HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
        SamlConfig config = samlConfigService.getConfig();
        if (!config.isEnabled()) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "SAML login is not enabled");
            return;
        }
        try {
            String expectedInResponseTo = readRequestIdCookie(request);
            SamlAuthenticatedSubject subject =
                    samlResponseValidator.validate(config, samlResponse, expectedInResponseTo);
            User user = samlIdentityResolver.resolve(subject);
            AccessTokenDto token = accessTokenService.save(user, AuthType.SAML_LOGIN.getValue());
            recordSession(subject, user, token);
            clearRequestIdCookie(response);
            response.sendRedirect(buildSuccessRedirect(config, relayState, token.getId()));
        } catch (SamlValidationException e) {
            // Do not leak assertion contents; log message only (AC-055).
            log.warn("SAML ACS validation failed: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "SAML authentication failed");
        } catch (Exception e) {
            log.error("SAML ACS processing error: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "SAML authentication error");
        }
    }

    private void recordSession(SamlAuthenticatedSubject subject, User user, AccessTokenDto token) {
        SsoSession session = new SsoSession();
        session.setNameId(subject.getNameId());
        session.setSessionIndex(subject.getSessionIndex());
        session.setIdpEntityId(subject.getIdpEntityId());
        session.setAccessTokenId(token.getId());
        session.setUserId(user.getId().toHexString());
        session.setCreatedAt(new Date());
        session.setSessionNotOnOrAfter(subject.getSessionNotOnOrAfter());
        mongoTemplate.insert(session);
    }

    private String buildSuccessRedirect(SamlConfig config, String relayState, String tokenId) {
        String base = StringUtils.isNotBlank(relayState) ? relayState
                : StringUtils.isNotBlank(config.getLoginRedirectUrl()) ? config.getLoginRedirectUrl() : "/";
        String separator = base.contains("?") ? "&" : "?";
        return base + separator + "access_token=" + URLEncoder.encode(tokenId, StandardCharsets.UTF_8);
    }

    private Cookie buildRequestIdCookie(String requestId) {
        Cookie cookie = new Cookie(REQUEST_ID_COOKIE, requestId);
        cookie.setHttpOnly(true);
        cookie.setSecure(true);
        cookie.setPath("/");
        cookie.setMaxAge(600);
        return cookie;
    }

    private String readRequestIdCookie(HttpServletRequest request) {
        if (request.getCookies() == null) {
            return null;
        }
        for (Cookie cookie : request.getCookies()) {
            if (REQUEST_ID_COOKIE.equals(cookie.getName())) {
                return StringUtils.trimToNull(cookie.getValue());
            }
        }
        return null;
    }

    private void clearRequestIdCookie(HttpServletResponse response) {
        Cookie cookie = new Cookie(REQUEST_ID_COOKIE, "");
        cookie.setPath("/");
        cookie.setMaxAge(0);
        response.addCookie(cookie);
    }
}
