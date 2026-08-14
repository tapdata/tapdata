package com.tapdata.tm.sso.controller;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.annotation.IgnoreLogin;
import com.tapdata.tm.base.controller.BaseController;
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
import com.tapdata.tm.sso.service.SamlValidationException;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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
    private SamlLogoutService samlLogoutService;
    private SamlSessionService samlSessionService;
    private AccessTokenService accessTokenService;
    private UserService userService;
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
            // Align with the username/password login path: when single-session is enabled,
            // revoke the user's existing SAML tokens before issuing a new one so only one
            // active SAML session remains per user.
            if (userService.checkLoginSingleSessionEnable()) {
                accessTokenService.removeAccessTokenByAuthType(user.getId(), AuthType.SAML_LOGIN.getValue());
            }
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

    @Operation(summary = "Start SP-Initiated Single Logout (redirect LogoutRequest to IdP)")
    @GetMapping("/logout")
    public void logout(@RequestParam(value = "access_token", required = false) String accessToken,
                       @RequestParam(value = "relayState", required = false) String relayState,
                       HttpServletResponse response) throws IOException {
        SamlConfig config = samlConfigService.getConfig();
        SsoSession session = findSessionByAccessToken(accessToken);

        // Always terminate the local session first, so logout is effective even if the
        // IdP is unreachable or SLO is not configured (partial complete, AC-043/046).
        if (StringUtils.isNotBlank(accessToken)) {
            samlSessionService.terminateByAccessToken(accessToken);
        }

        boolean canReachIdp = config != null && config.isEnabled()
                && StringUtils.isNotBlank(config.getIdpSloUrl()) && session != null
                && StringUtils.isNotBlank(session.getNameId());
        if (!canReachIdp) {
            response.sendRedirect(buildPostLogoutRedirect(config, relayState));
            return;
        }
        try {
            LogoutRedirectResult result = samlLogoutService.buildLogoutRequest(
                    config, session.getNameId(), session.getSessionIndex(), relayState);
            response.sendRedirect(result.getRedirectUrl());
        } catch (Exception e) {
            log.warn("SP-Initiated SLO could not build LogoutRequest, completing locally: {}", e.getMessage());
            response.sendRedirect(buildPostLogoutRedirect(config, relayState));
        }
    }

    @Operation(summary = "SAML Single Logout endpoint (IdP LogoutRequest / LogoutResponse)")
    @GetMapping("/slo")
    public void slo(@RequestParam(value = "SAMLRequest", required = false) String samlRequest,
                    @RequestParam(value = "SAMLResponse", required = false) String samlResponse,
                    @RequestParam(value = "SigAlg", required = false) String sigAlg,
                    @RequestParam(value = "Signature", required = false) String signature,
                    @RequestParam(value = "RelayState", required = false) String relayState,
                    HttpServletRequest request,
                    HttpServletResponse response) throws IOException {
        SamlConfig config = samlConfigService.getConfig();
        if (config == null || !config.isEnabled()) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "SAML login is not enabled");
            return;
        }
        // LogoutResponse completes an SP-Initiated flow: nothing to terminate here.
        if (StringUtils.isNotBlank(samlResponse)) {
            response.sendRedirect(buildPostLogoutRedirect(config, relayState));
            return;
        }
        if (StringUtils.isBlank(samlRequest)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing SAMLRequest");
            return;
        }
        try {
            String signedQuery = buildSignedQuery(request, samlRequest, relayState, sigAlg);
            InboundLogout inbound = samlLogoutService.parseLogoutRequest(
                    config, samlRequest, sigAlg, signature, signedQuery);
            samlSessionService.terminate(inbound.getNameId(), inbound.getSessionIndex());
            LogoutRedirectResult result = samlLogoutService.buildLogoutResponse(
                    config, inbound.getRequestId(), relayState);
            response.sendRedirect(result.getRedirectUrl());
        } catch (SamlValidationException e) {
            log.warn("SAML SLO validation failed: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "SAML logout failed");
        } catch (Exception e) {
            log.error("SAML SLO processing error: {}", e.getMessage());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "SAML logout error");
        }
    }

    private SsoSession findSessionByAccessToken(String accessToken) {
        if (StringUtils.isBlank(accessToken)) {
            return null;
        }
        return mongoTemplate.findOne(
                Query.query(Criteria.where("accessTokenId").is(accessToken)), SsoSession.class);
    }

    /**
     * Reconstruct the exact signed portion of the redirect query (per the HTTP-Redirect
     * binding: SAMLRequest[&RelayState]&SigAlg, in that order, URL-encoded) so the SP can
     * verify the IdP signature over precisely the bytes the IdP signed.
     */
    private String buildSignedQuery(HttpServletRequest request, String samlRequest, String relayState, String sigAlg) {
        if (StringUtils.isBlank(sigAlg)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SAMLRequest=").append(URLEncoder.encode(samlRequest, StandardCharsets.UTF_8));
        if (StringUtils.isNotBlank(relayState)) {
            sb.append("&RelayState=").append(URLEncoder.encode(relayState, StandardCharsets.UTF_8));
        }
        sb.append("&SigAlg=").append(URLEncoder.encode(sigAlg, StandardCharsets.UTF_8));
        return sb.toString();
    }

    private String buildPostLogoutRedirect(SamlConfig config, String relayState) {
        if (StringUtils.isNotBlank(relayState)) {
            return relayState;
        }
        if (config != null && StringUtils.isNotBlank(config.getLoginRedirectUrl())) {
            return config.getLoginRedirectUrl();
        }
        return "/";
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
