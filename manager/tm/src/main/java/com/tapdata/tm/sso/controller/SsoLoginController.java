package com.tapdata.tm.sso.controller;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.annotation.IgnoreLogin;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
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
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLDecoder;
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
    private static final String SAML_BASE_PATH = "/api/sso/saml";
    /** SPA hash route that consumes the access_token and completes the login. */
    private static final String DEFAULT_CALLBACK_REDIRECT = "/#/sso-callback";

    private SamlConfigService samlConfigService;
    private SamlAuthnRequestService samlAuthnRequestService;
    private SamlResponseValidator samlResponseValidator;
    private SamlIdentityResolver samlIdentityResolver;
    private SamlLogoutService samlLogoutService;
    private SamlSessionService samlSessionService;
    private AccessTokenService accessTokenService;
    private UserService userService;
    private MongoTemplate mongoTemplate;

    @Operation(summary = "Whether SAML SSO login is enabled (for showing the login button)")
    @GetMapping("/enabled")
    public ResponseMessage<Boolean> enabled() {
        return success(samlConfigService.isEnabled());
    }

    @Operation(summary = "Start SP-Initiated SAML login (redirect to IdP)")
    @GetMapping("/login")
    public void login(@RequestParam(value = "relayState", required = false) String relayState,
                      HttpServletResponse response) throws IOException {
        if (!isSafeLocalRedirect(relayState)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid RelayState");
            return;
        }
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
    @PostMapping(value = "/acs", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    public void acs(HttpServletRequest request,
                    HttpServletResponse response) throws IOException {

        // The upstream global RequestFilter reads and buffers the body, which prevents the
        // servlet container from parsing form parameters. Parse the (replayable) body here
        // instead of relying on @RequestParam.
        String body = readBody(request);
        String samlResponse = parseFormParameter(body, "SAMLResponse");
        String relayState = parseFormParameter(body, "RelayState");

        if (StringUtils.isBlank(samlResponse)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "SAMLResponse missing");
            return;
        }
        SamlConfig config = samlConfigService.getConfig();
        if (!config.isEnabled()) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "SAML login is not enabled");
            return;
        }
        try {
            if (!isSafeLocalRedirect(relayState)) {
                throw new SamlValidationException("Invalid RelayState");
            }
            // Never honour a RelayState that points back at the SAML endpoints themselves.
            // An IdP-initiated login carries an IdP-supplied RelayState; if it targets
            // /api/sso/saml/login the success redirect would re-start SP-initiated login
            // and loop indefinitely. Drop it so buildSuccessRedirect falls back to the
            // configured SPA callback URL.
            if (isSamlEndpointRedirect(relayState)) {
                relayState = null;
            }
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
        if (!isSafeLocalRedirect(relayState)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid RelayState");
            return;
        }
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

    /** Only a same-origin absolute path may be carried through an untrusted IdP. */
    private boolean isSafeLocalRedirect(String relayState) {
        return StringUtils.isBlank(relayState)
                || (relayState.startsWith("/") && !relayState.startsWith("//")
                && (relayState.length() == 1 || relayState.charAt(1) != '\\'));
    }

    /**
     * Whether the target points back at the SAML endpoints. Honouring such a value
     * as the post-login target would redirect to SP-initiated login again and loop.
     * Accepts both a local path (e.g. {@code /api/sso/saml/login}) and an absolute
     * URL (e.g. {@code https://host/api/sso/saml/login}); only the path is inspected.
     */
    private boolean isSamlEndpointRedirect(String target) {
        if (StringUtils.isBlank(target)) {
            return false;
        }
        String path = target;
        // Strip scheme://host so an absolute URL is compared on its path only.
        int schemeCut = path.indexOf("://");
        if (schemeCut >= 0) {
            int pathStart = path.indexOf('/', schemeCut + 3);
            path = pathStart >= 0 ? path.substring(pathStart) : "/";
        }
        int cut = path.indexOf('?');
        if (cut >= 0) {
            path = path.substring(0, cut);
        }
        cut = path.indexOf('#');
        if (cut >= 0) {
            path = path.substring(0, cut);
        }
        return path.equals(SAML_BASE_PATH) || path.startsWith(SAML_BASE_PATH + "/");
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
        // Neither the RelayState nor the configured loginRedirectUrl may point back at the
        // SAML endpoints: doing so would re-start SP-initiated login and loop indefinitely.
        // Both are guarded here so a misconfigured loginRedirectUrl (e.g. .../api/sso/saml/login)
        // still falls back to the SPA callback route instead of looping.
        String configured = config.getLoginRedirectUrl();
        String base = StringUtils.isNotBlank(relayState) && !isSamlEndpointRedirect(relayState) ? relayState
                : StringUtils.isNotBlank(configured) && !isSamlEndpointRedirect(configured) ? configured
                : DEFAULT_CALLBACK_REDIRECT;
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

    private String readBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            char[] buffer = new char[1024];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, read);
            }
        }
        return sb.toString();
    }

    /**
     * Extract a single field from an application/x-www-form-urlencoded body. Returns the
     * URL-decoded value of the first occurrence of {@code name}, or {@code null} if absent.
     */
    private String parseFormParameter(String body, String name) {
        if (StringUtils.isBlank(body)) {
            return null;
        }
        for (String pair : body.split("&")) {
            int idx = pair.indexOf('=');
            String key = idx >= 0 ? pair.substring(0, idx) : pair;
            if (name.equals(URLDecoder.decode(key, StandardCharsets.UTF_8))) {
                String value = idx >= 0 ? pair.substring(idx + 1) : "";
                return URLDecoder.decode(value, StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
