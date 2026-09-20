package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.AuthnRequestResult;
import com.tapdata.tm.sso.dto.SamlConfig;

/**
 * Builds SP-Initiated SAML 2.0 AuthnRequests for the HTTP-Redirect binding.
 */
public interface SamlAuthnRequestService {

    /**
     * Build an AuthnRequest and the HTTP-Redirect URL to the IdP SSO service.
     * <p>
     * When {@link SamlConfig#isSignAuthnRequest()} is true the redirect query is
     * signed with the SP private key per the HTTP-Redirect binding.
     *
     * @param config     the active SAML configuration (must be enabled/valid).
     * @param relayState opaque state echoed back by the IdP (may be {@code null}).
     * @return the redirect URL and the generated request id.
     */
    AuthnRequestResult buildRedirect(SamlConfig config, String relayState);
}
