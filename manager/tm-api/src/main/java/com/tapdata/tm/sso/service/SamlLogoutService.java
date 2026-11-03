package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.InboundLogout;
import com.tapdata.tm.sso.dto.LogoutRedirectResult;
import com.tapdata.tm.sso.dto.SamlConfig;

/**
 * Builds and validates SAML 2.0 Single Logout messages for the HTTP-Redirect binding.
 * <p>
 * SP-Initiated SLO builds a signed {@code LogoutRequest} to the IdP; IdP-Initiated SLO
 * validates an inbound {@code LogoutRequest} and builds the {@code LogoutResponse} back
 * to the IdP. The XML crypto (marshalling / DEFLATE / signature) mirrors the AuthnRequest
 * and response-validation paths so security behaviour (weak-algorithm rejection, replay
 * protection) is identical.
 */
public interface SamlLogoutService {

    /**
     * Build a signed LogoutRequest and the HTTP-Redirect URL to the IdP SLO service.
     *
     * @param config       the active SAML configuration (must have an IdP SLO URL).
     * @param nameId       the subject NameID to log out (required).
     * @param sessionIndex the IdP SessionIndex to target (optional).
     * @param relayState   opaque state echoed back by the IdP (may be {@code null}).
     * @return the redirect URL and the generated LogoutRequest id.
     */
    LogoutRedirectResult buildLogoutRequest(SamlConfig config, String nameId, String sessionIndex, String relayState);

    /**
     * Validate an inbound IdP-Initiated LogoutRequest (HTTP-Redirect binding) and return
     * the trusted subject/session correlation data.
     *
     * @param config     the active SAML configuration.
     * @param samlRequest the raw (Base64) SAMLRequest parameter.
     * @param sigAlg     the SigAlg query parameter (may be {@code null} if unsigned).
     * @param signature  the Signature query parameter (may be {@code null} if unsigned).
     * @param signedQuery the exact signed portion of the query string (for signature check).
     * @return the validated logout details.
     */
    InboundLogout parseLogoutRequest(SamlConfig config, String samlRequest, String sigAlg,
                                     String signature, String signedQuery);

    /**
     * Build a signed LogoutResponse (Success) and the HTTP-Redirect URL back to the IdP.
     *
     * @param config        the active SAML configuration.
     * @param inResponseTo  the id of the LogoutRequest being answered.
     * @param relayState    opaque state to echo back (may be {@code null}).
     * @return the redirect URL (requestId is {@code null} for a response).
     */
    LogoutRedirectResult buildLogoutResponse(SamlConfig config, String inResponseTo, String relayState);
}
