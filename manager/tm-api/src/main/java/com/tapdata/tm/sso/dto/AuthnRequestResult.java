package com.tapdata.tm.sso.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of building an SP-Initiated SAML AuthnRequest: the fully-formed
 * HTTP-Redirect URL to send the browser to, plus the generated request id so the
 * caller can persist it for later {@code InResponseTo} validation.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AuthnRequestResult {

    /** The IdP SSO URL with SAMLRequest (+ optional Signature) and RelayState query params. */
    private String redirectUrl;

    /** The AuthnRequest ID (used to validate InResponseTo on the returned assertion). */
    private String requestId;
}
