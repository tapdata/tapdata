package com.tapdata.tm.sso.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of building a SAML Single Logout redirect (a signed LogoutRequest for the
 * SP-Initiated flow, or a LogoutResponse for the IdP-Initiated flow): the fully-formed
 * HTTP-Redirect URL to send the browser to, plus the generated message id (for a
 * LogoutRequest, so the caller can correlate the returned LogoutResponse).
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogoutRedirectResult {

    /** The IdP SLO URL with SAMLRequest/SAMLResponse (+ optional Signature) and RelayState query params. */
    private String redirectUrl;

    /** The generated LogoutRequest message id (null for a LogoutResponse). */
    private String requestId;
}
