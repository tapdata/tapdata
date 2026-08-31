package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;

/**
 * Validates a SAML Response returned to the ACS endpoint and, on success, extracts
 * the trusted subject. All security checks are enforced here: XML signature against
 * the IdP certificate, rejection of weak signature algorithms, Conditions time
 * window (with clock skew), audience restriction, subject confirmation
 * (Recipient / InResponseTo / NotOnOrAfter) and single-use (replay) enforcement.
 */
public interface SamlResponseValidator {

    /**
     * Validate a base64-encoded SAML Response (HTTP-POST binding).
     *
     * @param config              the active SAML configuration.
     * @param base64SamlResponse  the raw {@code SAMLResponse} form field value.
     * @param expectedInResponseTo the AuthnRequest id we issued, or {@code null} for
     *                             an IdP-initiated (unsolicited) response.
     * @return the validated subject.
     * @throws com.tapdata.tm.sso.service.SamlValidationException if any check fails.
     */
    SamlAuthenticatedSubject validate(SamlConfig config, String base64SamlResponse, String expectedInResponseTo);
}
