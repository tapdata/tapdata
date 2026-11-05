package com.tapdata.tm.sso.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Values extracted from an imported IdP SAML 2.0 metadata document.
 * <p>
 * Used to prefill the IdP-side fields of the SAML configuration. Any field may be
 * {@code null} when the metadata does not contain the corresponding element.
 */
@Data
@Builder
public class IdpMetadata {

    /** IdP entity ID (the {@code entityID} attribute of {@code EntityDescriptor}). */
    private String idpEntityId;
    /** HTTP-Redirect single sign-on service URL. */
    private String idpSsoUrl;
    /** HTTP-Redirect single logout service URL (optional). */
    private String idpSloUrl;
    /** IdP signing certificate (Base64 DER, no PEM headers) used to verify assertions. */
    private String idpSigningCertificate;
}
