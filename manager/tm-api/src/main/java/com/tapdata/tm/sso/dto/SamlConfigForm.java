package com.tapdata.tm.sso.dto;

import lombok.Data;

/**
 * Request payload for saving the single, IdP-agnostic SAML SSO configuration.
 * <p>
 * Mirrors {@link SamlConfig} but represents client input. The SP private key is
 * only accepted here (write-only); it is never returned on read (see
 * {@link SamlConfigView}). A blank {@code spPrivateKey} on save means "keep the
 * existing stored key" so that clients can re-save other fields without having to
 * re-send the private key they can never read back (AC-053).
 */
@Data
public class SamlConfigForm {

    private Boolean enabled;

    // ---- Service Provider (TapData) ----
    private String spEntityId;
    private String spAcsUrl;
    private String spSloUrl;
    /** Write-only: PEM private key. Blank = keep existing stored key. */
    private String spPrivateKey;
    private String spCertificate;

    // ---- Identity Provider ----
    private String idpEntityId;
    private String idpSsoUrl;
    private String idpSloUrl;
    private String idpSigningCertificate;

    // ---- Protocol behavior ----
    private String nameIdFormat;
    private Boolean wantAssertionsSigned;
    private Boolean signAuthnRequest;
    private String signatureAlgorithm;
    private Integer clockSkewSeconds;
    private Boolean idpInitiatedEnabled;
    private Boolean jitProvisioningEnabled;
    private String loginRedirectUrl;

    // ---- Claim / attribute mapping ----
    private String claimUsername;
    private String claimEmail;
    private String claimDisplayName;
    private String claimGroups;
}
