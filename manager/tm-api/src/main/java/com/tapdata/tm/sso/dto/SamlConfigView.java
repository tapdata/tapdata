package com.tapdata.tm.sso.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Read-only view of the SAML SSO configuration returned to clients.
 * <p>
 * Deliberately omits the SP private key value entirely and instead exposes only
 * {@link #spPrivateKeyConfigured} so the UI can indicate whether a key is present
 * without ever echoing it back (AC-053: private key non-echoable / non-downloadable).
 */
@Data
@Builder
public class SamlConfigView {

    private boolean enabled;

    // ---- Service Provider (TapData) ----
    private String spEntityId;
    private String spAcsUrl;
    private String spSloUrl;
    /** True when an SP private key is stored; the value itself is never returned. */
    private boolean spPrivateKeyConfigured;
    private String spCertificate;

    // ---- Identity Provider ----
    private String idpEntityId;
    private String idpSsoUrl;
    private String idpSloUrl;
    private String idpSigningCertificate;

    // ---- Protocol behavior ----
    private String nameIdFormat;
    private boolean wantAssertionsSigned;
    private boolean signAuthnRequest;
    private String signatureAlgorithm;
    private int clockSkewSeconds;
    private boolean idpInitiatedEnabled;
    private boolean jitProvisioningEnabled;
    private String loginRedirectUrl;

    // ---- Claim / attribute mapping ----
    private String claimUsername;
    private String claimEmail;
    private String claimDisplayName;
    private String claimGroups;
}
