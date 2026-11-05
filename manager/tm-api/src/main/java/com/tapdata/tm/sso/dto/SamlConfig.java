package com.tapdata.tm.sso.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Generic, IdP-agnostic SAML 2.0 Service Provider configuration.
 * <p>
 * Every field maps to a standard SAML 2.0 concept (SP / IdP / NameID / Assertion /
 * Signature) so that any standard IdP (ADFS, Microsoft Entra ID, Okta, Keycloak, ...)
 * can be integrated by supplying values only - there is no vendor-specific field or
 * branching. The SP private key is stored encrypted at rest and is never echoed back.
 */
@Data
@Builder
public class SamlConfig {

    /** Whether SAML login is enabled. Defaults to false (security closed by default). */
    @Builder.Default
    private boolean enabled = false;

    // ---- Service Provider (TapData) ----
    /** SP entity ID (issuer of AuthnRequest). */
    private String spEntityId;
    /** Assertion Consumer Service URL the IdP posts the SAML response to. */
    private String spAcsUrl;
    /** SP Single Logout Service URL (HTTP-Redirect binding) advertised in SP metadata. */
    private String spSloUrl;
    /** SP signing/decryption private key (PEM), decrypted for in-memory use only. */
    private String spPrivateKey;
    /** SP certificate (PEM) shared with the IdP for signature verification. */
    private String spCertificate;

    // ---- Identity Provider (any standard SAML IdP) ----
    /** IdP entity ID. */
    private String idpEntityId;
    /** IdP single sign-on service URL (SP-Initiated redirect target). */
    private String idpSsoUrl;
    /** IdP single logout service URL (optional, required for SLO). */
    private String idpSloUrl;
    /** IdP signing certificate (PEM) used to verify SAML assertions. */
    private String idpSigningCertificate;

    // ---- Protocol behavior (standard toggles) ----
    /** NameID format URI. Defaults to emailAddress. */
    @Builder.Default
    private String nameIdFormat = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress";
    /** Require the assertion to be signed. Defaults to true. */
    @Builder.Default
    private boolean wantAssertionsSigned = true;
    /** Whether the SP signs the AuthnRequest. Defaults to false. */
    @Builder.Default
    private boolean signAuthnRequest = false;
    /** Signature algorithm URI. Weak algorithms (e.g. SHA-1) must be rejected. */
    @Builder.Default
    private String signatureAlgorithm = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
    /** Allowed clock skew in seconds for timestamp validation. Defaults to 120. */
    @Builder.Default
    private int clockSkewSeconds = 120;
    /** Whether IdP-Initiated SSO (unsolicited response) is accepted. Defaults to false. */
    @Builder.Default
    private boolean idpInitiatedEnabled = false;
    /** Whether Just-In-Time user provisioning is allowed on first login. Defaults to false. */
    @Builder.Default
    private boolean jitProvisioningEnabled = false;
    /** SPA URL the browser is redirected to after a successful ACS login (token appended). */
    private String loginRedirectUrl;

    // ---- Claim / attribute mapping (generic, not vendor-bound) ----
    /** SAML attribute (or NameID) carrying the username. */
    private String claimUsername;
    /** SAML attribute carrying the email address. */
    private String claimEmail;
    /** SAML attribute carrying the display name (optional). */
    private String claimDisplayName;
    /** SAML attribute carrying the group/role memberships (used for role mapping). */
    private String claimGroups;
}
