package com.tapdata.tm.Settings.constant;

public enum KeyEnum {
    NOTIFICATION("notification"),
    JOB_HEART_TIMEOUT("jobHeartTimeout"),
    WORKER_HEART_TIMEOUT("lastHeartbeat"),

    EMAIL_HREF("emailHref"),
    ALLOW_CONNECTION_TYPE("ALLOW_CONNECTION_TYPE"),
    EMAIL_RECEIVER("email.receivers"),
    EMAIL_TITLE_PREFIX("email.title.prefix"),
    BUILD_PROFILE("buildProfile"),
    SHARE_AGENT_CREATE_USER("shareAgentCreateUser"),
    SUPPORT_CDC_CONNECTION("supportCdcConnection"),
    SHARE_AGENT_EXPRIRE_DAYS("shareAgentExprireDays"),
    LDAP_LOGIN_ENABLE("ldap.login.enable"),
    LDAP_SSL_ENABLE("ldap.ssl.enable"),
    LOGIN_SINGLE_SESSION("login.single.session"),
    LOGIN_BRIEF_TIPS("login.brief.tips"),
    TASK_START_TRANSFORM_WAIT_SECONDS("task.start.transformWaitSeconds"),
    TOKEN_IDLE_TIMEOUT_MINUTES("access.token.idle.timeout.minutes"),

    // ---- Generic, IdP-agnostic SAML 2.0 SSO configuration (single active config) ----
    SAML_LOGIN_ENABLE("saml.login.enable"),
    SAML_SP_ENTITY_ID("saml.sp.entityId"),
    SAML_SP_ACS_URL("saml.sp.acsUrl"),
    SAML_SP_PRIVATE_KEY("saml.sp.privateKey"),
    SAML_SP_CERTIFICATE("saml.sp.certificate"),
    SAML_IDP_ENTITY_ID("saml.idp.entityId"),
    SAML_IDP_SSO_URL("saml.idp.ssoUrl"),
    SAML_IDP_SLO_URL("saml.idp.sloUrl"),
    SAML_IDP_SIGNING_CERTIFICATE("saml.idp.signingCertificate"),
    SAML_NAME_ID_FORMAT("saml.nameIdFormat"),
    SAML_WANT_ASSERTIONS_SIGNED("saml.wantAssertionsSigned"),
    SAML_SIGN_AUTHN_REQUEST("saml.signAuthnRequest"),
    SAML_SIGNATURE_ALGORITHM("saml.signatureAlgorithm"),
    SAML_CLOCK_SKEW_SECONDS("saml.clockSkewSeconds"),
    SAML_CLAIM_USERNAME("saml.claim.username"),
    SAML_CLAIM_EMAIL("saml.claim.email"),
    SAML_CLAIM_DISPLAY_NAME("saml.claim.displayName"),
    SAML_CLAIM_GROUPS("saml.claim.groups"),
    SAML_IDP_INITIATED_ENABLED("saml.idpInitiatedEnabled"),
    SAML_JIT_PROVISIONING_ENABLED("saml.jitProvisioningEnabled"),
    SAML_LOGIN_REDIRECT_URL("saml.loginRedirectUrl")
    ;

    private String value;

    KeyEnum(String value) {
        this.value = value;
    }


    public String getValue() {
        return value;
    }
}
