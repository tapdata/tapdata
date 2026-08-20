package com.tapdata.tm.sso.service;

/**
 * Stable, non-sensitive reason codes for a refused SAML login. The {@code code} is
 * carried back to the SPA login page (as {@code sso_error=<code>}) so the frontend
 * can render a clear localized message. Codes must stay in sync with the frontend
 * i18n keys {@code app_signIn_ssoError_*}.
 */
public enum SamlLoginError {

    /** No TapData user matches the asserted subject (and JIT provisioning is off). */
    USER_NOT_FOUND("user_not_found"),

    /** The matched user account is disabled/frozen (accountStatus == 0). */
    USER_DISABLED("user_disabled"),

    /** The matched user account is pending approval (accountStatus == 2). */
    USER_PENDING("user_pending"),

    /** Any other SAML authentication failure (validation, config, etc.). */
    SSO_FAILED("sso_failed");

    private final String code;

    SamlLoginError(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
