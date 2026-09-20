package com.tapdata.tm.sso.service;

/**
 * A {@link SamlValidationException} that additionally carries a stable, non-sensitive
 * reason {@code code}. The code is safe to surface to the browser (as a query
 * parameter on the login redirect) so the SPA can show a clear, localized message
 * (e.g. user not found / account disabled) instead of an opaque failure.
 * <p>
 * The message still must never contain assertion contents or secret material.
 */
public class SamlLoginException extends SamlValidationException {

    /** Stable machine-readable reason code (see {@link SamlLoginError}). */
    private final String code;

    public SamlLoginException(SamlLoginError error, String message) {
        super(message);
        this.code = error == null ? SamlLoginError.SSO_FAILED.getCode() : error.getCode();
    }

    public String getCode() {
        return code;
    }
}
