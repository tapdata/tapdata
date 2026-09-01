package com.tapdata.tm.sso.service;

/**
 * Thrown when a SAML Response/Assertion fails any security validation check.
 * The message is safe to log but must never contain the full assertion or any
 * secret material (AC-055).
 */
public class SamlValidationException extends RuntimeException {

    public SamlValidationException(String message) {
        super(message);
    }

    public SamlValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
