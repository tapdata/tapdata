package io.tapdata.dql.recovery;

/** Safe, payload-free validation error for the Engine recovery boundary. */
public class DqlRecoveryMessageValidationException extends IllegalArgumentException {
    public DqlRecoveryMessageValidationException(String message) {
        super(message);
    }
}
