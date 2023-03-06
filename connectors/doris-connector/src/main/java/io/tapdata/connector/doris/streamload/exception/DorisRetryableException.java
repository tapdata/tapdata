package io.tapdata.connector.doris.streamload.exception;

public class DorisRetryableException extends Exception {
    public DorisRetryableException() {
        super();
    }

    public DorisRetryableException(String message) {
        super(message);
    }

    public DorisRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public DorisRetryableException(Throwable cause) {
        super(cause);
    }

    protected DorisRetryableException(String message, Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
