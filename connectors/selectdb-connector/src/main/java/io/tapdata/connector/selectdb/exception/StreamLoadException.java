package io.tapdata.connector.selectdb.exception;

/**
 * Author:Skeet
 * Date: 2022/12/14
 **/
public class StreamLoadException extends Exception {
    public StreamLoadException() {
        super();
    }

    public StreamLoadException(String message) {
        super(message);
    }

    public StreamLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public StreamLoadException(Throwable cause) {
        super(cause);
    }

    protected StreamLoadException(String message, Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
