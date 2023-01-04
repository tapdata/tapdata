package io.tapdata.connector.selectdb.exception;

/**
 * Author:Skeet
 * Date: 2022/12/14
 **/
public class SelectDbRunTimeException extends RuntimeException{
    public SelectDbRunTimeException(Throwable cause) {
        super(cause);
    }

    protected SelectDbRunTimeException(String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
