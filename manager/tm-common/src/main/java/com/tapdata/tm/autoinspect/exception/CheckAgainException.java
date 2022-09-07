package com.tapdata.tm.autoinspect.exception;

import com.tapdata.tm.autoinspect.constants.CheckAgainStatus;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/6 11:07 Create
 */
public class CheckAgainException extends RuntimeException {
    CheckAgainException() {
        super();
    }

    CheckAgainException(String message) {
        super(message);
    }

    CheckAgainException(String message, Throwable cause) {
        super(message, cause);
    }

    CheckAgainException(Throwable cause) {
        super(cause);
    }

    public static void exNotScheduling(CheckAgainStatus status) {
        if (CheckAgainStatus.Scheduling != status) {
            throw new CheckAgainException("Status is " + status);
        }
    }
}
