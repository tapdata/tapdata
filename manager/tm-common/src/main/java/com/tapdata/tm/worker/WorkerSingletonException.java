package com.tapdata.tm.worker;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/17 14:30 Create
 */
public class WorkerSingletonException extends RuntimeException {
    public WorkerSingletonException(String message) {
        super(message);
    }
}
