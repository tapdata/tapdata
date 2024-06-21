package io.tapdata.inspect.exception;

/**
 * 自动修复数据异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/21 11:40 Create
 */
public class AutoRecoveryException extends RuntimeException {
    public AutoRecoveryException() {
    }

    public AutoRecoveryException(String message) {
        super(message);
    }

    public AutoRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public AutoRecoveryException(Throwable cause) {
        super(cause);
    }
}
