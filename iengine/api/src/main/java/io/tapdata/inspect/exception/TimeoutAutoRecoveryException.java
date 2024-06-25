package io.tapdata.inspect.exception;

import lombok.Getter;

/**
 * 自动修复超时异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/21 12:22 Create
 */
@Getter
public class TimeoutAutoRecoveryException extends AutoRecoveryException {

    private final String taskId;

    public TimeoutAutoRecoveryException(String taskId) {
        super(String.format("Timeout auto-recovery task '%s'", taskId));
        this.taskId = taskId;
    }

    public static void assertFalse(String taskId, boolean expected) {
        if (expected) {
            throw new TimeoutAutoRecoveryException(taskId);
        }
    }
}
