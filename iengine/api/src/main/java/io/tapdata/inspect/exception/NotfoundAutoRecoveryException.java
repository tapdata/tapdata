package io.tapdata.inspect.exception;

import lombok.Getter;

/**
 * 自动修复实例不存在异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/21 12:22 Create
 */
@Getter
public class NotfoundAutoRecoveryException extends AutoRecoveryException {

    private final String taskId;

    public NotfoundAutoRecoveryException(String taskId) {
        super(String.format("Not found auto-recovery instance by taskId: %s", taskId));
        this.taskId = taskId;
    }

    public static <T> T failed(String taskId) {
        throw new NotfoundAutoRecoveryException(taskId);
    }
}
