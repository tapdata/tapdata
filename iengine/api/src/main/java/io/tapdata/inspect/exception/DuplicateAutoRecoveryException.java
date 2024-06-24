package io.tapdata.inspect.exception;

import io.tapdata.inspect.AutoRecovery;
import lombok.Getter;

/**
 * 自动修复数据实例重复异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/21 11:42 Create
 */
@Getter
public class DuplicateAutoRecoveryException extends AutoRecoveryException {

    private final String taskId;

    public DuplicateAutoRecoveryException(String taskId) {
        super(String.format("Task %s auto-recovery instance already exists.", taskId));
        this.taskId = taskId;
    }

    public static void assertNull(AutoRecovery ins) {
        if (null != ins) {
            throw new DuplicateAutoRecoveryException(ins.getTaskId());
        }
    }
}
