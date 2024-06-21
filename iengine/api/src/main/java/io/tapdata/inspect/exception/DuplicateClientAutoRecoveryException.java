package io.tapdata.inspect.exception;

import io.tapdata.inspect.AutoRecoveryClient;
import lombok.Getter;

/**
 * 自动修复客户端异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/21 11:42 Create
 */
@Getter
public class DuplicateClientAutoRecoveryException extends AutoRecoveryException {

    private final String taskId;
    private final String inspectTaskId;

    public DuplicateClientAutoRecoveryException(String taskId, String inspectTaskId) {
        super(String.format("Task '%s-%s' auto-recovery client already exists.", taskId, inspectTaskId));
        this.taskId = taskId;
        this.inspectTaskId = inspectTaskId;
    }

    public static void assertNull(AutoRecoveryClient ins) {
        if (null != ins) {
            throw new DuplicateClientAutoRecoveryException(ins.getTaskId(), ins.getInspectTaskId());
        }
    }
}
