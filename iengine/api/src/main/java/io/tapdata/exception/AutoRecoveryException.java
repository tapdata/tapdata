package io.tapdata.exception;

import io.tapdata.error.TaskInspectExCode_27;

/**
 * 自动恢复异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/5 19:04 Create
 */
public class AutoRecoveryException extends TapCodeException {
    public AutoRecoveryException(String code) {
        super(code);
    }

    public AutoRecoveryException(String code, String message) {
        super(code, message);
    }

    public AutoRecoveryException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }

    public static AutoRecoveryException instanceExists(String taskId) {
        return new AutoRecoveryException(TaskInspectExCode_27.AUTO_RECOVERY_DUPLICATE, String.format("Task %s auto-recovery instance already exists.", taskId));
    }

    public static AutoRecoveryException clientExists(String taskId) {
        return new AutoRecoveryException(TaskInspectExCode_27.AUTO_RECOVERY_CLIENT_DUPLICATE, String.format("Task %s auto-recovery client already exists.", taskId));
    }

    public static AutoRecoveryException notFoundInstance(String taskId) {
        return new AutoRecoveryException(TaskInspectExCode_27.AUTO_RECOVERY_NOT_EXISTS, String.format("Not found auto-recovery instance by taskId: %s", taskId));
    }

    public static AutoRecoveryException fixedTimeout() {
        return new AutoRecoveryException(TaskInspectExCode_27.AUTO_RECOVERY_TIMEOUT, "Auto-recovery timeout");
    }

    public static AutoRecoveryException notFoundRecoveryType(String recoveryType) {
        return new AutoRecoveryException(TaskInspectExCode_27.AUTO_RECOVERY_NOT_FOUND_RECOVERY_TYPE, String.format("Not found recovery type for auto-recovery: %s", recoveryType));
    }
}
