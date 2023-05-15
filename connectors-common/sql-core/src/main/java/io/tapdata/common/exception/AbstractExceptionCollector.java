package io.tapdata.common.exception;

import java.util.List;

public abstract class AbstractExceptionCollector implements ExceptionCollector {

    @Override
    public void collectTerminateByServer(Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectOffsetInvalid(Object offset, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectViolateUnique(String targetFieldName, Object data, Object constraint, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        throw new RuntimeException(cause);
    }

    @Override
    public void collectCdcConfigInvalid(Throwable cause) {
        throw new RuntimeException(cause);
    }
}
