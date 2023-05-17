package io.tapdata.connector.clickhouse;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;

import java.util.List;

public class ClickhouseExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {

    @Override
    public void collectTerminateByServer(Throwable cause) {
        super.collectTerminateByServer(cause);
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        super.collectUserPwdInvalid(username, cause);
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        super.collectReadPrivileges(operation, privileges, cause);
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        super.collectWritePrivileges(operation, privileges, cause);
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        super.collectWriteType(targetFieldName, targetFieldType, data, cause);
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        super.collectWriteLength(targetFieldName, targetFieldType, data, cause);
    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        super.collectViolateNull(targetFieldName, cause);
    }

}
