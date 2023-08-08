package io.tapdata.common.exception;

import java.util.List;

public interface ExceptionCollector {

    void collectTerminateByServer(Throwable cause) throws RuntimeException;

    void collectUserPwdInvalid(String username, Throwable cause) throws RuntimeException;

    void collectOffsetInvalid(Object offset, Throwable cause) throws RuntimeException;

    void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) throws RuntimeException;

    void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) throws RuntimeException;

    void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) throws RuntimeException;

    void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) throws RuntimeException;

    void collectViolateUnique(String targetFieldName, Object data, Object constraint, Throwable cause) throws RuntimeException;

    void collectViolateNull(String targetFieldName, Throwable cause) throws RuntimeException;

    void collectCdcConfigInvalid(Throwable cause) throws RuntimeException;

    void revealException(Throwable cause) throws RuntimeException;
}
