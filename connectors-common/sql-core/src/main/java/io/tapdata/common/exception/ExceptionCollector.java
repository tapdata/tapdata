package io.tapdata.common.exception;

import java.sql.SQLException;
import java.util.List;

public interface ExceptionCollector {
    default void collectTerminateByServer(SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectUserPwdInvalid(String username, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectOffsetInvalid(Object offset, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectReadPrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectWritePrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectWriteType(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectWriteLength(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectViolateUnique(String targetFieldName, Object data, Object constraint, SQLException cause) throws SQLException {
        throw cause;
    }

    default void collectViolateNull(String targetFieldName, SQLException cause) throws SQLException {
        throw cause;
    }
}
