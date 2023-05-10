package io.tapdata.common.exception;

import java.sql.SQLException;
import java.util.List;

public interface ExceptionCollector {

    void collectTerminateByServer(SQLException cause) throws SQLException;

    void collectUserPwdInvalid(String username, SQLException cause) throws SQLException;

    void collectOffsetInvalid(Object offset, SQLException cause) throws SQLException;

    void collectReadPrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException;

    void collectWritePrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException;

    void collectWriteType(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException;

    void collectWriteLength(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException;

    void collectViolateUnique(String targetFieldName, Object data, Object constraint, SQLException cause) throws SQLException;

    void collectViolateNull(String targetFieldName, SQLException cause) throws SQLException;

    void collectCdcConfigInvalid(SQLException cause) throws SQLException;
}
