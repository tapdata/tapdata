package io.tapdata.common.exception;

import java.sql.SQLException;
import java.util.List;

public abstract class AbstractExceptionCollector implements ExceptionCollector {

    @Override
    public void collectTerminateByServer(SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectUserPwdInvalid(String username, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectOffsetInvalid(Object offset, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectViolateUnique(String targetFieldName, Object data, Object constraint, SQLException cause) throws SQLException {
        throw cause;
    }

    @Override
    public void collectViolateNull(String targetFieldName, SQLException cause) throws SQLException {
        throw cause;
    }
}
