package io.tapdata.connector.mysql;

import com.github.shyiko.mysql.binlog.network.ServerException;
import io.debezium.connector.mysql.utils.GTIDException;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {

    private final static String pdkId = "mysql";

    @Override
    protected String getPdkId() {
        return pdkId;
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof SQLException && "08003".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(pdkId, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof SQLException && "28000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkUserPwdInvalidEx(pdkId, username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectOffsetInvalid(Object offset, Throwable cause) {
        if (cause instanceof ServerException && "HY000".equals(((ServerException) cause).getSqlState())) {
            throw new TapPdkOffsetOutOfLogEx(pdkId, offset, ErrorKit.getLastCause(cause));
        }
        if (cause instanceof GTIDException) {
            throw new TapPdkOffsetOutOfLogEx(pdkId, offset, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkReadMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof SQLException && "42000".equals(((SQLException) cause).getSQLState())) {
            throw new TapPdkWriteMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        if (cause instanceof SQLException) {
            switch (((SQLException) cause).getErrorCode()) {
                case 1292:
                case 1366: {
                    Pattern pattern = Pattern.compile("Incorrect (\\w+) value: '(.*)' for column '(.*)' at row ");
                    Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
                    String fieldName = null;
                    if (matcher.find()) {
                        fieldName = matcher.group(3);
                    }
                    throw new TapPdkWriteTypeEx(pdkId, fieldName, null, data, ErrorKit.getLastCause(cause));
                }
                case 1265: {
                    Pattern pattern = Pattern.compile("Data truncated for column '(.*)' at row ");
                    Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
                    String fieldName = null;
                    if (matcher.find()) {
                        fieldName = matcher.group(1);
                    }
                    throw new TapPdkWriteTypeEx(pdkId, fieldName, null, data, ErrorKit.getLastCause(cause));
                }
            }
        }
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //string length
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 1406) {
            Pattern pattern = Pattern.compile("Data too long for column '(.*)' at row ");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldName = null;
            if (matcher.find()) {
                fieldName = matcher.group(1);
            }
            throw new TapPdkWriteLengthEx(pdkId, fieldName, null, data, ErrorKit.getLastCause(cause));
        }
        //number length
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 1264) {
            Pattern pattern = Pattern.compile("Out of range value for column '(.*)' at row ");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldName = null;
            if (matcher.find()) {
                fieldName = matcher.group(1);
            }
            throw new TapPdkWriteLengthEx(pdkId, fieldName, null, data, ErrorKit.getLastCause(cause));
        }

    }

    @Override
    public void collectViolateUnique(String targetFieldName, Object data, Object constraint, Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 1062) {
            Pattern pattern = Pattern.compile("Duplicate entry '(.*)' for key '(.*)'");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String constraintStr = null;
            if (matcher.find()) {
                constraintStr = matcher.group(2);
            }
            throw new TapPdkViolateUniqueEx(pdkId, targetFieldName, data, constraintStr, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        if (cause instanceof SQLException && ((SQLException) cause).getErrorCode() == 1048) {
            Pattern pattern = Pattern.compile("Column '(.*)' cannot be null");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldName = null;
            if (matcher.find()) {
                fieldName = matcher.group(1);
            }
            throw new TapPdkViolateNullableEx(pdkId, fieldName, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectCdcConfigInvalid(Throwable cause) {
        super.collectCdcConfigInvalid(cause);
    }

    //exception collector for mysql needed to add later
    /*
     * 1、SQLSyntaxErrorException SQLState 42000 ErrorCode 1118
     * Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs
     *
     *
     * */
}
