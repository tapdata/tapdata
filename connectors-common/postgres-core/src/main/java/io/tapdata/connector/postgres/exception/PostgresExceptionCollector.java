package io.tapdata.connector.postgres.exception;

import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;
import org.postgresql.util.PSQLException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresExceptionCollector extends AbstractExceptionCollector implements ExceptionCollector {

    private final static String pdkId = "postgres";

    @Override
    public void collectTerminateByServer(Throwable cause) {
        if (cause instanceof PSQLException && "57014".equals(((PSQLException) cause).getSQLState())) {
            throw new TapPdkTerminateByServerEx(pdkId, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {
        if (cause instanceof PSQLException && "28P01".equals(((PSQLException) cause).getSQLState())) {
            throw new TapPdkUserPwdInvalidEx(pdkId, username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectOffsetInvalid(Object offset, Throwable cause) {

    }

    @Override
    public void collectReadPrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof PSQLException && "42501".equals(((PSQLException) cause).getSQLState())) {
            throw new TapPdkReadMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWritePrivileges(Object operation, List<String> privileges, Throwable cause) {
        if (cause instanceof PSQLException && "42501".equals(((PSQLException) cause).getSQLState())) {
            throw new TapPdkReadMissingPrivilegesEx(pdkId, operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteType(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        if (cause instanceof PSQLException && "42804".equals(((PSQLException) cause).getSQLState())) {
            Pattern pattern = Pattern.compile("ERROR: column \"(.*)\" is of type (.*) but expression is of type (.*)");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldName = null;
            String fieldType = null;
            if (matcher.find()) {
                fieldName = matcher.group(1);
                fieldType = matcher.group(2);
            }
            throw new TapPdkWriteTypeEx(pdkId, fieldName, fieldType, data, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectWriteLength(String targetFieldName, String targetFieldType, Object data, Throwable cause) {
        //string length
        if (cause instanceof PSQLException && "22001".equals(((PSQLException) cause).getSQLState())) {
            Pattern pattern = Pattern.compile("ERROR: value too long for type (.*)");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String fieldType = null;
            if (matcher.find()) {
                fieldType = matcher.group(1);
            }
            throw new TapPdkWriteLengthEx(pdkId, null, fieldType, data, ErrorKit.getLastCause(cause));
        }
        //number length
        if (cause instanceof PSQLException && "22003".equals(((PSQLException) cause).getSQLState())) {
            throw new TapPdkWriteLengthEx(pdkId, null, null, data, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectViolateUnique(String targetFieldName, Object data, Object constraint, Throwable cause) {
        if (cause instanceof PSQLException && "23505".equals(((PSQLException) cause).getSQLState())) {
            Pattern pattern = Pattern.compile("ERROR: duplicate key value violates unique constraint \"(.*)\" ");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            String constraintStr = null;
            if (matcher.find()) {
                constraintStr = matcher.group(1);
            }
            throw new TapPdkViolateUniqueEx(pdkId, targetFieldName, data, constraintStr, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectViolateNull(String targetFieldName, Throwable cause) {
        if (cause instanceof PSQLException && "23502".equals(((PSQLException) cause).getSQLState())) {
            Pattern pattern = Pattern.compile("ERROR: null value in column \"(.*)\" of relation \"(.*)\" violates not-null constraint");
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
        if (cause instanceof PSQLException) {
            switch (((PSQLException) cause).getSQLState()) {
                case "58P01": //log plugin selected is not available
                    throw new TapDbCdcConfigInvalidEx(pdkId,
                            "Please select the correct logging plugin. If there are no available plugins on the server, you can refer to Markdown for installation and deployment",
                            ErrorKit.getLastCause(cause));
                case "55000": //wal_level not logical
                    throw new TapDbCdcConfigInvalidEx(pdkId,
                            "Please find postgres.conf, change with wal_level = logical",
                            ErrorKit.getLastCause(cause));
                case "53400": //max_replication_slots is full
                    throw new TapDbCdcConfigInvalidEx(pdkId,
                            "Check the maximum number of logical replication slots (max_replication_slots) in postgres.conf and promptly clean up any unused replication slots",
                            ErrorKit.getLastCause(cause));
                case "28000": //pg_hba.conf has no permission for replication
                    throw new TapDbCdcConfigInvalidEx(pdkId,
                            "Check pg_hba.conf, confirm if you have permission to create a logical replication slot",
                            ErrorKit.getLastCause(cause));
                case "0"://append error type
            }
        }
    }
}
