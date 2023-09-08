package io.tapdata.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.*;
import io.tapdata.kit.ErrorKit;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongodbExceptionCollector extends AbstractExceptionCollector {

    protected String getPdkId() {
        return "mongodb";
    }



    //write
    public void throwWriteExIfNeed(Object data, Throwable cause){
        this.collectTerminateByServer(cause);
        this.collectWritePrivileges(cause);
//        this.collectWriteType(data, cause);
        this.collectWriteLength(data, cause);
        this.collectViolateUnique(data, cause);
    }

    @Override
    public void revealException(Throwable cause) {
        if(cause instanceof TapPdkBaseException) return;
        if (cause instanceof MongoException) {
            throw new TapPdkRetryableEx(getPdkId(), ErrorKit.getLastCause(cause)).withServerErrorCode(String.valueOf(((MongoException) cause).getCode()));
        }
    }

    @Override
    public void collectTerminateByServer(Throwable cause) {

        if (cause instanceof MongoException && (((MongoException) cause).getCode())==-3) {
            if (cause.getMessage().contains("Timed out after 30000 ms while waiting to connect")){
                throw new TapPdkTerminateByServerEx(getPdkId(), ErrorKit.getLastCause(cause));
            }
        }
    }


    @Override
    public void collectUserPwdInvalid(String username, Throwable cause) {

        if (cause instanceof MongoException && (((MongoException) cause).getCode())==18) {
            throw new TapPdkUserPwdInvalidEx(getPdkId(), username, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectOffsetInvalid(Object offset, Throwable cause) {
        if (cause instanceof MongoException && (((MongoException) cause).getCode())==286) {
            throw new TapPdkOffsetOutOfLogEx(getPdkId(), offset, ErrorKit.getLastCause(cause));
        }
    }

    public void collectReadPrivileges(Throwable cause) {
        if (cause instanceof MongoException && (((MongoException) cause).getCode())==13) {
            Object operation = "Read";
            List<String> privileges = new ArrayList<>();
            privileges.add("Read");
            throw new TapPdkReadMissingPrivilegesEx(getPdkId(), operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    public void collectWritePrivileges(Throwable cause) {
        if (cause instanceof MongoException && (((MongoException) cause).getCode())==13) {
            Object operation = "Write";
            Pattern pattern = Pattern.compile("execute command '(.*)' on server");
            Matcher matcher = pattern.matcher(ErrorKit.getLastCause(cause).getMessage());
            if (matcher.find()) {
                operation = matcher.group(1);
            }
            List<String> privileges = new ArrayList<>();
            privileges.add("Write");
            throw new TapPdkWriteMissingPrivilegesEx(getPdkId(), operation, privileges, ErrorKit.getLastCause(cause));
        }
    }

    public void collectWriteType(Object data, Throwable cause) {
        //null类型
    }

    public void collectWriteLength(Object data, Throwable cause) {
        //gt 16M
        if (cause instanceof MongoException && FromMongoBulkWriteExceptionGetWriteErrors(cause,13134)) {
            if (null!=data && data instanceof List){
                int index = ((MongoBulkWriteException) cause).getWriteErrors().get(0).getIndex();
                data = ((List<TapRecordEvent>) data).get(index);
            }
            throw new TapPdkWriteLengthEx(getPdkId(), null, "BSON", data, ErrorKit.getLastCause(cause));
        }
    }
    public void collectViolateUnique(Object data,Throwable cause) {
        if (cause instanceof MongoBulkWriteException && FromMongoBulkWriteExceptionGetWriteErrors(cause,11000)) {
            String targetFieldName = null;
            String errorMessage = cause.getMessage();
            // 定义正则表达式来匹配错误信息中的表名和字段信息  \{ (\w+): collection: (\w+)\.(\w+) .*_id: (\w+)
            Pattern pattern = Pattern.compile("collection: (\\w+)\\..+ dup key: \\{ (\\w+):");
            Matcher matcher = pattern.matcher(errorMessage);
            // 查找匹配项
            if (matcher.find()) {
                // 提取表名和字段信息
                String tableName = matcher.group(1);
                targetFieldName = matcher.group(2);
            }
            String constraintStr = "duplicate key error "+targetFieldName;
            if (null!=data && data instanceof List){
                int index = ((MongoBulkWriteException) cause).getWriteErrors().get(0).getIndex();
                data = ((List<TapRecordEvent>) data).get(index);
            }
            throw new TapPdkViolateUniqueEx(getPdkId(), targetFieldName, data, constraintStr, ErrorKit.getLastCause(cause));
        }
    }

    @Override
    public void collectCdcConfigInvalid(Throwable cause) {
        super.collectCdcConfigInvalid(cause);
    }


    public boolean FromMongoBulkWriteExceptionGetWriteErrors(Throwable cause, int errorCode){
        if (cause instanceof MongoBulkWriteException){
            List<BulkWriteError> writeErrors = ((MongoBulkWriteException) cause).getWriteErrors();
            if (null!=writeErrors && writeErrors.size()>0){
                if (writeErrors.get(0).getCode()==errorCode){
                    return true;
                }
            }
        }
        return false;
    }

}
