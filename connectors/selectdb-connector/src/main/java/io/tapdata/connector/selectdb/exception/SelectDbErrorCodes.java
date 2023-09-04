package io.tapdata.connector.selectdb.exception;

/**
 * Author:Skeet
 * Date: 2023/4/7
 **/
public interface SelectDbErrorCodes {
    int ERROR_SDB_COPY_INTO_CANCELLED = 10001;
    int ERROR_SDB_COPY_INTO_STATE_NULL = 10002;
    int ERROR_SDB_COPY_INTO_NETWORK = 10003;
}
