package com.tapdata.tm.commons.exception;

/**
 * 无主键处理异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/17 16:07 Create
 */
public class NoPrimaryKeyException extends RuntimeException {
    public static final int CODE_INCOMPLETE_FIELDS = 1;
    public static final int CODE_NOTFOUND_HASH_ALGORITHM = 10;
    public static final int CODE_OTHER = 99;

    private final int code;

    public NoPrimaryKeyException(String message, int code) {
        super(message);
        this.code = code;
    }

    public NoPrimaryKeyException(String message, Throwable cause, int code) {
        super(message, cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static NoPrimaryKeyException incompleteFields(String field) {
        return new NoPrimaryKeyException(String.format("not exist field '%s'", field), CODE_INCOMPLETE_FIELDS);
    }

    public static NoPrimaryKeyException notfoundHashAlgorithm(String type, Throwable e) {
        return new NoPrimaryKeyException(String.format("%s algorithm not found", type), e, CODE_NOTFOUND_HASH_ALGORITHM);
    }

    public static NoPrimaryKeyException otherFailed(Throwable e) {
        return new NoPrimaryKeyException("add hash filed value failed", e, CODE_OTHER);
    }
}
