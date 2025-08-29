package com.tapdata.exception;

import lombok.Getter;
import lombok.Setter;

/**
 * 对比异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/7/11 01:17 Create
 */
@Setter
@Getter
public class CompareException extends Exception {
    private String sourceTable;
    private String sourceField;
    private Object sourceValue;
    private String targetTable;
    private String targetField;
    private Object targetValue;

    public CompareException() {
    }

    public CompareException(Throwable cause) {
        super(cause);
    }

    public CompareException sourceTable(String table) {
        setSourceTable(table);
        return this;
    }

    public CompareException sourceField(String field) {
        setSourceField(field);
        return this;
    }

    public CompareException sourceValue(Object o) {
        setSourceValue(o);
        return this;
    }

    public CompareException targetTable(String table) {
        setTargetTable(table);
        return this;
    }

    public CompareException targetField(String field) {
        setTargetField(field);
        return this;
    }

    public CompareException targetValue(Object o) {
        setTargetValue(o);
        return this;
    }

    @Override
    public String getMessage() {
        return String.format("source.%s.%s=%s, target.%s.%s=%s"
            , getSourceTable(), getSourceField(), getSourceValue()
            , getTargetTable(), getTargetField(), getTargetValue()
        );
    }
}
