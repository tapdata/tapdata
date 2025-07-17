package com.tapdata.exception;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/7/17 10:32 Create
 */
@Getter
@Setter
public class FindOneByKeysException extends RuntimeException {
    private String tableName;
    private LinkedHashMap<String, Object> keys;

    public FindOneByKeysException(String tableName, LinkedHashMap<String, Object> keys) {
        super(formatMessage(tableName, keys));
        this.tableName = tableName;
        this.keys = keys;
    }

    public FindOneByKeysException(Throwable cause, String tableName, LinkedHashMap<String, Object> keys) {
        super(formatMessage(tableName, keys), cause);
        this.tableName = tableName;
        this.keys = keys;
    }

    private static String formatMessage(String tableName, LinkedHashMap<String, Object> keys) {
        return String.format("table '%s' can't query of keys: %s", tableName, JSON.toJSONString(keys));
    }
}
