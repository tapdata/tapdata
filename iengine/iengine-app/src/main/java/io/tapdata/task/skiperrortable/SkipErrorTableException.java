package io.tapdata.task.skiperrortable;

import lombok.Getter;

/**
 * 跳过错误表异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/19 16:27 Create
 */
public class SkipErrorTableException extends RuntimeException {
    @Getter
    private final String tableName;

    public SkipErrorTableException(String tableName) {
        this.tableName = tableName;
    }
}
