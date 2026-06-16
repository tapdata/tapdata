package com.tapdata.tm.commons.trace;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/27 17:29 Create
 * @description
 */
@Data
public class ChangeLogCriteria {
    private String ringBuffer;
    private String connectionId;
    private String tableName;
    private List<Map<String, Object>> filters;
    private String externalStorageId;
    long startTime;
    long endTime;
    int limit = 10;
    Long key;
}
