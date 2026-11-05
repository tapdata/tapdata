package com.tapdata.tm.trace.param;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/27 17:14 Create
 * @description
 */
@Data
public class ChangeLogParam {
    String connectionId;
    String table;

    //毫秒级时间戳
    Long endTime;
    //毫秒级时间戳
    Long startTime;

    List<Map<String, Object>> queryConditions;

    int limit = 10;
    Long lastKey;
}
