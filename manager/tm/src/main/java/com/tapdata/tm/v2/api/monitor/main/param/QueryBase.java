package com.tapdata.tm.v2.api.monitor.main.param;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:16 Create
 * @description
 */
@Data
public class QueryBase {
    Long startAt;

    Long endAt;

    int granularity;
}
