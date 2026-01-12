package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:07 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiDetail extends ValueBase {
    String apiPath;
    String apiName;
    /**
     * 总调用数
     * */
    Long requestCount;

    /**
     * 全局错误率
     * */
    Double errorRate;

    /**
     * 平均耗时
     * */
    Double requestCostAvg;

    Long p95;
    Long p99;
    Long maxDelay;
    Long minDelay;
}
