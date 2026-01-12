package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:10 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiOfEachServer extends ValueBase {
    String serverId;
    String serverName;
    Long requestCount;
    @DecimalFormat
    Double requestCostAvg;
    Long p95;
    Long p99;
    Long maxDelay;
    Long minDelay;
    @DecimalFormat
    Double errorRate;
}
