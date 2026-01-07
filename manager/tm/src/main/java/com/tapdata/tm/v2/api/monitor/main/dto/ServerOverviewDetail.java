package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:23 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ServerOverviewDetail extends ValueBase {
    private String serverName;

    private String serverId;

    private Double cpuUsage;

    private Long memoryUsage;

    private Long usagePingTime;

    private Long requestCount;

    private Double errorRate;

    private Long p95;

    private Long p99;

    private Long maxDelay;

    private Long minDelay;
}
