package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.commons.base.DoubleValueBound;
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
public class ServerOverviewDetail extends DataValueBase {
    private String serverName;

    private String serverId;

    @DoubleValueBound
    @DecimalFormat
    private Double cpuUsage;

    @DoubleValueBound
    @DecimalFormat
    private Double memoryUsage;

    private Long usagePingTime;

    private Long requestCount;

    @DoubleValueBound
    @DecimalFormat
    private double errorRate;

    private long errorCount;

    TopWorkerInServer workerInfo = new TopWorkerInServer();
}
