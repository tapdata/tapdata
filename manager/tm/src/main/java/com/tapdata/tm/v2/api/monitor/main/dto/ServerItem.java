package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:14 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ServerItem extends ValueBase {
    String serverStatus;

    String serverPingStatus;

    Long serverPingTime;

    String serverName;

    String serverId;

    @DecimalFormat
    List<Double> cpuUsage;

    @DecimalFormat
    List<Double> memoryUsage;

    List<Long> ts;

    Long requestCount;

    @DecimalFormat
    Double errorRate;

    Long p95;

    Long p99;

    Long maxDelay;

    Long minDelay;

    boolean deleted;

    public static ServerItem create() {
        ServerItem item = new ServerItem();
        item.setCpuUsage(new ArrayList<>());
        item.setMemoryUsage(new ArrayList<>());
        item.setTs(new ArrayList<>());
        item.setRequestCount(0L);
        item.setErrorRate(0.0D);
        item.setP95(0L);
        item.setP99(0L);
        return item;
    }
}
