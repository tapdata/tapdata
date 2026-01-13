package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.worker.entity.Worker;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        return item;
    }

    public static List<ServerItem> supplement(List<ServerItem> apiMetricsRaws, Map<String, Worker> apiServerMap) {
        List<String> serverIds = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .map(ServerItem::getServerId)
                .filter(StringUtils::isNotBlank)
                .toList();
        apiServerMap.forEach((serverId, info) -> {
            if (!serverIds.contains(serverId)) {
                ServerItem item = create();
                item.setServerId(serverId);
                item.setServerName(info.getHostname());
                apiMetricsRaws.add(item);
            }
        });
        return apiMetricsRaws;
    }
}
