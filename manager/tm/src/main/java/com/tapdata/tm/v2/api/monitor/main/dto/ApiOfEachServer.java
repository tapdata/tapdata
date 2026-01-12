package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.worker.entity.Worker;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public static List<ApiOfEachServer> supplement(List<ApiOfEachServer> apiMetricsRaws, Map<String, Worker> apiServerMap) {
        List<String> serverIds = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .map(ApiOfEachServer::getServerId)
                .filter(StringUtils::isNotBlank)
                .toList();
        apiServerMap.forEach((serverId, info) -> {
            if (!serverIds.contains(serverId)) {
                ApiOfEachServer item = new ApiOfEachServer();
                item.setRequestCount(0L);
                item.setRequestCostAvg(0D);
                item.setErrorRate(0D);
                item.setServerId(serverId);
                item.setServerName(info.getHostname());
                apiMetricsRaws.add(item);
            }
        });
        return apiMetricsRaws;
    }
}
