package com.tapdata.tm.worker.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/7 11:45 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ServerUsageMetric")
public class ServerUsageMetric extends UsageBase {
    int timeGranularity;

    private Double maxCpuUsage;

    private Double minCpuUsage;

    private Long minHeapMemoryUsage;

    private Long maxHeapMemoryUsage;


    public static ServerUsageMetric instance(int timeGranularity, long ts, String processId, String workerId, int processType) {
        final ServerUsageMetric metric = new ServerUsageMetric();
        metric.setTimeGranularity(timeGranularity);
        metric.setLastUpdateTime(ts);
        metric.setProcessId(processId);
        metric.setWorkOid(workerId);
        metric.setProcessType(processType);
        return metric;
    }
}
