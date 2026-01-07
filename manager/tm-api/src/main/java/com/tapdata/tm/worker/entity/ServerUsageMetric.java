package com.tapdata.tm.worker.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/7 11:45 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ServerUsageMetric")
public class ServerUsageMetric extends ServerUsage {
    int timeGranularity;

    private Double maxCpuUsage;

    private Double minCpuUsage;

    private Long minHeapMemoryUsage;

    private Long maxHeapMemoryUsage;


    public static ServerUsageMetric instance(int timeGranularity, long ts, String processId, String workerId, int processType) {
        ServerUsageMetric metric = new ServerUsageMetric();
        metric.setTimeGranularity(timeGranularity);
        metric.setLastUpdateTime(ts);
        metric.setProcessId(processId);
        metric.setWorkOid(workerId);
        metric.setProcessType(processType);
        return metric;
    }

    public void append(org.bson.Document usage) {
        Double cpuUsage = Optional.ofNullable(usage.get("cpuUsage", Double.class)).orElse(0D);
        Long heapMemoryUsage = Optional.ofNullable(usage.get("heapMemoryUsage", Long.class)).orElse(0L);
        Long heapMemoryMax = Optional.ofNullable(usage.get("heapMemoryMax", Long.class)).orElse(0L);
        setCpuUsage((cpuUsage + Optional.ofNullable(getCpuUsage()).orElse(0D)) / 2);
        setHeapMemoryUsage((heapMemoryUsage + Optional.ofNullable(getHeapMemoryUsage()).orElse(0L)) / 2);
        setHeapMemoryMax(Math.max(Optional.ofNullable(getHeapMemoryMax()).orElse(heapMemoryMax), heapMemoryMax));
        setMaxCpuUsage(Math.max(Optional.ofNullable(getMaxCpuUsage()).orElse(cpuUsage), cpuUsage));
        setMinCpuUsage(Math.min(Optional.ofNullable(getMinCpuUsage()).orElse(cpuUsage), cpuUsage));
        setMaxHeapMemoryUsage(Math.max(Optional.ofNullable(getMaxHeapMemoryUsage()).orElse(heapMemoryUsage), heapMemoryUsage));
        setMinHeapMemoryUsage(Math.max(Optional.ofNullable(getMinHeapMemoryUsage()).orElse(heapMemoryUsage), heapMemoryUsage));
    }
}
