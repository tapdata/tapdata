package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 15:10 Create
 * @description
 */
public final class ServerUsageMetricInstanceAcceptor implements AcceptorBase {
    Consumer<ServerUsageMetric> consumer;

    ServerUsageMetric lastBucketMin;
    ServerUsageMetric lastBucketHour;

    final List<Long> minMemory;
    final List<Long> minMemoryMax;
    final List<Double> minCpu;

    final List<Long> hourMemoryMax;
    final List<Long> hourMemory;
    final List<Double> hourCpu;

    public ServerUsageMetricInstanceAcceptor(ServerUsageMetric lastBucketMin, ServerUsageMetric lastBucketHour, Consumer<ServerUsageMetric> consumer) {
        this.lastBucketMin = lastBucketMin;
        this.lastBucketHour = lastBucketHour;
        this.consumer = consumer;
        this.minMemory = new ArrayList<>(12);
        this.hourMemory = new ArrayList<>(12 * 60);
        this.minMemoryMax = new ArrayList<>(12);
        this.hourMemoryMax = new ArrayList<>(12 * 60);
        this.minCpu = new ArrayList<>(12);
        this.hourCpu = new ArrayList<>(12 * 60);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String serverId = entity.get("processId", String.class);
        Object workOidObj = entity.get("workOid");
        String workOid = workOidObj != null ? workOidObj.toString() : null;
        long lastUpdateTime = Optional.ofNullable(entity.get("lastUpdateTime", Long.class)).orElse(0L) / 1000L;
        long bucketMin = (lastUpdateTime / 60) * 60 * 1000L;
        long bucketHour = (lastUpdateTime / 3600) * 3600 * 1000L;
        if (null != lastBucketMin && lastBucketMin.getLastUpdateTime() != bucketMin) {
            acceptMin();
            lastBucketMin = null;
        }
        if (null != lastBucketHour && lastBucketHour.getLastUpdateTime() != bucketHour) {
            acceptHour();
            lastBucketHour = null;
        }
        ServerUsage.ProcessType processType = null == workOid ? ServerUsage.ProcessType.API_SERVER : ServerUsage.ProcessType.API_SERVER_WORKER;
        if (null == lastBucketMin) {
            lastBucketMin = ServerUsageMetric.instance(1, bucketMin, serverId, workOid, processType.getType());
        }
        if (null == lastBucketHour) {
            lastBucketHour = ServerUsageMetric.instance(2, bucketHour, serverId, workOid, processType.getType());
        }
        append(entity);
    }

    void append(Document usage) {
        final Double cpuUsage = Optional.ofNullable(usage.get("cpuUsage", Double.class)).orElse(0D);
        final Long heapMemoryUsage = Optional.ofNullable(usage.get("heapMemoryUsage", Long.class)).orElse(0L);
        final Long heapMemoryMax = Optional.ofNullable(usage.get("heapMemoryMax", Long.class)).orElse(0L);
        this.minMemory.add(heapMemoryUsage);
        this.minMemoryMax.add(heapMemoryMax);
        this.minCpu.add(cpuUsage);
        this.hourMemory.add(heapMemoryUsage);
        this.hourMemoryMax.add(heapMemoryMax);
        this.hourCpu.add(cpuUsage);
    }

    void acceptMin() {
        if (null == lastBucketMin) {
            return;
        }
        accept(lastBucketMin, minMemory, minMemoryMax, minCpu);
        consumer.accept(lastBucketMin);
    }


    void accept(ServerUsageMetric usage, List<Long> memory, List<Long> memoryMax, List<Double> cpu) {
        if (!memoryMax.isEmpty()) {
            memoryMax.stream().filter(Objects::nonNull)
                    .mapToLong(Long::longValue)
                    .average()
                    .ifPresent(avg -> usage.setHeapMemoryMax(((Double) avg).longValue()));
            memoryMax.clear();
        }
        if (!memory.isEmpty()) {
            memory.stream().filter(Objects::nonNull).mapToLong(Long::longValue).min().ifPresent(usage::setMinHeapMemoryUsage);
            memory.stream().filter(Objects::nonNull).mapToLong(Long::longValue).max().ifPresent(usage::setMaxHeapMemoryUsage);
            memory.stream().filter(Objects::nonNull)
                    .mapToLong(Long::longValue)
                    .average()
                    .ifPresent(avg -> usage.setHeapMemoryUsage(((Double) avg).longValue()));
            memory.clear();
        }
        if (!cpu.isEmpty()) {
            cpu.stream().filter(Objects::nonNull).mapToDouble(Double::doubleValue).min().ifPresent(usage::setMinCpuUsage);
            cpu.stream().filter(Objects::nonNull).mapToDouble(Double::doubleValue).max().ifPresent(usage::setMaxCpuUsage);
            cpu.stream().filter(Objects::nonNull)
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .ifPresent(usage::setCpuUsage);
            cpu.clear();
        }
    }

    void acceptHour() {
        if (null == lastBucketHour) {
            return;
        }
        accept(lastBucketHour, hourMemory, hourMemoryMax, hourCpu);
        consumer.accept(lastBucketHour);
    }

    @Override
    public void close() {
        acceptMin();
        acceptHour();
    }
}
