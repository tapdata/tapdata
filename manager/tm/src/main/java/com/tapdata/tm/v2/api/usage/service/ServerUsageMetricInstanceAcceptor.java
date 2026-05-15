package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.field.ServerUsageField;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
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
    @Getter
    public enum ContainerType {
        MINUTE_MEM(12),
        MINUTE_MEM_MAX(12),
        MINUTE_CPU(12),
        MINUTE_POOL_CONNECTIONS_MAX(12),
        MINUTE_POOL_CONNECTIONS_USED(12),
        MINUTE_POOL_QUEUE_SIZE(12),

        HOUR_MEMORY_MAX(12 * 60),
        HOUR_MEMORY(12 * 60),
        HOUR_CPU(12 * 60),
        HOUR_POOL_CONNECTIONS_MAX(12 * 60),
        HOUR_POOL_CONNECTIONS_USED(12 * 60),
        HOUR_POOL_QUEUE_SIZE(12 * 60),


        ;
        final int initSize;

        ContainerType(int initSize) {
            this.initSize = initSize;
        }

        public static ContainerType[] ofMinute() {
            return new ContainerType[]{MINUTE_MEM, MINUTE_MEM_MAX, MINUTE_CPU, MINUTE_POOL_CONNECTIONS_MAX, MINUTE_POOL_CONNECTIONS_USED, MINUTE_POOL_QUEUE_SIZE};
        }

        public static ContainerType[] ofHour() {
            return new ContainerType[]{HOUR_MEMORY_MAX, HOUR_MEMORY, HOUR_CPU, HOUR_POOL_CONNECTIONS_MAX, HOUR_POOL_CONNECTIONS_USED, HOUR_POOL_QUEUE_SIZE};
        }
    }

    static class Container<T extends Number> {
        List<T> space;
        Pull<T> pull;

        interface Pull<V> {
            void pull(ServerUsageMetric usage, List<V> space);
        }

        public Container(int initSize, Pull<T> pull) {
            this.space = new ArrayList<>(initSize);
            this.pull = pull;
        }

        public void push(Document usage, ServerUsageField key, Class<T> matterClass, T defaultMatter) {
            T matter = Optional.ofNullable(usage.get(key.field(), matterClass)).orElse(defaultMatter);
            this.space.add(matter);
        }

        public void pullAndClean(ServerUsageMetric usage) {
            this.pull.pull(usage, this.space);
            this.space.clear();
        }
    }

    Consumer<ServerUsageMetric> consumer;

    ServerUsageMetric lastBucketMin;
    ServerUsageMetric lastBucketHour;
    final Map<ContainerType, Container<?>> containerMap = new EnumMap<>(ContainerType.class);

    public <T extends Number> void push(ContainerType type, Document usage, ServerUsageField key, Class<T> matterClass, T defaultMatter) {
        Container<T> container = (Container<T>) containerMap.get(type);
        assert container != null;
        container.push(usage, key, matterClass, defaultMatter);
    }

    public ServerUsageMetricInstanceAcceptor(ServerUsageMetric lastBucketMin, ServerUsageMetric lastBucketHour, Consumer<ServerUsageMetric> consumer) {
        this.lastBucketMin = lastBucketMin;
        this.lastBucketHour = lastBucketHour;
        this.consumer = consumer;
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_MEM, k -> new Container<>(k.getInitSize(), this::acceptMemory));
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_MEM_MAX, k -> new Container<>(k.getInitSize(), this::acceptMemMax));
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_CPU, k -> new Container<>(k.getInitSize(), this::acceptCpu));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_MEMORY_MAX, k -> new Container<>(k.getInitSize(), this::acceptMemMax));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_MEMORY, k -> new Container<>(k.getInitSize(), this::acceptMemory));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_CPU, k -> new Container<>(k.getInitSize(), this::acceptCpu));
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_POOL_CONNECTIONS_MAX, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolMaxConnections)));
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_POOL_CONNECTIONS_USED, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolUsedConnections)));
        this.containerMap.computeIfAbsent(ContainerType.MINUTE_POOL_QUEUE_SIZE, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolQueueSize)));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_POOL_CONNECTIONS_MAX, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolMaxConnections)));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_POOL_CONNECTIONS_USED, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolUsedConnections)));
        this.containerMap.computeIfAbsent(ContainerType.HOUR_POOL_QUEUE_SIZE, k -> new Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setPoolQueueSize)));
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String serverId = entity.get(ServerUsageField.PROCESS_ID.field(), String.class);
        Object workOidObj = entity.get(ServerUsageField.WORK_OID.field());
        String workOid = workOidObj != null ? workOidObj.toString() : null;
        long lastUpdateTime = Optional.ofNullable(entity.get(ServerUsageField.LAST_UPDATE_TIME.field(), Long.class)).orElse(0L) / 1000L;
        long bucketMin = TimeGranularity.MINUTE.fixTime(lastUpdateTime) * 1000L;
        long bucketHour = TimeGranularity.HOUR.fixTime(lastUpdateTime) * 1000L;
        if (null != lastBucketMin && lastBucketMin.getLastUpdateTime() != bucketMin) {
            acceptMin();
            lastBucketMin = null;
        }
        if (null != lastBucketHour && lastBucketHour.getLastUpdateTime() != bucketHour) {
            acceptHour();
            lastBucketHour = null;
        }
        ServerUsage.ProcessType processType = StringUtils.isBlank(workOid) ? ServerUsage.ProcessType.API_SERVER : ServerUsage.ProcessType.API_SERVER_WORKER;
        if (null == lastBucketMin) {
            lastBucketMin = ServerUsageMetric.instance(TimeGranularity.MINUTE.getType(), bucketMin, serverId, workOid, processType.getType());
        }
        if (null == lastBucketHour) {
            lastBucketHour = ServerUsageMetric.instance(TimeGranularity.HOUR.getType(), bucketHour, serverId, workOid, processType.getType());
        }
        append(entity);
    }

    void append(Document usage) {
        push(ContainerType.MINUTE_MEM, usage, ServerUsageField.HEAP_MEMORY_USAGE, Long.class, 0L);
        push(ContainerType.MINUTE_MEM_MAX, usage, ServerUsageField.HEAP_MEMORY_MAX, Long.class, 0L);
        push(ContainerType.MINUTE_CPU, usage, ServerUsageField.CPU_USAGE, Double.class, 0D);
        push(ContainerType.HOUR_MEMORY, usage, ServerUsageField.HEAP_MEMORY_USAGE, Long.class, 0L);
        push(ContainerType.HOUR_MEMORY_MAX, usage, ServerUsageField.HEAP_MEMORY_MAX, Long.class, 0L);
        push(ContainerType.HOUR_CPU, usage, ServerUsageField.CPU_USAGE, Double.class, 0D);

        push(ContainerType.MINUTE_POOL_CONNECTIONS_MAX, usage, ServerUsageField.POOL_MAX_CONNECTIONS, Integer.class, 0);
        push(ContainerType.MINUTE_POOL_CONNECTIONS_USED, usage, ServerUsageField.POOL_USED_CONNECTIONS, Integer.class, 0);
        push(ContainerType.MINUTE_POOL_QUEUE_SIZE, usage, ServerUsageField.POOL_QUEUE_SIZE, Integer.class, 0);
        push(ContainerType.HOUR_POOL_CONNECTIONS_MAX, usage, ServerUsageField.POOL_MAX_CONNECTIONS, Integer.class, 0);
        push(ContainerType.HOUR_POOL_CONNECTIONS_USED, usage, ServerUsageField.POOL_USED_CONNECTIONS, Integer.class, 0);
        push(ContainerType.HOUR_POOL_QUEUE_SIZE, usage, ServerUsageField.POOL_QUEUE_SIZE, Integer.class, 0);
    }

    void acceptMin() {
        if (null == lastBucketMin) {
            return;
        }
        accept(lastBucketMin, ContainerType.ofMinute());
        consumer.accept(lastBucketMin);
    }

    public void acceptMemMax(ServerUsageMetric usage, List<Long> memoryMax) {
        if (memoryMax.isEmpty()) {
            return;
        }
        memoryMax.stream().filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .average()
                .ifPresent(avg -> usage.setHeapMemoryMax(((Double) avg).longValue()));
    }

    public void acceptMemory(ServerUsageMetric usage, List<Long> memory) {
        if (memory.isEmpty()) {
            return;
        }
        memory.stream().filter(Objects::nonNull).mapToLong(Long::longValue).min().ifPresent(usage::setMinHeapMemoryUsage);
        memory.stream().filter(Objects::nonNull).mapToLong(Long::longValue).max().ifPresent(usage::setMaxHeapMemoryUsage);
        memory.stream().filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .average()
                .ifPresent(avg -> usage.setHeapMemoryUsage(((Double) avg).longValue()));
    }

    public void acceptCpu(ServerUsageMetric usage, List<Double> cpu) {
        if (cpu.isEmpty()) {
            return;
        }
        cpu.stream().filter(Objects::nonNull).mapToDouble(Double::doubleValue).min().ifPresent(usage::setMinCpuUsage);
        cpu.stream().filter(Objects::nonNull).mapToDouble(Double::doubleValue).max().ifPresent(usage::setMaxCpuUsage);
        cpu.stream().filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .average()
                .ifPresent(usage::setCpuUsage);
    }

    public void acceptAvg(List<Number> number, Consumer<Integer> consumer) {
        assert number != null;
        if (number.isEmpty()) {
            return;
        }
        Double value = number.stream()
                .filter(Objects::nonNull)
                .filter(v -> v.intValue() > 0)
                .mapToLong(Number::longValue)
                .average()
                .orElse(0D);
        consumer.accept(value.intValue());
    }

    void accept(ServerUsageMetric usage, ContainerType... types) {
        for (ContainerType type : types) {
            Container<?> container = containerMap.get(type);
            assert null != container;
            container.pullAndClean(usage);
        }
    }

    void acceptHour() {
        if (null == lastBucketHour) {
            return;
        }
        accept(lastBucketHour, ContainerType.ofHour());
        consumer.accept(lastBucketHour);
    }

    @Override
    public void close() {
        acceptMin();
        acceptHour();
    }
}
