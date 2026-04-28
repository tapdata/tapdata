package com.tapdata.tm.v2.api.pool.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
import lombok.Getter;
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
 * @version v1.0 2026/4/29 14:50 Create
 * @description
 */
public class ConnectionPoolInstanceAcceptor implements AcceptorBase {
    @Getter
    public enum ContainerType {
        MINUTE_POOL_CONNECTIONS_MAX(12),
        MINUTE_POOL_CONNECTIONS_USED(12),
        MINUTE_POOL_QUEUE_SIZE(12),

        HOUR_POOL_CONNECTIONS_MAX(12 * 60),
        HOUR_POOL_CONNECTIONS_USED(12 * 60),
        HOUR_POOL_QUEUE_SIZE(12 * 60),


        ;
        final int initSize;

        ContainerType(int initSize) {
            this.initSize = initSize;
        }

        public static ConnectionPoolInstanceAcceptor.ContainerType[] ofMinute() {
            return new ConnectionPoolInstanceAcceptor.ContainerType[]{MINUTE_POOL_CONNECTIONS_MAX, MINUTE_POOL_CONNECTIONS_USED, MINUTE_POOL_QUEUE_SIZE};
        }

        public static ConnectionPoolInstanceAcceptor.ContainerType[] ofHour() {
            return new ConnectionPoolInstanceAcceptor.ContainerType[]{HOUR_POOL_CONNECTIONS_MAX, HOUR_POOL_CONNECTIONS_USED, HOUR_POOL_QUEUE_SIZE};
        }
    }

    static class Container<T extends Number> {
        List<T> space;
        ConnectionPoolInstanceAcceptor.Container.Pull<T> pull;

        interface Pull<V> {
            void pull(ConnectionPoolEntity usage, List<V> space);
        }

        public Container(int initSize, ConnectionPoolInstanceAcceptor.Container.Pull<T> pull) {
            this.space = new ArrayList<>(initSize);
            this.pull = pull;
        }

        public void push(Document usage, ConnectionPoolField key, Class<T> matterClass, T defaultMatter) {
            T matter = Optional.ofNullable(usage.get(key.field(), matterClass)).orElse(defaultMatter);
            this.space.add(matter);
        }

        public void pullAndClean(ConnectionPoolEntity usage) {
            this.pull.pull(usage, this.space);
            this.space.clear();
        }
    }

    Consumer<ConnectionPoolEntity> consumer;

    ConnectionPoolEntity lastBucketMin;
    ConnectionPoolEntity lastBucketHour;
    final Map<ConnectionPoolInstanceAcceptor.ContainerType, ConnectionPoolInstanceAcceptor.Container<?>> containerMap = new EnumMap<>(ConnectionPoolInstanceAcceptor.ContainerType.class);

    public <T extends Number> void push(ConnectionPoolInstanceAcceptor.ContainerType type, Document usage, ConnectionPoolField key, Class<T> matterClass, T defaultMatter) {
        ConnectionPoolInstanceAcceptor.Container<T> container = (ConnectionPoolInstanceAcceptor.Container<T>) containerMap.get(type);
        assert container != null;
        container.push(usage, key, matterClass, defaultMatter);
    }

    public ConnectionPoolInstanceAcceptor(ConnectionPoolEntity lastBucketMin, ConnectionPoolEntity lastBucketHour, Consumer<ConnectionPoolEntity> consumer) {
        this.lastBucketMin = lastBucketMin;
        this.lastBucketHour = lastBucketHour;
        this.consumer = consumer;
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_CONNECTIONS_MAX, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setMaxConnections)));
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_CONNECTIONS_USED, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setUsedConnections)));
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_QUEUE_SIZE, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setQueueSize)));
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_CONNECTIONS_MAX, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setMaxConnections)));
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_CONNECTIONS_USED, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setUsedConnections)));
        this.containerMap.computeIfAbsent(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_QUEUE_SIZE, k -> new ConnectionPoolInstanceAcceptor.Container<>(k.getInitSize(), (u, items) -> acceptAvg(items, u::setQueueSize)));
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String serverId = entity.get(ConnectionPoolField.PROCESS_ID.field(), String.class);
        String connectionId = entity.get(ConnectionPoolField.CONNECTION_ID.field(), String.class);
        long lastUpdateTime = Optional.ofNullable(entity.get(ConnectionPoolField.LAST_UPDATE_TIME.field(), Long.class)).orElse(0L) / 1000L;
        long bucketHour = TimeGranularity.HOUR.fixTime(lastUpdateTime) * 1000L;
        long bucketMin = TimeGranularity.MINUTE.fixTime(lastUpdateTime) * 1000L;
        if (null != lastBucketMin && lastBucketMin.getLastUpdateTime() != bucketMin) {
            acceptMin();
            lastBucketMin = null;
        }
        if (null != lastBucketHour && lastBucketHour.getLastUpdateTime() != bucketHour) {
            acceptHour();
            lastBucketHour = null;
        }
        if (null == lastBucketMin) {
            lastBucketMin = instance(TimeGranularity.MINUTE, bucketMin, serverId, connectionId);
        }
        if (null == lastBucketHour) {
            lastBucketHour = instance(TimeGranularity.HOUR, bucketHour, serverId, connectionId);
        }
        append(entity);
    }

    protected ConnectionPoolEntity instance(TimeGranularity timeGranularity, long bucketTime, String serverId, String connectionId) {
        ConnectionPoolEntity entity = new ConnectionPoolEntity();
        entity.setProcessId(serverId);
        entity.setTimeGranularity(timeGranularity.getType());
        entity.setLastUpdateTime(bucketTime);
        entity.setConnectionId(connectionId);
        return entity;
    }

    void append(Document usage) {
        push(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_CONNECTIONS_MAX, usage, ConnectionPoolField.MAX_CONNECTIONS, Integer.class, 0);
        push(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_CONNECTIONS_USED, usage, ConnectionPoolField.USED_CONNECTIONS, Integer.class, 0);
        push(ConnectionPoolInstanceAcceptor.ContainerType.MINUTE_POOL_QUEUE_SIZE, usage, ConnectionPoolField.QUEUE_SIZE, Integer.class, 0);
        push(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_CONNECTIONS_MAX, usage, ConnectionPoolField.MAX_CONNECTIONS, Integer.class, 0);
        push(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_CONNECTIONS_USED, usage, ConnectionPoolField.USED_CONNECTIONS, Integer.class, 0);
        push(ConnectionPoolInstanceAcceptor.ContainerType.HOUR_POOL_QUEUE_SIZE, usage, ConnectionPoolField.QUEUE_SIZE, Integer.class, 0);
    }

    void acceptMin() {
        if (null == lastBucketMin) {
            return;
        }
        accept(lastBucketMin, ConnectionPoolInstanceAcceptor.ContainerType.ofMinute());
        consumer.accept(lastBucketMin);
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

    void accept(ConnectionPoolEntity usage, ConnectionPoolInstanceAcceptor.ContainerType... types) {
        for (ConnectionPoolInstanceAcceptor.ContainerType type : types) {
            ConnectionPoolInstanceAcceptor.Container<?> container = containerMap.get(type);
            assert null != container;
            container.pullAndClean(usage);
        }
    }

    void acceptHour() {
        if (null == lastBucketHour) {
            return;
        }
        accept(lastBucketHour, ConnectionPoolInstanceAcceptor.ContainerType.ofHour());
        consumer.accept(lastBucketHour);
    }

    @Override
    public void close() {
        acceptMin();
        acceptHour();
    }
}
