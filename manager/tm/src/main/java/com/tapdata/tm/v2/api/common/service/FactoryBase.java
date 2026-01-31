package com.tapdata.tm.v2.api.common.service;

import lombok.Getter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.CollectionUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/8 14:04 Create
 * @description
 */
@Getter
public abstract class FactoryBase<E, A extends AcceptorBase> implements Closeable {
    protected static final int BATCH_SIZE = 100;
    protected final Consumer<List<E>> consumer;
    protected final List<E> apiMetricsRaws;
    protected final Map<String, A> instanceMap;
    protected final Function<Query, E> findOne;
    protected boolean needUpdateTag = false;

    protected FactoryBase(Consumer<List<E>> consumer, Function<Query, E> findOne) {
        this.apiMetricsRaws = new ArrayList<>();
        this.instanceMap = new HashMap<>();
        this.consumer = consumer;
        this.findOne = findOne;
    }

    public boolean needUpdate() {
        return needUpdateTag;
    }

    public void needUpdate(boolean needUpdateTag) {
        this.needUpdateTag = needUpdateTag;
    }

    public void flush() {
        if (this.apiMetricsRaws.isEmpty()) {
            return;
        }
        consumer.accept(apiMetricsRaws);
        this.apiMetricsRaws.clear();
    }

    @Override
    public void close() {
        if (!CollectionUtils.isEmpty(instanceMap)) {
            instanceMap.forEach((k, v) -> v.close());
        }
        if (!needUpdateTag) {
            return;
        }
        flush();
    }

    public void append(List<E> rows) {
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }
        this.apiMetricsRaws.addAll(rows);
    }
}
