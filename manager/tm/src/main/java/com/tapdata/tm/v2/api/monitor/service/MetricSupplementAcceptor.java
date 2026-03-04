package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/31 14:40 Create
 * @description
 */
public class MetricSupplementAcceptor implements AcceptorBase {

    ApiMetricsRaw lastBucketMin;
    Consumer<ApiMetricsRaw> consumer;
    MetricTypes metricType;
    Function<Long, ApiMetricsRaw> bucketInfoGetter;

    boolean needWorkerInfo = false;

    MetricSupplementAcceptor beSaveWorkerInfo() {
        this.needWorkerInfo = true;
        return this;
    }

    public void accept(MetricInstanceFactory.CallInfo entity) {
        String serverId = entity.getServerId();
        String apiId = entity.getApiId();
        double requestCost = entity.getRequestCost();
        double dbCost = entity.getDbCost();
        boolean isOk = entity.isOk();
        long reqBytes = entity.getReqBytes();
        String workerId = entity.getWorkerId();
        long bucketMin = entity.getBucketMin();
        if (lastBucketMin == null) {
            this.lastBucketMin = createOrNewInstance(bucketMin, entity);
            initSecondIfNeed(entity);
        } else if (this.lastBucketMin.getTimeStart() != bucketMin) {
            acceptOnce(this.lastBucketMin);
            this.lastBucketMin = null;
            this.lastBucketMin = createOrNewInstance(bucketMin, entity);
            initSecondIfNeed(entity);
        }
        this.lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
        Map<Long, ApiMetricsRaw> subMetrics = lastBucketMin.getSubMetrics();
        ApiMetricsRaw sub = subMetrics.computeIfAbsent(entity.getBucketSec(), k -> ApiMetricsRaw.instance(serverId, entity.getReqPath(), apiId, entity.getBucketSec(), TimeGranularity.SECOND_FIVE, metricType));
        sub.merge(isOk, reqBytes, requestCost, dbCost);
        sub.mergeWorker(workerId, isOk, needWorkerInfo);
    }

    ApiMetricsRaw createOrNewInstance(long bucketMin, MetricInstanceFactory.CallInfo entity) {
        String apiId = entity.getApiId();
        String serverId = entity.getServerId();
        return Optional.ofNullable(bucketInfoGetter.apply(bucketMin))
                .orElse(ApiMetricsRaw.instance(serverId, entity.getReqPath(), apiId, bucketMin, TimeGranularity.MINUTE, metricType));
    }

    void initSecondIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null == lastBucketMin.getSubMetrics()) {
            ApiMetricsRaw sub = ApiMetricsRaw.instance(
                    entity.getServerId(),
                    entity.getReqPath(),
                    entity.getApiId(),
                    entity.getBucketSec(),
                    TimeGranularity.SECOND_FIVE,
                    metricType);
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            subMetrics.put(entity.getBucketSec(), sub);
            lastBucketMin.setSubMetrics(subMetrics);
        }
    }

    void acceptOnce(ApiMetricsRaw item) {
        Map<Long, ApiMetricsRaw> subMetrics = item.getSubMetrics();
        if (!CollectionUtils.isEmpty(subMetrics)) {
            subMetrics.values()
                    .forEach(MetricInstanceAcceptor::calcPValue);
        }
        MetricInstanceAcceptor.calcPValue(item);
        consumer.accept(item);
    }


    @Override
    public void close() {

    }
}
