package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 15:10 Create
 * @description
 */
public final class MetricInstanceAcceptor implements AcceptorBase {
    public static final String UN_KNOW = "UN_KNOWN";
    Consumer<ApiMetricsRaw> consumer;

    ApiMetricsRaw lastBucketMin;
    ApiMetricsRaw lastBucketHour;
    ApiMetricsRaw lastBucketDay;

    MetricTypes metricType;
    BiFunction<Long, TimeGranularity, ApiMetricsRaw> bucketInfoGetter;

    boolean needWorkerInfo = false;

    MetricInstanceAcceptor beSaveWorkerInfo() {
        this.needWorkerInfo = true;
        return this;
    }

    public MetricInstanceAcceptor(MetricTypes metricType,
                                  BiFunction<Long, TimeGranularity, ApiMetricsRaw> bucketInfoGetter,
                                  Consumer<ApiMetricsRaw> consumer) {
        this.bucketInfoGetter = bucketInfoGetter;
        this.consumer = consumer;
        this.metricType = metricType;
    }

    public void accept(MetricInstanceFactory.CallInfo entity) {
        String apiId = entity.getApiId();
        String serverId = entity.getServerId();
        double requestCost = entity.getRequestCost();
        double dbCost = entity.getDbCost();
        boolean isOk = entity.isOk();
        long reqBytes = entity.getReqBytes();
        String workerId = entity.getWorkerId();
        long bucketSec = entity.getBucketSec();

        acceptMinuteIfNeed(entity);
        acceptHourIfNeed(entity);
        acceptDayIfNeed(entity);

        initMinuteIfNeed(entity);
        initSecondIfNeed(entity);
        Map<Long, ApiMetricsRaw> subMetrics = lastBucketMin.getSubMetrics();
        ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, entity.getReqPath(), apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
        sub.merge(isOk, reqBytes, requestCost, dbCost);
        sub.mergeWorker(workerId, isOk, needWorkerInfo);
        lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketMin.mergeWorker(workerId, isOk, needWorkerInfo);

        initHourIfNeed(entity);
        lastBucketHour.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketHour.mergeWorker(workerId, isOk, needWorkerInfo);

        initDayIfNeed(entity);
        lastBucketDay.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketDay.mergeWorker(workerId, isOk, needWorkerInfo);
    }

    void initMinuteIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null != lastBucketMin) {
            return;
        }
        lastBucketMin = bucketInfoGetter.apply(entity.getBucketMin(), TimeGranularity.MINUTE);
        if (null == lastBucketMin) {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw sub = ApiMetricsRaw.instance(
                    entity.getServerId(),
                    entity.getReqPath(),
                    entity.getApiId(),
                    entity.getBucketSec(),
                    TimeGranularity.SECOND_FIVE,
                    metricType);
            subMetrics.put(entity.getBucketSec(), sub);
            lastBucketMin = ApiMetricsRaw.instance(entity.getServerId(), entity.getReqPath(), entity.getApiId(), entity.getBucketMin(), TimeGranularity.MINUTE, metricType);
            lastBucketMin.setSubMetrics(subMetrics);
        }
    }

    void initSecondIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null != lastBucketMin.getSubMetrics()) {
            return;
        }
        Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
        ApiMetricsRaw sub = ApiMetricsRaw.instance(entity.getServerId(), entity.getReqPath(), entity.getApiId(), entity.getBucketSec(), TimeGranularity.SECOND_FIVE, metricType);
        subMetrics.put(entity.getBucketSec(), sub);
        lastBucketMin.setSubMetrics(subMetrics);
    }

    void initHourIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null != lastBucketHour) {
            return;
        }
        lastBucketHour = bucketInfoGetter.apply(entity.getBucketHour(), TimeGranularity.HOUR);
        if (null == lastBucketHour) {
            lastBucketHour = ApiMetricsRaw.instance(entity.getServerId(), entity.getReqPath(), entity.getApiId(), entity.getBucketHour(), TimeGranularity.HOUR, metricType);
        }
    }

    void initDayIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null != lastBucketDay) {
            return;
        }
        lastBucketDay = bucketInfoGetter.apply(entity.getBucketDay(), TimeGranularity.DAY);
        if (null != lastBucketDay) {
            return;
        }
        lastBucketDay = ApiMetricsRaw.instance(entity.getServerId(), entity.getReqPath(), entity.getApiId(), entity.getBucketDay(), TimeGranularity.DAY, metricType);
    }

    void acceptMinuteIfNeed(MetricInstanceFactory.CallInfo entity) {
        long bucketMin = entity.getBucketMin();
        if (null != lastBucketMin && lastBucketMin.getTimeStart() != bucketMin) {
            acceptOnce(lastBucketMin);
            lastBucketMin = null;
        }
    }

    void acceptHourIfNeed(MetricInstanceFactory.CallInfo entity) {
        long bucketHour = entity.getBucketHour();
        if (null != lastBucketHour && lastBucketHour.getTimeStart() != bucketHour) {
            acceptOnce(lastBucketHour);
            lastBucketHour = null;
        }
    }

    void acceptDayIfNeed(MetricInstanceFactory.CallInfo entity) {
        long bucketDay = entity.getBucketDay();
        if (null != lastBucketDay && lastBucketDay.getTimeStart() != bucketDay) {
            acceptOnce(lastBucketDay);
            lastBucketDay = null;
        }
    }

    void acceptOnce(ApiMetricsRaw item) {
        if (null == item) {
            return;
        }
        Map<Long, ApiMetricsRaw> subMetrics = item.getSubMetrics();
        if (!CollectionUtils.isEmpty(subMetrics)) {
            subMetrics.values()
                    .forEach(MetricInstanceAcceptor::calcPValue);
        }
        MetricInstanceAcceptor.calcPValue(item);
        consumer.accept(item);
    }

    public static ApiMetricsRaw calcPValue(ApiMetricsRaw item) {
        if (null == item) {
            return item;
        }
        int total = item.getReqCount().intValue();
        List<Map<String, Number>> delay = item.getDelay();
        Double p50 = ApiMetricsDelayUtil.p50(delay, total);
        Double p95 = ApiMetricsDelayUtil.p95(delay, total);
        Double p99 = ApiMetricsDelayUtil.p99(delay, total);
        ApiMetricsDelayUtil.readMaxAndMin(delay, item::setMaxDelay, item::setMinDelay);
        item.setP50(p50);
        item.setP95(p95);
        item.setP99(p99);
        return item;
    }

    @Override
    public void close() {
        acceptOnce(lastBucketMin);
        acceptOnce(lastBucketHour);
        acceptOnce(lastBucketDay);
    }
}