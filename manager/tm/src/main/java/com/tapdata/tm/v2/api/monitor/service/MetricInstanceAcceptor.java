package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 15:10 Create
 * @description
 */
public final class MetricInstanceAcceptor implements AcceptorBase {
    public static final String UN_KNOW = "UN_KNOWN";
    BiFunction<Boolean, ApiMetricsRaw, Void> consumer;

    Long lastCallTime;
    @Data
    public static class BucketInfo {
        ApiMetricsRaw lastBucketMin;
        ApiMetricsRaw lastBucketHour;

        public BucketInfo(ApiMetricsRaw min, ApiMetricsRaw hour) {
            this.lastBucketMin = min;
            this.lastBucketHour = hour;
        }
    }

    ApiMetricsRaw lastBucketMin;
    ApiMetricsRaw lastBucketHour;
    ObjectId lastCallId;

    MetricTypes metricType;
    Function<Long, BucketInfo> bucketInfoGetter;

    boolean needWorkerInfo = false;

    MetricInstanceAcceptor beSaveWorkerInfo() {
        this.needWorkerInfo = true;
        return this;
    }

    public MetricInstanceAcceptor lastCallTime(Long lastCallTime) {
        this.lastCallTime = null != lastCallTime ? lastCallTime / 1000L : null;
        return this;
    }

    public MetricInstanceAcceptor(MetricTypes metricType,
                                  Function<Long, BucketInfo> bucketInfoGetter,
                                  BiFunction<Boolean, ApiMetricsRaw, Void> consumer) {
        this.bucketInfoGetter = bucketInfoGetter;
        this.lastBucketMin = null;
        this.lastBucketHour = null;
        this.consumer = consumer;
        this.metricType = metricType;
    }

    BucketInfo acceptLastMinute(MetricInstanceFactory.CallInfo entity, ObjectId topCallId) {
        String apiId = entity.getApiId();
        String reqPath = entity.getReqPath();
        String serverId = entity.getServerId();
        long requestCost = entity.getRequestCost();
        long dbCost = entity.getDbCost();
        boolean isOk = entity.isOk();
        long reqBytes = entity.getReqBytes();
        String workerId = entity.getWorkerId();
        long bucketSec = entity.getBucketSec();
        long bucketMin = entity.getBucketMin();
        BucketInfo lessBucket = this.bucketInfoGetter.apply(bucketMin);
        if (null == lessBucket.getLastBucketMin()) {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, reqPath, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
            subMetrics.put(bucketSec, sub);
            lessBucket.setLastBucketMin(ApiMetricsRaw.instance(serverId, reqPath, apiId, bucketMin, TimeGranularity.MINUTE, metricType));
            lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
        }
        updateLastCallId(lessBucket.getLastBucketMin(), topCallId);
        lessBucket.getLastBucketMin().merge(isOk, reqBytes, requestCost, dbCost);
        lessBucket.getLastBucketMin().mergeWorker(workerId, isOk, needWorkerInfo);
        if (null == lessBucket.getLastBucketMin().getSubMetrics()) {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, reqPath, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
            subMetrics.put(bucketSec, sub);
            lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
        }
        Map<Long, ApiMetricsRaw> subMetrics = lessBucket.getLastBucketMin().getSubMetrics();
        ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, reqPath, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
        sub.merge(isOk, reqBytes, requestCost, dbCost);
        sub.mergeWorker(workerId, isOk, needWorkerInfo);
        acceptOnce(lessBucket.getLastBucketMin(), true);
        return lessBucket;
    }

    void acceptLastHour(MetricInstanceFactory.CallInfo entity, BucketInfo lessBucket, ObjectId topCallId) {
        String serverId = entity.getServerId();
        String apiId = entity.getApiId();
        long requestCost = entity.getRequestCost();
        long dbCost = entity.getDbCost();
        boolean isOk = entity.isOk();
        long reqBytes = entity.getReqBytes();
        String workerId = entity.getWorkerId();
        long bucketHour = entity.getBucketHour();
        if (lessBucket.getLastBucketHour() == null) {
            lessBucket.setLastBucketHour(ApiMetricsRaw.instance(serverId, entity.getReqPath(), apiId, bucketHour, TimeGranularity.HOUR, metricType));
        }
        updateLastCallId(lessBucket.getLastBucketHour(), topCallId);
        lessBucket.getLastBucketHour().merge(isOk, reqBytes, requestCost, dbCost);
        lessBucket.getLastBucketHour().mergeWorker(workerId, isOk, needWorkerInfo);
        acceptOnce(lessBucket.getLastBucketHour(), true);
    }

    ObjectId compareOid(ObjectId callId) {
        return this.lastCallId != null && this.lastCallId.compareTo(callId) >= 0 ? this.lastCallId : callId;
    }

    public void accept(MetricInstanceFactory.CallInfo entity) {
        String apiId = entity.getApiId();
        String serverId = entity.getServerId();
        long requestCost = entity.getRequestCost();
        long dbCost = entity.getDbCost();
        boolean isOk = entity.isOk();
        long reqBytes = entity.getReqBytes();
        boolean supplement = entity.isSupplement();
        String workerId = entity.getWorkerId();
        long bucketSec = entity.getBucketSec();
        ObjectId callId = entity.getCallId();
        ObjectId topCallId = compareOid(callId);
        if (this.lastCallId != null && this.lastCallId.compareTo(callId) >= 0 && !supplement) {
            return;
        }
        BucketInfo lessBucket = acceptMinuteIfNeed(entity, topCallId);
        boolean supplementMin = null != lessBucket;
        boolean supplementHour = acceptHourIfNeed(entity, topCallId, lessBucket);
        if (!supplementMin) {
            initMinuteIfNeed(entity);
            initSecondIfNeed(entity);
            Map<Long, ApiMetricsRaw> subMetrics = lastBucketMin.getSubMetrics();
            ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, entity.getReqPath(), apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
            sub.merge(isOk, reqBytes, requestCost, dbCost);
            sub.mergeWorker(workerId, isOk, needWorkerInfo);
            updateLastCallId(lastBucketMin, topCallId);
            lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
            lastBucketMin.mergeWorker(workerId, isOk, needWorkerInfo);
        }
        if (!supplementHour) {
            initHourIfNeed(entity);
            updateLastCallId(lastBucketHour, topCallId);
            lastBucketHour.merge(isOk, reqBytes, requestCost, dbCost);
            lastBucketHour.mergeWorker(workerId, isOk, needWorkerInfo);
        }
    }

    void initMinuteIfNeed(MetricInstanceFactory.CallInfo entity) {
        if (null != lastBucketMin) {
            return;
        }
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
        lastBucketHour = ApiMetricsRaw.instance(entity.getServerId(), entity.getReqPath(), entity.getApiId(), entity.getBucketHour(), TimeGranularity.HOUR, metricType);
    }

    BucketInfo acceptMinuteIfNeed(MetricInstanceFactory.CallInfo entity, ObjectId topCallId) {
        long bucketMin = entity.getBucketMin();
        if (null != lastBucketMin && lastBucketMin.getTimeStart() != bucketMin) {
            if (bucketMin < lastBucketMin.getTimeStart()) {
                return acceptLastMinute(entity, topCallId);
            } else {
                acceptOnce(lastBucketMin);
                lastBucketMin = null;
            }
        } else if (null == lastBucketMin) {
            if (entity.isSupplement() && this.lastCallTime != null && this.lastCallTime > bucketMin) {
                return acceptLastMinute(entity, topCallId);
            }
        }
        return null;
    }

    boolean acceptHourIfNeed(MetricInstanceFactory.CallInfo entity, ObjectId topCallId, BucketInfo lessBucket) {
        long bucketHour = entity.getBucketHour();
        if (null != lastBucketHour && lastBucketHour.getTimeStart() != bucketHour) {
            if (bucketHour < lastBucketHour.getTimeStart()) {
                assert lessBucket != null;
                acceptLastHour(entity, lessBucket, topCallId);
                return true;
            } else {
                acceptOnce(lastBucketHour);
                lastBucketHour = null;
            }
        } else if (null == lastBucketHour) {
            if (entity.isSupplement() && this.lastCallTime != null && this.lastCallTime > bucketHour) {
                assert lessBucket != null;
                acceptLastHour(entity, lessBucket, topCallId);
                return true;
            }
        }
        return false;
    }

    void updateLastCallId(ApiMetricsRaw raw, ObjectId callId) {
        if (raw.getLastCallId() == null || raw.getLastCallId().compareTo(callId) < 0) {
            raw.setLastCallId(callId);
        }
    }


    void acceptOnce(ApiMetricsRaw item) {
        acceptOnce(item, false);
    }

    void acceptOnce(ApiMetricsRaw item, boolean uploadQuick) {
        if (null == item) {
            return;
        }
        Map<Long, ApiMetricsRaw> subMetrics = item.getSubMetrics();
        if (!CollectionUtils.isEmpty(subMetrics)) {
            subMetrics.values()
                    .forEach(MetricInstanceAcceptor::calcPValue);
        }
        MetricInstanceAcceptor.calcPValue(item);
        consumer.apply(uploadQuick, item);
    }

    public static ApiMetricsRaw calcPValue(ApiMetricsRaw item) {
        if (null == item) {
            return item;
        }
        int total = item.getReqCount().intValue();
        List<Map<String, Number>> delay = item.getDelay();
        Long p50 = ApiMetricsDelayUtil.p50(delay, total);
        Long p95 = ApiMetricsDelayUtil.p95(delay, total);
        Long p99 = ApiMetricsDelayUtil.p99(delay, total);
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
    }
}
