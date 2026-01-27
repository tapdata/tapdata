package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    @Data
    public static class BucketInfo {
        ApiMetricsRaw lastBucketMin;
        ApiMetricsRaw lastBucketHour;
        ApiMetricsRaw lastBucketDay;

        public BucketInfo(ApiMetricsRaw min, ApiMetricsRaw hour, ApiMetricsRaw day) {
            this.lastBucketMin = min;
            this.lastBucketHour = hour;
            this.lastBucketDay = day;
        }
    }

    ApiMetricsRaw lastBucketMin;
    ApiMetricsRaw lastBucketHour;
    ApiMetricsRaw lastBucketDay;
    ObjectId lastCallId;

    MetricTypes metricType;
    Function<Long, BucketInfo> bucketInfoGetter;

    boolean needWorkerInfo = false;

    MetricInstanceAcceptor beSaveWorkerInfo() {
        this.needWorkerInfo = true;
        return this;
    }

    public MetricInstanceAcceptor(MetricTypes metricType,
                                  Function<Long, BucketInfo> bucketInfoGetter,
                                  BiFunction<Boolean, ApiMetricsRaw, Void> consumer) {
        this.bucketInfoGetter = bucketInfoGetter;
        BucketInfo apply = bucketInfoGetter.apply(null);
        this.lastBucketMin = apply.getLastBucketMin();
        if (this.lastBucketMin != null) {
            this.lastCallId = this.lastBucketMin.getLastCallId();
        }
        this.lastBucketHour = apply.getLastBucketHour();
        this.lastBucketDay = apply.getLastBucketDay();
        this.consumer = consumer;
        this.metricType = metricType;
    }

    long value(Long val) {
        if (null == val) {
            return 0L;
        }
        return Math.max(0L, val);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String allPathId = entity.get(ApiCallField.ALL_PATH_ID.field(), String.class);
        String reqPath = entity.get(ApiCallField.REQ_PATH.field(), String.class);
        String apiId = Optional.ofNullable(allPathId)
                .orElse(Optional.ofNullable(reqPath).orElse(UN_KNOW));
        String serverId = entity.get(ApiCallField.API_GATEWAY_UUID.field(), String.class);
        if (StringUtils.isBlank(apiId)) {
            return;
        }
        long requestCost = value(entity.get(ApiCallField.LATENCY.field(), Long.class));
        long dbCost = value(entity.get(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), Long.class));
        boolean isOk = ApiMetricsCompressValueUtil.checkByCode(entity.get(ApiCallField.CODE.field(), String.class), entity.get(ApiCallField.HTTP_STATUS.field(), String.class));
        long reqBytes = value(entity.get(ApiCallField.REQ_BYTES.field(), Long.class));
        boolean supplement = Optional.ofNullable(entity.get(ApiCallField.SUPPLEMENT.field(), Boolean.class)).orElse(false);
        String workerId = entity.get(ApiCallField.WORK_O_ID.field(), String.class);
        Long apiCallReqTime = entity.get(ApiCallField.REQ_TIME.field(), Long.class);
        long reqTimeOSec = Optional.ofNullable(apiCallReqTime).orElse(0L) / 1000L;
        long bucketSec = TimeGranularity.SECOND_FIVE.fixTime(reqTimeOSec);
        long bucketMin = TimeGranularity.MINUTE.fixTime(reqTimeOSec);
        long bucketHour = TimeGranularity.HOUR.fixTime(reqTimeOSec);
        long bucketDay = TimeGranularity.DAY.fixTime(reqTimeOSec);
        ObjectId callId = entity.get(BaseEntityFields._ID.field(), ObjectId.class);
        ObjectId topCallId = this.lastCallId != null && this.lastCallId.compareTo(callId) >= 0 ? this.lastCallId : callId;
        if (this.lastCallId != null && this.lastCallId.compareTo(callId) >= 0 && !supplement) {
            return;
        }

        BucketInfo lessBucket = null;
        if (null != lastBucketMin && lastBucketMin.getTimeStart() != bucketMin) {
            if (bucketMin < lastBucketMin.getTimeStart()) {
                lessBucket = this.bucketInfoGetter.apply(bucketMin);
                if (null == lessBucket.getLastBucketMin()) {
                    Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
                    ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
                    subMetrics.put(bucketSec, sub);
                    lessBucket.setLastBucketMin(ApiMetricsRaw.instance(serverId, apiId, bucketMin, TimeGranularity.MINUTE, metricType));
                    lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
                }
                lastCallId(lessBucket.getLastBucketMin(), topCallId);
                lessBucket.getLastBucketMin().merge(isOk, reqBytes, requestCost, dbCost);
                lessBucket.getLastBucketMin().mergeWorker(workerId, isOk, needWorkerInfo);
                if (null == lessBucket.getLastBucketMin().getSubMetrics()) {
                    Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
                    ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
                    subMetrics.put(bucketSec, sub);
                    lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
                }
                Map<Long, ApiMetricsRaw> subMetrics = lessBucket.getLastBucketMin().getSubMetrics();
                ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
                sub.merge(isOk, reqBytes, requestCost, dbCost);
                sub.mergeWorker(workerId, isOk, needWorkerInfo);
                acceptOnce(lessBucket.getLastBucketMin(), true);
            } else {
                acceptOnce(lastBucketMin);
                lastBucketMin = null;
            }
        }
        if (null != lastBucketHour && lastBucketHour.getTimeStart() != bucketHour) {
            if (bucketHour < lastBucketHour.getTimeStart()) {
                assert lessBucket != null;
                if (lessBucket.getLastBucketHour() == null) {
                    lessBucket.setLastBucketHour(ApiMetricsRaw.instance(serverId, apiId, bucketHour, TimeGranularity.HOUR, metricType));
                }
                lastCallId(lessBucket.getLastBucketHour(), topCallId);
                lessBucket.getLastBucketHour().merge(isOk, reqBytes, requestCost, dbCost);
                lessBucket.getLastBucketHour().mergeWorker(workerId, isOk, needWorkerInfo);
                acceptOnce(lessBucket.getLastBucketHour(), true);
            } else {
                acceptOnce(lastBucketHour);
                lastBucketHour = null;
            }
        }
        if (null != lastBucketDay && lastBucketDay.getTimeStart() != bucketDay) {
            if (bucketDay < lastBucketDay.getTimeStart()) {
                assert lessBucket != null;
                if (lessBucket.getLastBucketDay() == null) {
                    lessBucket.setLastBucketDay(ApiMetricsRaw.instance(serverId, apiId, bucketDay, TimeGranularity.DAY, metricType));
                }
                lastCallId(lessBucket.getLastBucketDay(), topCallId);
                lessBucket.getLastBucketDay().merge(isOk, reqBytes, requestCost, dbCost);
                lessBucket.getLastBucketDay().mergeWorker(workerId, isOk, needWorkerInfo);
                acceptOnce(lessBucket.getLastBucketDay(), true);
            } else {
                acceptOnce(lastBucketDay);
                lastBucketDay = null;
            }
        }

        if (null == lastBucketMin) {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
            subMetrics.put(bucketSec, sub);
            lastBucketMin = ApiMetricsRaw.instance(serverId, apiId, bucketMin, TimeGranularity.MINUTE, metricType);
            lastBucketMin.setSubMetrics(subMetrics);
        }
        if (null == lastBucketHour) {
            lastBucketHour = ApiMetricsRaw.instance(serverId, apiId, bucketHour, TimeGranularity.HOUR, metricType);
        }
        if (null == lastBucketDay) {
            lastBucketDay = ApiMetricsRaw.instance(serverId, apiId, bucketDay, TimeGranularity.DAY, metricType);
        }

        if (null == lastBucketMin.getSubMetrics()) {
            Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
            ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
            subMetrics.put(bucketSec, sub);
            lastBucketMin.setSubMetrics(subMetrics);
        }
        Map<Long, ApiMetricsRaw> subMetrics = lastBucketMin.getSubMetrics();
        ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
        sub.merge(isOk, reqBytes, requestCost, dbCost);
        sub.mergeWorker(workerId, isOk, needWorkerInfo);

        lastCallId(lastBucketMin, topCallId);
        lastCallId(lastBucketHour, topCallId);
        lastCallId(lastBucketDay, topCallId);
        lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketHour.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketDay.merge(isOk, reqBytes, requestCost, dbCost);

        lastBucketMin.mergeWorker(workerId, isOk, needWorkerInfo);
        lastBucketHour.mergeWorker(workerId, isOk, needWorkerInfo);
        lastBucketDay.mergeWorker(workerId, isOk, needWorkerInfo);
    }

    void lastCallId(ApiMetricsRaw raw, ObjectId callId) {
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
        acceptOnce(lastBucketDay);
    }
}
