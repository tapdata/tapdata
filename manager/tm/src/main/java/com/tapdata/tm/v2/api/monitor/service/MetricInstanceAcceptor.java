package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String allPathId = entity.get("allPathId", String.class);
        String reqPath = entity.get("req_path", String.class);
        String apiId = Optional.ofNullable(allPathId)
                .orElse(Optional.ofNullable(reqPath).orElse(UN_KNOW));
        String serverId = entity.get("api_gateway_uuid", String.class);
        if (StringUtils.isBlank(apiId)) {
            return;
        }
        long requestCost = Optional.ofNullable(entity.get("latency", Long.class)).orElse(0L);
        long dbCost = Optional.ofNullable(entity.get("dataQueryTotalTime", Long.class)).orElse(0L);
        boolean isOk = ApiMetricsDelayInfoUtil.checkByCode(entity.get("code", String.class), entity.get("httpStatus", String.class));
        long reqBytes = Optional.ofNullable(entity.get("req_bytes", Long.class)).orElse(0L);
        Long apiCallReqTime = entity.get("reqTime", Long.class);
        long reqTimeOSec = Optional.ofNullable(apiCallReqTime).orElse(0L) / 1000L;
        long bucketSec = TimeGranularity.SECOND_FIVE.fixTime(reqTimeOSec);
        long bucketMin = TimeGranularity.MINUTE.fixTime(reqTimeOSec);
        long bucketHour = TimeGranularity.HOUR.fixTime(reqTimeOSec);
        long bucketDay = TimeGranularity.DAY.fixTime(reqTimeOSec);
        ObjectId callId = entity.get("_id", ObjectId.class);
        if (this.lastCallId != null && this.lastCallId.compareTo(callId) >= 0) {
            System.out.println("Too old api call data");
            return;
        }

        BucketInfo lessBucket = null;
        if (null != lastBucketMin && lastBucketMin.getTimeStart() != bucketMin) {
            if (bucketMin < lastBucketMin.getTimeStart()) {
//                System.out.println("Less minute time: " + bucketMin);
                lessBucket = this.bucketInfoGetter.apply(bucketMin);
                if (null == lessBucket.getLastBucketMin()) {
                    Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
                    ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
                    subMetrics.put(bucketSec, sub);
                    lessBucket.setLastBucketMin(ApiMetricsRaw.instance(serverId, apiId, bucketMin, TimeGranularity.MINUTE, metricType));
                    lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
                }
                lessBucket.getLastBucketMin().setLastCallId(callId);
                lessBucket.getLastBucketMin().merge(isOk, reqBytes, requestCost, dbCost);
                if (null == lessBucket.getLastBucketMin().getSubMetrics()) {
                    Map<Long, ApiMetricsRaw> subMetrics = new HashMap<>();
                    ApiMetricsRaw sub = ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType);
                    subMetrics.put(bucketSec, sub);
                    lessBucket.getLastBucketMin().setSubMetrics(subMetrics);
                }
                Map<Long, ApiMetricsRaw> subMetrics = lessBucket.getLastBucketMin().getSubMetrics();
                ApiMetricsRaw sub = subMetrics.computeIfAbsent(bucketSec, k -> ApiMetricsRaw.instance(serverId, apiId, bucketSec, TimeGranularity.SECOND_FIVE, metricType));
                sub.merge(isOk, reqBytes, requestCost, dbCost);
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
                lessBucket.getLastBucketHour().setLastCallId(callId);
                lessBucket.getLastBucketHour().merge(isOk, reqBytes, requestCost, dbCost);
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
                lessBucket.getLastBucketDay().setLastCallId(callId);
                lessBucket.getLastBucketDay().merge(isOk, reqBytes, requestCost, dbCost);
                acceptOnce(lessBucket.getLastBucketDay(), true);
            }
            acceptOnce(lastBucketDay);
            lastBucketDay = null;
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

        lastBucketMin.setLastCallId(callId);
        lastBucketHour.setLastCallId(callId);
        lastBucketDay.setLastCallId(callId);
        lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketHour.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketDay.merge(isOk, reqBytes, requestCost, dbCost);
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
        List<Map<Long, Integer>> delay = ApiMetricsDelayUtil.fixDelayAsMap(item.getDelay());
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
