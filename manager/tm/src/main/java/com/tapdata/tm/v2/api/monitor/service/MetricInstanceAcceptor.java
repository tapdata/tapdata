package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public MetricInstanceAcceptor(MetricTypes metricType,
                                  ApiMetricsRaw lastBucketMin,
                                  ApiMetricsRaw lastBucketHour,
                                  ApiMetricsRaw lastBucketDay,
                                  Consumer<ApiMetricsRaw> consumer) {
        this.lastBucketMin = lastBucketMin;
        this.lastBucketHour = lastBucketHour;
        this.lastBucketDay = lastBucketDay;
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
        long reqTimeOSec = Optional.ofNullable(entity.get("reqTime", Long.class)).orElse(0L) / 1000L;
        long bucketSec = (reqTimeOSec / 5L) * 5L;
        long bucketMin = (reqTimeOSec / 60L) * 60L;
        long bucketHour = (reqTimeOSec / 3600L) * 3600L;
        long bucketDay = (reqTimeOSec / 86400L) * 86400L;
        if (null != lastBucketMin && lastBucketMin.getTimeStart() != bucketMin) {
            acceptOnce(lastBucketMin);
            lastBucketMin = null;
        }
        if (null != lastBucketHour && lastBucketHour.getTimeStart() != bucketHour) {
            acceptOnce(lastBucketHour);
            lastBucketHour = null;
        }
        if (null != lastBucketDay && lastBucketDay.getTimeStart() != bucketDay) {
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
        final ObjectId callId = entity.get("_id", ObjectId.class);
        lastBucketMin.setCallId(callId);
        lastBucketHour.setCallId(callId);
        lastBucketDay.setCallId(callId);
        lastBucketMin.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketHour.merge(isOk, reqBytes, requestCost, dbCost);
        lastBucketDay.merge(isOk, reqBytes, requestCost, dbCost);
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
