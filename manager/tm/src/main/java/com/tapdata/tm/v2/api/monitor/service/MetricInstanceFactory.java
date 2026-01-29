package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import lombok.Getter;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 14:02 Create
 * @description
 */
public final class MetricInstanceFactory extends FactoryBase<ApiMetricsRaw, MetricInstanceAcceptor> {
    public static final List<String> IGNORE_PATH = List.of(
            "/sitemap.xml",
            "/openapi-readOnly.json",
            "/robots.txt",
            "/favicon.ico",
            "//v4.loopback.io/favicon.ico",
            "/security.txt"
    );

    public MetricInstanceFactory(Consumer<List<ApiMetricsRaw>> consumer, Function<Query, ApiMetricsRaw> findOne) {
        super(consumer, findOne);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        CallInfo item = new CallInfo(entity);
        final String reqPath = item.getReqPath();
        if (null != reqPath && IGNORE_PATH.contains(reqPath)) {
            return;
        }
        if (!needUpdate()) {
            needUpdate(true);
        }
        final String apiId = item.getApiId();
        final String serverId = item.getServerId();
        final String keyOfApiServer = String.format("%s:%s:%s_0", apiId, reqPath, serverId);
        final String keyOfApi = String.format("%s:%s:*_1", reqPath, apiId);
        final String keyOfServer = String.format("*:%s_2", serverId);
        final String keyOfAll = "*:*_3";
        final MetricInstanceAcceptor acceptorOfApiServer = create(keyOfApiServer, MetricTypes.API_SERVER, reqPath, serverId);
        final MetricInstanceAcceptor acceptorOfApi = create(keyOfApi, MetricTypes.API, reqPath, null);
        final MetricInstanceAcceptor acceptorOfServer = create(keyOfServer, MetricTypes.SERER, null, serverId)
                .beSaveWorkerInfo();
        final MetricInstanceAcceptor acceptorOfAll = create(keyOfAll, MetricTypes.ALL, null, null);
        acceptorOfApiServer.accept(item);
        acceptorOfApi.accept(item);
        acceptorOfServer.accept(item);
        acceptorOfAll.accept(item);
        if (apiMetricsRaws.size() >= BATCH_SIZE) {
            flush();
        }
    }

    MetricInstanceAcceptor create(String key, MetricTypes metricType, String reqPath, String serverId) {
        return instanceMap.computeIfAbsent(key, k -> new MetricInstanceAcceptor(metricType, ts -> {
                    final ApiMetricsRaw lastMin = lastOne(reqPath, serverId, metricType, TimeGranularity.MINUTE, ts);
                    ApiMetricsRaw lastHour = null;
                    ApiMetricsRaw lastDay = null;
                    if (null != lastMin) {
                        long bucketHour = TimeGranularity.HOUR.fixTime(lastMin.getTimeStart());
                        lastHour = lastOne(reqPath, serverId, metricType, TimeGranularity.HOUR, bucketHour);
                        long bucketDay = TimeGranularity.DAY.fixTime(lastMin.getTimeStart());
                        lastDay = lastOne(reqPath, serverId, metricType, TimeGranularity.DAY, bucketDay);
                    }
                    return new MetricInstanceAcceptor.BucketInfo(lastMin, lastHour, lastDay);
                }, (quickUpload, info) -> {
                    if (null != quickUpload && !quickUpload) {
                        this.apiMetricsRaws.add(info);
                    } else {
                        this.consumer.accept(List.of(info));
                    }
                    return null;
                })
        );
    }

    ApiMetricsRaw lastOne(String reqPath, String serverId, MetricTypes metricType, TimeGranularity timeGranularity, Long timeStart) {
        final Criteria criteria = Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(metricType.getType())
                .and(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(timeGranularity.getType());
        switch (metricType) {
            case API_SERVER:
                criteria.and(ApiMetricsRawFields.REQ_PATH.field()).is(reqPath)
                        .and(ApiMetricsRawFields.PROCESS_ID.field()).is(serverId);
                break;
            case API:
                criteria.and(ApiMetricsRawFields.REQ_PATH.field()).is(reqPath);
                break;
            case SERER:
                criteria.and(ApiMetricsRawFields.PROCESS_ID.field()).is(serverId);
                break;
            default:
                //do nothing
        }
        if (null != timeStart) {
            criteria.and(ApiMetricsRawFields.TIME_START.field()).is(timeStart);
        }
        final Query query = Query.query(criteria);
        query.with(Sort.by(ApiMetricsRawFields.TIME_START.field()).descending());
        query.limit(1);
        return findOne.apply(query);
    }

    @Getter
    public static class CallInfo {
        static final String UN_KNOW = "UN_KNOWN";
        String allPathId;
        String reqPath;
        String apiId;
        String serverId;
        long requestCost;
        long dbCost;
        boolean isOk;
        long reqBytes;
        boolean supplement;
        String workerId;
        Long apiCallReqTime;
        long reqTimeOSec;
        long bucketSec;
        long bucketMin;
        long bucketHour;
        long bucketDay;
        ObjectId callId;

        public CallInfo(Document entity) {
            this.allPathId = entity.get(ApiCallField.ALL_PATH_ID.field(), String.class);
            this.reqPath = entity.get(ApiCallField.REQ_PATH.field(), String.class);
            this.apiId = Optional.ofNullable(allPathId)
                    .orElse(Optional.ofNullable(reqPath)
                            .orElse(UN_KNOW));
            this.serverId = entity.get(ApiCallField.API_GATEWAY_UUID.field(), String.class);
            this.requestCost = value(entity.get(ApiCallField.LATENCY.field(), Long.class));
            this.dbCost = value(entity.get(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), Long.class));
            this.isOk = entity.get(ApiCallField.SUCCEED.field(), Boolean.class);
            this.reqBytes = value(entity.get(ApiCallField.REQ_BYTES.field(), Long.class));
            this.supplement = Optional.ofNullable(entity.get(ApiCallField.SUPPLEMENT.field(), Boolean.class)).orElse(false);
            this.workerId = entity.get(ApiCallField.WORK_O_ID.field(), String.class);
            this.apiCallReqTime = entity.get(ApiCallField.REQ_TIME.field(), Long.class);
            this.reqTimeOSec = Optional.ofNullable(apiCallReqTime).orElse(0L) / 1000L;
            this.bucketSec = TimeGranularity.SECOND_FIVE.fixTime(reqTimeOSec);
            this.bucketMin = TimeGranularity.MINUTE.fixTime(reqTimeOSec);
            this.bucketHour = TimeGranularity.HOUR.fixTime(reqTimeOSec);
            this.bucketDay = TimeGranularity.DAY.fixTime(reqTimeOSec);
            this.callId = entity.get(BaseEntityFields._ID.field(), ObjectId.class);
        }

        long value(Long val) {
            if (null == val) {
                return 0L;
            }
            return Math.max(0L, val);
        }
    }

}