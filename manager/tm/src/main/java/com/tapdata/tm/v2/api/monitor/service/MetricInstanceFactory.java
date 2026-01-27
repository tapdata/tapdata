package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import org.bson.Document;
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
            "//v4.loopback.io/favicon.ico"
    );

    public MetricInstanceFactory(Consumer<List<ApiMetricsRaw>> consumer, Function<Query, ApiMetricsRaw> findOne) {
        super(consumer, findOne);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        final String reqPath = entity.get(ApiCallField.REQ_PATH.field(), String.class);
        if (null != reqPath && IGNORE_PATH.contains(reqPath)) {
            return;
        }
        if (!needUpdate()) {
            needUpdate(true);
        }
        final String allPathId = entity.get(ApiCallField.ALL_PATH_ID.field(), String.class);
        final String apiId = Optional.ofNullable(allPathId)
                .orElse(Optional.ofNullable(reqPath).orElse(MetricInstanceAcceptor.UN_KNOW));
        final String serverId = entity.get(ApiCallField.API_GATEWAY_UUID.field(), String.class);
        final String keyOfApiServer = String.format("%s:%s_0", apiId, serverId);
        final String keyOfApi = String.format("%s:*_1", apiId);
        final String keyOfServer = String.format("*:%s_2", serverId);
        final String keyOfAll = "*:*_3";
        final MetricInstanceAcceptor acceptorOfApiServer = create(keyOfApiServer, MetricTypes.API_SERVER, apiId, serverId);
        final MetricInstanceAcceptor acceptorOfApi = create(keyOfApi, MetricTypes.API, apiId, null);
        final MetricInstanceAcceptor acceptorOfServer = create(keyOfServer, MetricTypes.SERER, null, serverId)
                .beSaveWorkerInfo();
        final MetricInstanceAcceptor acceptorOfAll = create(keyOfAll, MetricTypes.ALL, null, null);
        acceptorOfApiServer.accept(entity);
        acceptorOfApi.accept(entity);
        acceptorOfServer.accept(entity);
        acceptorOfAll.accept(entity);
        if (apiMetricsRaws.size() >= BATCH_SIZE) {
            flush();
        }
    }

    MetricInstanceAcceptor create(String key, MetricTypes metricType, String apiId, String serverId) {
        return instanceMap.computeIfAbsent(key, k -> new MetricInstanceAcceptor(metricType, ts -> {
                    final ApiMetricsRaw lastMin = lastOne(apiId, serverId, metricType, TimeGranularity.MINUTE, ts);
                    ApiMetricsRaw lastHour = null;
                    ApiMetricsRaw lastDay = null;
                    if (null != lastMin) {
                        long bucketHour = TimeGranularity.HOUR.fixTime(lastMin.getTimeStart());
                        lastHour = lastOne(apiId, serverId, metricType, TimeGranularity.HOUR, bucketHour);
                        long bucketDay = TimeGranularity.DAY.fixTime(lastMin.getTimeStart());
                        lastDay = lastOne(apiId, serverId, metricType, TimeGranularity.DAY, bucketDay);
                    }
                    return new MetricInstanceAcceptor.BucketInfo(lastMin, lastHour, lastDay);
                }, (quickUpload, info) -> {
                    if (!quickUpload) {
                        this.apiMetricsRaws.add(info);
                    } else {
                        this.consumer.accept(List.of(info));
                    }
                    return null;
                })
        );
    }


    ApiMetricsRaw lastOne(String apiId, String serverId, MetricTypes metricType, TimeGranularity timeGranularity, Long timeStart) {
        final Criteria criteria = Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(metricType.getType())
                .and(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(timeGranularity.getType());
        switch (metricType) {
            case API_SERVER:
                criteria.and(ApiMetricsRawFields.API_ID.field()).is(apiId)
                        .and(ApiMetricsRawFields.PROCESS_ID.field()).is(serverId);
                break;
            case API:
                criteria.and(ApiMetricsRawFields.API_ID.field()).is(apiId);
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
}