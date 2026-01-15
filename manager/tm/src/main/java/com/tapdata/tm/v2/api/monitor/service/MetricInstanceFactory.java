package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
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
    public static final List<String> IGNORE_PATH = List.of("/sitemap.xml","/openapi-readOnly.json","/robots.txt","/favicon.ico","//v4.loopback.io/favicon.ico");

    public MetricInstanceFactory(Consumer<List<ApiMetricsRaw>> consumer, Function<Query, ApiMetricsRaw> findOne) {
        super(consumer, findOne);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        final String reqPath = entity.get("req_path", String.class);
        if (null != reqPath && IGNORE_PATH.contains(reqPath)) {
            return;
        }
        if (!needUpdate()) {
            needUpdate(true);
        }
        final String allPathId = entity.get("allPathId", String.class);
        final String apiId = Optional.ofNullable(allPathId)
                .orElse(Optional.ofNullable(reqPath).orElse(MetricInstanceAcceptor.UN_KNOW));
        final String serverId = entity.get("api_gateway_uuid", String.class);
        final String key = String.format("%s:%s", apiId, serverId);
        final MetricInstanceAcceptor acceptor = instanceMap.computeIfAbsent(key, k -> {
            final ApiMetricsRaw lastMin = lastOne(apiId, serverId, 1, null);
            ApiMetricsRaw lastHour = null;
            if (null != lastMin) {
                long bucketHour = (lastMin.getTimeStart() / 60L) * 60L;
                lastHour = lastOne(apiId, serverId, 2, bucketHour);
            }
            return new MetricInstanceAcceptor(lastMin, lastHour, this.apiMetricsRaws::add);
        });
        acceptor.accept(entity);
        if (apiMetricsRaws.size() >= BATCH_SIZE) {
            flush();
        }
    }

    ApiMetricsRaw lastOne(String apiId, String serverId, int type, Long timeStart) {
        final Criteria criteria = Criteria.where("apiId").is(apiId)
                .and("processId").is(serverId)
                .and("timeGranularity").is(type);
        if (null != timeStart) {
            criteria.and("timeStart").is(timeStart);
        }
        final Query query = Query.query(criteria);
        query.with(Sort.by("_id").descending());
        query.limit(1);
        return findOne.apply(query);
    }
}