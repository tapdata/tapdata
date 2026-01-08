package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 14:02 Create
 * @description
 */
public final class ServerUsageMetricInstanceFactory extends FactoryBase<ServerUsageMetric, ServerUsageMetricInstanceAcceptor> {

    public ServerUsageMetricInstanceFactory(Consumer<List<ServerUsageMetric>> consumer, Function<Query, ServerUsageMetric> findOne) {
        super(consumer, findOne);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        if (!needUpdate()) {
            needUpdate(true);
        }
        final String serverId = entity.get("processId", String.class);
        final String workOid = entity.get("workOid", String.class);
        final String key = String.format("%s:%s", serverId, workOid);
        final ServerUsageMetricInstanceAcceptor acceptor = instanceMap.computeIfAbsent(key, k -> {
            final ServerUsageMetric lastMin = lastOne(serverId, 1, null);
            ServerUsageMetric lastHour = null;
            if (null != lastMin) {
                long bucketHour = (lastMin.getLastUpdateTime() / 60) * 60;
                lastHour = lastOne(serverId, 2, bucketHour);
            }
            return new ServerUsageMetricInstanceAcceptor(lastMin, lastHour, this.apiMetricsRaws::add);
        });
        acceptor.accept(entity);
        if (apiMetricsRaws.size() >= BATCH_SIZE) {
            flush();
        }
    }

    ServerUsageMetric lastOne(String serverId, int type, Long timeStart) {
        final Criteria criteria = Criteria.where("processId").is(serverId)
                .and("timeGranularity").is(type);
        if (null != timeStart) {
            criteria.and("lastUpdateTime").is(timeStart);
        }
        final Query query = Query.query(criteria)
                .with(Sort.by("lastUpdateTime").descending())
                .limit(1);
        return findOne.apply(query);
    }
}