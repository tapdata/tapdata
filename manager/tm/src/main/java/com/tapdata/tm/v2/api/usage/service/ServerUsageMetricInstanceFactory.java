package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.field.ServerUsageField;
import com.tapdata.tm.worker.entity.field.ServerUsageMetricField;
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
        final String serverId = entity.get(ServerUsageField.PROCESS_ID.field(), String.class);
        final String workOid = entity.get(ServerUsageField.WORK_OID.field(), String.class);
        final String key = String.format("%s:%s", serverId, workOid);
        final ServerUsageMetricInstanceAcceptor acceptor = instanceMap.computeIfAbsent(key, k -> {
            final ServerUsageMetric lastMin = lastOne(serverId, 1, null);
            ServerUsageMetric lastHour = null;
            if (null != lastMin) {
                long bucketHour = TimeGranularity.HOUR.fixTime(lastMin.getLastUpdateTime());
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
        final Criteria criteria = Criteria.where(ServerUsageField.PROCESS_ID.field()).is(serverId)
                .and(ServerUsageMetricField.TIME_GRANULARITY.field()).is(type);
        if (null != timeStart) {
            criteria.and(ServerUsageField.LAST_UPDATE_TIME.field()).gte(timeStart);
        }
        final Query query = Query.query(criteria)
                .with(Sort.by(ServerUsageField.LAST_UPDATE_TIME.field()).descending())
                .limit(1);
        return findOne.apply(query);
    }
}