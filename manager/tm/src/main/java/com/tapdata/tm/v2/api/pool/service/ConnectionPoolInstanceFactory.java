package com.tapdata.tm.v2.api.pool.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.common.service.FactoryBase;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
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
 * @version v1.0 2026/4/29 14:50 Create
 * @description
 */
public class ConnectionPoolInstanceFactory extends FactoryBase<ConnectionPoolEntity, ConnectionPoolInstanceAcceptor> {
    protected ConnectionPoolInstanceFactory(Consumer<List<ConnectionPoolEntity>> consumer, Function<Query, ConnectionPoolEntity> findOne) {
        super(consumer, findOne);
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        if (!needUpdate()) {
            needUpdate(true);
        }
        final String serverId = entity.get(ConnectionPoolField.PROCESS_ID.field(), String.class);
        final String connectionId = entity.get(ConnectionPoolField.CONNECTION_ID.field(), String.class);
        final String key = String.format("%s:%s", serverId, connectionId);
        final ConnectionPoolInstanceAcceptor acceptor = instanceMap.computeIfAbsent(key, k -> {
            final ConnectionPoolEntity lastMin = lastOne(serverId, connectionId, TimeGranularity.MINUTE.getType(), null);
            ConnectionPoolEntity lastHour = null;
            if (null != lastMin) {
                long bucketHour = TimeGranularity.HOUR.fixTime(lastMin.getLastUpdateTime());
                lastHour = lastOne(serverId, connectionId, TimeGranularity.HOUR.getType(), bucketHour);
            }
            return new ConnectionPoolInstanceAcceptor(lastMin, lastHour, this.apiMetricsRaws::add);
        });
        acceptor.accept(entity);
        if (apiMetricsRaws.size() >= BATCH_SIZE) {
            flush();
        }
    }

    ConnectionPoolEntity lastOne(String serverId, String connectionId, int type, Long timeStart) {
        final Criteria criteria = Criteria.where(ConnectionPoolField.PROCESS_ID.field()).is(serverId)
                .and(ConnectionPoolField.CONNECTION_ID.field()).is(connectionId)
                .and(ConnectionPoolField.TIME_GRANULARITY.field()).is(type);
        if (null != timeStart) {
            criteria.and(ConnectionPoolField.LAST_UPDATE_TIME.field()).gte(timeStart);
        }
        final Query query = Query.query(criteria)
                .with(Sort.by(ConnectionPoolField.LAST_UPDATE_TIME.field()).descending())
                .limit(1);
        return findOne.apply(query);
    }
}
