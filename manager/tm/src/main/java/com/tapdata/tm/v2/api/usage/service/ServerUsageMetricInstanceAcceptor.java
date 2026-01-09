package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.v2.api.common.service.AcceptorBase;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.Document;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 15:10 Create
 * @description
 */
public final class ServerUsageMetricInstanceAcceptor implements AcceptorBase {
    Consumer<ServerUsageMetric> consumer;

    ServerUsageMetric lastBucketMin;
    ServerUsageMetric lastBucketHour;

    public ServerUsageMetricInstanceAcceptor(ServerUsageMetric lastBucketMin, ServerUsageMetric lastBucketHour, Consumer<ServerUsageMetric> consumer) {
        this.lastBucketMin = lastBucketMin;
        this.lastBucketHour = lastBucketHour;
        this.consumer = consumer;
    }

    public void accept(Document entity) {
        if (null == entity) {
            return;
        }
        String serverId = entity.get("processId", String.class);
        Object workOidObj = entity.get("workOid");
        String workOid = workOidObj != null ? workOidObj.toString() : null;
        long lastUpdateTime = Optional.ofNullable(entity.get("lastUpdateTime", Long.class)).orElse(0L) / 1000L;
        long bucketMin = (lastUpdateTime / 60) * 60 * 1000L;
        long bucketHour = (lastUpdateTime / 3600) * 3600 * 1000L;
        if (null != lastBucketMin && lastBucketMin.getLastUpdateTime() != bucketMin) {
            acceptOnce(lastBucketMin);
            lastBucketMin = null;
        }
        if (null != lastBucketHour && lastBucketHour.getLastUpdateTime() != bucketHour) {
            acceptOnce(lastBucketHour);
            lastBucketMin = null;
        }

        ServerUsage.ProcessType processType = null == workOid ? ServerUsage.ProcessType.API_SERVER : ServerUsage.ProcessType.API_SERVER_WORKER;
        if (null == lastBucketMin || null == lastBucketHour) {
            if (null == lastBucketMin) {
                lastBucketMin = ServerUsageMetric.instance(1, bucketMin, serverId, workOid, processType.getType());
            }
            if (null == lastBucketHour) {
                lastBucketHour = ServerUsageMetric.instance(2, bucketHour, serverId, workOid, processType.getType());
            }
        }
        lastBucketMin.append(entity);
        lastBucketHour.append(entity);
    }

    void acceptOnce(ServerUsageMetric item) {
        if (null == item) {
            return;
        }
        consumer.accept(item);
    }

    @Override
    public void close() {
        acceptOnce(lastBucketMin);
        acceptOnce(lastBucketHour);
    }
}
