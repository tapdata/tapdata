package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.enums.TimeGranularityType;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.monitor.service.MetricInstanceFactory;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 17:15 Create
 * @description
 */
public class WorkerCallsInfoGenerator implements AutoCloseable {
    final Acceptor acceptor;
    private Map<Long, Map<String, WorkerCallEntity>> calls;
    public static final int BATCH_ACCEPT = 100;
    private Map<String, WorkerCallEntity> last;
    private Long lastKey;
    private final int batchSize;

    public WorkerCallsInfoGenerator(Acceptor acceptor) {
        this(acceptor, null);
    }

    public WorkerCallsInfoGenerator(Acceptor acceptor, Integer batchSize) {
        this.acceptor = acceptor;
        if (null == this.acceptor) {
            throw new IllegalArgumentException("Acceptor cannot be null");
        }
        this.calls = new HashMap<>();
        this.last = new HashMap<>();
        this.batchSize = Optional.ofNullable(batchSize).orElse(BATCH_ACCEPT);
    }

    void append(WorkerCallsInfo info) {
        try {
            this.map(info);
        } finally {
            if (calls != null && !calls.isEmpty() && batchSize >= calls.size()) {
                calls.remove(lastKey);
                accept();
                calls.put(lastKey, last);
            }
        }
    }

    void append(Document info) {
        String reqPath = info.getString("req_path");
        if (MetricInstanceFactory.IGNORE_PATH.contains(reqPath)) {
            return;
        }
        WorkerCallsInfo item = new WorkerCallsInfo();
        item.setWorkOid(info.getString("workOid"));
        item.setApiGatewayUuid(info.getString("api_gateway_uuid"));
        item.setApiId(info.getString("allPathId"));
        item.setLatency(info.getLong("latency"));
        item.setCode(info.getString("code"));
        item.setFailed(!info.getBoolean("succeed"));
        item.setHttpStatus(info.getString("httpStatus"));
        item.setReqTime(info.getLong("reqTime"));
        item.setResTime(info.getLong("resTime"));
        item.setReqPath(reqPath);
        item.setLastApiCallId(info.getObjectId("_id"));
        try {
            this.map(item);
        } finally {
            if (calls != null && !calls.isEmpty() && batchSize >= calls.size()) {
                calls.remove(lastKey);
                accept();
                calls.put(lastKey, last);
            }
        }
    }

    public void append(List<WorkerCallsInfo> infos) {
        if (null == infos || infos.isEmpty()) {
            return;
        }
        infos.stream().filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getReqPath()))
                .filter(e -> !MetricInstanceFactory.IGNORE_PATH.contains(e.getReqPath()))
                .forEach(this::append);
    }

    void map(WorkerCallsInfo info) {
        final Long reqTime = info.getReqTime();
        final String processId = info.getApiGatewayUuid();
        final String workOid = info.getWorkOid();
        final String apiId = info.getApiId();
        long latency = Optional.ofNullable(info.getLatency()).orElse(0L);
        if (latency < 0L) {
            latency = 0L;
        }
        final long key = (reqTime / 60000L) * 60000L;
        final Map<String, WorkerCallEntity> itemMap = calls.computeIfAbsent(key, k -> new HashMap<>());
        final WorkerCallEntity item = itemMap.computeIfAbsent(apiId, k -> new WorkerCallEntity());
        if (null == this.lastKey || this.lastKey != key) {
            this.last = itemMap;
            this.lastKey = key;
        } else {
            this.last.put(apiId, item);
        }
        List<Map<String, Number>> delays = item.getDelays();
        item.setDelays(delays);
        delays = ApiMetricsDelayUtil.addDelay(delays, latency);
        item.setErrorCount(Optional.ofNullable(item.getErrorCount()).orElse(0L));
        if (info.isFailed()) {
            item.setErrorCount(item.getErrorCount() + 1);
        }
        item.setReqCount(Optional.ofNullable(item.getReqCount()).orElse(0L) + 1);
        item.setProcessId(processId);
        item.setApiId(apiId);
        item.setId(Optional.ofNullable(item.getId()).orElse(new ObjectId()));
        item.setWorkOid(workOid);
        item.setTimeStart(key);
        item.setTimeGranularity(TimeGranularityType.MINUTE.getCode());
        item.setRps(item.getReqCount() / 60.0d);
        long total = Optional.ofNullable(item.getReqCount()).orElse(0L);
        long error = Optional.ofNullable(item.getErrorCount()).orElse(0L);
        item.setErrorRate(total == 0L || error == 0d ? 0d : (1.0d * error / total));
        item.setP50(ApiMetricsDelayUtil.p50(delays, total));
        item.setP95(ApiMetricsDelayUtil.p95(delays, total));
        item.setP99(ApiMetricsDelayUtil.p99(delays, total));
        item.setLastApiCallId(info.getLastApiCallId());
    }

    void accept() {
        final List<WorkerCallEntity> list = new ArrayList<>();
        calls.values().stream().map(Map::values).toList().forEach(list::addAll);
        if (!list.isEmpty()) {
            acceptor.accept(list);
        }
        calls = new HashMap<>();
    }

    @Override
    public void close() {
        if (null == calls || calls.isEmpty()) {
            return;
        }
        accept();
    }


    public interface Acceptor {
        void accept(List<WorkerCallEntity> info);
    }
}
