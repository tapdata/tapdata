package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.utils.PercentileCalculator;
import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
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
    private Map<Long, WorkerCallEntity> calls;
    public static final int BATCH_ACCEPT = 100;
    private WorkerCallEntity last;
    private Long lastKey;

    public WorkerCallsInfoGenerator(Acceptor acceptor) {
        this.acceptor = acceptor;
        if (null == this.acceptor) {
            throw new IllegalArgumentException("Acceptor cannot be null");
        }
        this.calls = new HashMap<>();
    }

    void append(WorkerCallsInfo info) {
        try {
            this.map(info);
        } finally {
            if (calls != null && !calls.isEmpty() && BATCH_ACCEPT >= calls.size()) {
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
        infos.stream().filter(Objects::nonNull).forEach(this::append);
    }

    void map(WorkerCallsInfo info) {
        final Long reqTime = info.getReqTime();
        final String processId = info.getApiGatewayUuid();
        final String workOid = info.getWorkOid();
        final Long latency = info.getLatency();
        final int code = Integer.parseInt(info.getCode());
        final long key = (reqTime / 60000) * 60000;
        final WorkerCallEntity item = calls.computeIfAbsent(key, k -> new WorkerCallEntity());
        List<Long> delays = Optional.ofNullable(item.getDelays()).orElse(new ArrayList<>());
        delays.add(latency);
        item.setDelays(delays);
        item.setErrorCount(Optional.ofNullable(item.getErrorCount()).orElse(0L));
        if (!(code >= 200 && code < 300)) {
            item.setErrorCount(item.getErrorCount() + 1);
        }
        item.setReqCount(Optional.ofNullable(item.getReqCount()).orElse(0L) + 1);
        item.setProcessId(processId);
        item.setId(Optional.ofNullable(item.getId()).orElse(new ObjectId()));
        item.setWorkOid(workOid);
        item.setTimeStart(key);
        item.setTimeGranularity(1);
        item.setDelete(false);
        item.setRps(item.getReqCount() / 60.0d);
        item.setErrorRate(item.getErrorCount() / item.getReqCount() * 1.0D);
        item.setP50(PercentileCalculator.calculatePercentile(delays, 0.5));
        item.setP95(PercentileCalculator.calculatePercentile(delays, 0.95));
        item.setP99(PercentileCalculator.calculatePercentile(delays, 0.99));
        this.last = item;
        this.lastKey = key;
    }

    void accept() {
        acceptor.accept(new ArrayList<>(calls.values()));
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
