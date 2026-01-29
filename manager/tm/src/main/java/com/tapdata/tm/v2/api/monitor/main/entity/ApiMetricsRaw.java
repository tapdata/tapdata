package com.tapdata.tm.v2.api.monitor.main.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 10:41 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiMetricsRaw")
public class ApiMetricsRaw extends BaseEntity {
    private String processId;

    private String apiId;

    private String reqPath;

    /**
     * 0: each api * each server
     * 1: each api * all server
     * 2: each server * all api
     *
     * @see MetricTypes
     */
    private int metricType;

    /**
     * 0: 5S level
     * 1: 1-minute level
     * 2: 1-hour level
     * 3: day-level
     *
     * @see TimeGranularity
     */
    private int timeGranularity;

    /**
     * time：
     * When timeGranularity=0, it represents a 5S level timestamp
     * When timeGranularity=1, it represents a timestamp at the 1-minute level
     * When timeGranularity=2, it represents a timestamp at the 1-hour level
     * When timeGranularity=3, it represents a timestamp at the day level
     */
    private Long timeStart;

    private Long reqCount;

    private Long errorCount;

    private Double rps;

    /**
     * To save space, the content of this field is as follows: [{k: 102, v: 2}, {k: 89, v: 5}]
     * k represents the byte of a request, and v represents the corresponding number of occurrences
     */
    private List<Map<String, Number>> bytes;

    /**
     * To save space, the content of this field is as follows: [{k: 102, v: 2}, {k: 89, v: 5}]
     * k represents the delay of a request, and v represents the corresponding number of occurrences
     */
    private List<Map<String, Number>> delay;

    /**
     * To save space, the content of this field is as follows: [{k: 102, v: 2}, {k: 89, v: 5}]
     * k represents the query db cost time of a request, and v represents the corresponding number of occurrences
     */
    private List<Map<String, Number>> dbCost;

    /**
     * When timeGranularity=0, 1 minute of data is aggregated, with 5 seconds per data point and 12 records per minute
     */
    private Map<Long, ApiMetricsRaw> subMetrics;

    private Long p50;

    private Long p95;

    private Long p99;

    private Long maxDelay;

    private Long minDelay;

    private List<WorkerInfo> workerInfoMap;

    private ObjectId lastCallId;

    private Date ttlKey;

    public static ApiMetricsRaw instance(String serverId, String reqPath, String apiId, Long bucketMin, TimeGranularity type, MetricTypes metricType) {
        ApiMetricsRaw item = new ApiMetricsRaw();
        item.setId(new ObjectId());
        item.setTimeStart(bucketMin);
        item.setTimeGranularity(type.getType());
        item.setReqCount(0L);
        item.setErrorCount(0L);
        item.setRps(0d);
        item.setBytes(new ArrayList<>());
        item.setDelay(new ArrayList<>());
        item.setMetricType(metricType.getType());
        switch (metricType) {
            case API_SERVER:
                item.setProcessId(serverId);
                item.setApiId(apiId);
                item.setReqPath(reqPath);
                break;
            case API:
                item.setApiId(apiId);
                item.setReqPath(reqPath);
                break;
            case SERER:
                item.setProcessId(serverId);
                item.setWorkerInfoMap(new ArrayList<>());
                break;
            default:
                //do nothing
        }
        return item;
    }

    public void merge(ApiMetricsRaw raw) {
        setReqCount(Optional.ofNullable(getReqCount()).orElse(0L) + Optional.ofNullable(raw.getReqCount()).orElse(0L));
        setErrorCount(Optional.ofNullable(getErrorCount()).orElse(0L) + Optional.ofNullable(raw.getErrorCount()).orElse(0L));
        List<Map<String, Number>> mergeBytes = ApiMetricsDelayUtil.merge(Optional.ofNullable(getBytes())
                .orElse(new ArrayList<>()), Optional.ofNullable(raw.getBytes()).orElse(new ArrayList<>()));
        setBytes(mergeBytes);
        List<Map<String, Number>> mergeDelay = ApiMetricsDelayUtil.merge(Optional.ofNullable(getDelay())
                .orElse(new ArrayList<>()), Optional.ofNullable(raw.getDelay()).orElse(new ArrayList<>()));
        setDelay(mergeDelay);
        List<Map<String, Number>> mergeDbCost = ApiMetricsDelayUtil.merge(Optional.ofNullable(getDbCost())
                .orElse(new ArrayList<>()), Optional.ofNullable(raw.getDbCost()).orElse(new ArrayList<>()));
        setDbCost(mergeDbCost);
        calcRps();
        this.mergeWorkerInfo(raw.getWorkerInfoMap());
    }

    protected void mergeWorkerInfo(List<WorkerInfo> list) {
        if (null == list || list.isEmpty()) {
            return;
        }
        List<WorkerInfo> workerInfos = Optional.ofNullable(getWorkerInfoMap()).orElse(new ArrayList<>());
        Map<String, WorkerInfo> collected = workerInfos.stream()
                .collect(Collectors.toMap(WorkerInfo::getWorkerOid, e -> e, (e1, e2) -> e2));
        list.forEach(workerInfo -> {
            String workerOid = workerInfo.getWorkerOid();
            WorkerInfo info = collected.computeIfAbsent(workerOid, k -> new WorkerInfo());
            info.setWorkerOid(workerOid);
            info.setReqCount(ApiMetricsCompressValueUtil.sum(info.getReqCount(), workerInfo.getReqCount()));
            info.setErrorCount(ApiMetricsCompressValueUtil.sum(info.getErrorCount(), workerInfo.getErrorCount()));
        });
        setWorkerInfoMap(workerInfos);
    }

    public void merge(ApiMetricsRaw raw, long time, long timeGranularity) {
        List<Map<String, Number>> rawDelay = Optional.ofNullable(raw.getDelay()).orElse(new ArrayList<>());
        List<Map<String, Number>> rawBytes = Optional.ofNullable(raw.getBytes()).orElse(new ArrayList<>());
        List<Map<String, Number>> rawDbCost = Optional.ofNullable(raw.getDbCost()).orElse(new ArrayList<>());
        long rawReqCount = Optional.ofNullable(raw.getReqCount()).orElse(0L);
        long rawErrorCount = Optional.ofNullable(raw.getErrorCount()).orElse(0L);
        mergeWorkerInfo(raw.getWorkerInfoMap());
        if (timeGranularity == 0) {
            Map<Long, ApiMetricsRaw> subMetricsInfo = Optional.ofNullable(getSubMetrics()).orElse(new HashMap<>());
            ApiMetricsRaw sub = Optional.ofNullable(subMetricsInfo.get(time)).orElse(new ApiMetricsRaw());
            long reqCountInfo = Optional.ofNullable(sub.getReqCount()).orElse(0L);
            long errorCountInfo = Optional.ofNullable(sub.getErrorCount()).orElse(0L);
            List<Map<String, Number>> bytesInfo = Optional.ofNullable(sub.getBytes()).orElse(new ArrayList<>());
            List<Map<String, Number>> delayInfo = Optional.ofNullable(sub.getDelay()).orElse(new ArrayList<>());
            List<Map<String, Number>> dbCostInfo = Optional.ofNullable(sub.getDbCost()).orElse(new ArrayList<>());
            sub.setReqCount(reqCountInfo + rawReqCount);
            sub.setErrorCount(errorCountInfo + rawErrorCount);
            sub.setBytes(ApiMetricsDelayUtil.merge(bytesInfo, rawBytes));
            sub.setDelay(ApiMetricsDelayUtil.merge(delayInfo, rawDelay));
            sub.setDbCost(ApiMetricsDelayUtil.merge(dbCostInfo, rawDbCost));
            setSubMetrics(subMetricsInfo);
        } else {
            long reqCountInfo = Optional.ofNullable(getReqCount()).orElse(0L);
            long errorCountInfo = Optional.ofNullable(getErrorCount()).orElse(0L);
            List<Map<String, Number>> bytesInfo = Optional.ofNullable(getBytes()).orElse(new ArrayList<>());
            List<Map<String, Number>> delayInfo = Optional.ofNullable(getDelay()).orElse(new ArrayList<>());
            List<Map<String, Number>> dbCostInfo = Optional.ofNullable(getDbCost()).orElse(new ArrayList<>());
            setReqCount(reqCountInfo + rawReqCount);
            setErrorCount(errorCountInfo + rawErrorCount);
            setBytes(ApiMetricsDelayUtil.merge(bytesInfo, rawBytes));
            setDelay(ApiMetricsDelayUtil.merge(delayInfo, rawDelay));
            setDbCost(ApiMetricsDelayUtil.merge(dbCostInfo, rawDbCost));
        }
    }

    public void merge(boolean isOk, long reqBytes, long requestCost, long dbCost) {
        setReqCount(Optional.ofNullable(getReqCount()).orElse(0L) + 1L);
        setErrorCount(Optional.ofNullable(getErrorCount()).orElse(0L) + (isOk ? 0L : 1L));
        setBytes(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getBytes()).orElse(new ArrayList<>()), reqBytes));
        setDelay(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getDelay()).orElse(new ArrayList<>()), requestCost));
        setDbCost(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getDbCost()).orElse(new ArrayList<>()), dbCost));
        calcRps();
    }

    public void mergeWorker(String workerOid, boolean isOk, boolean needWorkerInfo) {
        if (!needWorkerInfo) {
            return;
        }
        List<WorkerInfo> workerInfos = Optional.ofNullable(getWorkerInfoMap()).orElse(new ArrayList<>());
        Map<String, WorkerInfo> collected = workerInfos.stream()
                .collect(Collectors.toMap(WorkerInfo::getWorkerOid, e -> e, (e1, e2) -> e2));
        WorkerInfo info = collected.computeIfAbsent(workerOid, k -> new WorkerInfo());
        info.setWorkerOid(workerOid);
        info.setReqCount(info.getReqCount() + 1L);
        info.setErrorCount(info.getErrorCount() + (isOk ? 0L : 1L));
        setWorkerInfoMap(new ArrayList<>(collected.values()));
    }

    void calcRps() {
        switch (timeGranularity) {
            case 1:
                setRps(Optional.ofNullable(getReqCount()).orElse(0L) / 60D);
                break;
            case 2:
                setRps(Optional.ofNullable(getReqCount()).orElse(0L) / 3600D);
                break;
            default:
                setRps(Optional.ofNullable(getReqCount()).orElse(0L) / 5D);
        }
    }
}