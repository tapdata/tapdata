package com.tapdata.tm.v2.api.monitor.main.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
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

    /**
     * 0: each api * each server
     * 1: each api * all server
     * 2: each server * all api
     *
     * @see MetricTypes
     */
    private int metricType;

    /**
     * 0: 5S级别
     * 1: 1分钟级别
     * 2: 1小时级别
     * 3: 天级别
     *
     * @see TimeGranularity
     */
    private int timeGranularity;

    /**
     * 时间：
     * timeGranularity = 0 时，表示5S级别的时间戳
     * timeGranularity = 1 时，表示1分钟级别的时间戳
     * timeGranularity = 2 时，表示1小时级别的时间戳
     * timeGranularity = 3 时，表示天级别的时间戳
     */
    private Long timeStart;

    private Long reqCount;

    private Long errorCount;

    private Double rps;

    /**
     * 为了节省空间, 此字段内容如下：[100, {102:2},{89:5}]
     * 数组元素为纯数子：表示某次请求的延时
     * 数组元素为对象：表示某次请求的延时以及出现的次数
     */
    private List<?> bytes;

    /**
     * 为了节省空间, 此字段内容如下：[100, {102:2},{89:5}]
     * 数组元素为纯数子：表示某次请求的延时
     * 数组元素为对象：表示某次请求的延时以及出现的次数
     */
    private List<?> delay;

    /**
     * 为了节省空间, 此字段内容如下：[100, {102:2},{89:5}]
     * 数组元素为纯数子：表示某次请求的延时
     * 数组元素为对象：表示某次请求的延时以及出现的次数
     */
    private List<?> dbCost;

    /**
     * timeGranularity = 0 时，聚合了1分钟的数据，5S一个数据点，每分钟12条
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

    @Data
    public static class WorkerInfo {
        String workerOid;
        Long reqCount;
        Long errorCount;

        public WorkerInfo() {
            this.reqCount = 0L;
            this.errorCount = 0L;
        }
    }

    public static ApiMetricsRaw instance(String serverId, String apiId, Long bucketMin, TimeGranularity type, MetricTypes metricType) {
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
                break;
            case API:
                item.setApiId(apiId);
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
        List<Map<Long, Integer>> mergeBytes = ApiMetricsDelayUtil.merge(Optional.ofNullable(getBytes()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()), Optional.ofNullable(raw.getBytes()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()));
        setBytes(mergeBytes);
        List<Map<Long, Integer>> mergeDelay = ApiMetricsDelayUtil.merge(Optional.ofNullable(getDelay()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()), Optional.ofNullable(raw.getDelay()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()));
        setDelay(mergeDelay);
        List<Map<Long, Integer>> mergeDbCost = ApiMetricsDelayUtil.merge(Optional.ofNullable(getDbCost()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()), Optional.ofNullable(raw.getDbCost()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>()));
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
            info.setReqCount(ApiMetricsDelayInfoUtil.sum(info.getReqCount(), workerInfo.getReqCount()));
            info.setErrorCount(ApiMetricsDelayInfoUtil.sum(info.getErrorCount(), workerInfo.getErrorCount()));
        });
        setWorkerInfoMap(workerInfos);
    }

    public void merge(ApiMetricsRaw raw, long time, long timeGranularity) {
        List<Map<Long, Integer>> rawDelay = Optional.ofNullable(raw.getDelay()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
        List<Map<Long, Integer>> rawBytes = Optional.ofNullable(raw.getBytes()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
        List<Map<Long, Integer>> rawDbCost = Optional.ofNullable(raw.getDbCost()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
        long rawReqCount = Optional.ofNullable(raw.getReqCount()).orElse(0L);
        long rawErrorCount = Optional.ofNullable(raw.getErrorCount()).orElse(0L);
        mergeWorkerInfo(raw.getWorkerInfoMap());
        if (timeGranularity == 0) {
            Map<Long, ApiMetricsRaw> subMetrics = Optional.ofNullable(getSubMetrics()).orElse(new HashMap<>());
            ApiMetricsRaw sub = Optional.ofNullable(subMetrics.get(time)).orElse(new ApiMetricsRaw());
            long reqCount = Optional.ofNullable(sub.getReqCount()).orElse(0L);
            long errorCount = Optional.ofNullable(sub.getErrorCount()).orElse(0L);
            List<Map<Long, Integer>> bytes = Optional.ofNullable(sub.getBytes()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            List<Map<Long, Integer>> delay = Optional.ofNullable(sub.getDelay()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            List<Map<Long, Integer>> dbCost = Optional.ofNullable(sub.getDbCost()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            sub.setReqCount(reqCount + rawReqCount);
            sub.setErrorCount(errorCount + rawErrorCount);
            sub.setBytes(ApiMetricsDelayUtil.merge(bytes, rawBytes));
            sub.setDelay(ApiMetricsDelayUtil.merge(delay, rawDelay));
            sub.setDbCost(ApiMetricsDelayUtil.merge(dbCost, rawDbCost));
            setSubMetrics(subMetrics);
        } else {
            long reqCount = Optional.ofNullable(getReqCount()).orElse(0L);
            long errorCount = Optional.ofNullable(getErrorCount()).orElse(0L);
            List<Map<Long, Integer>> bytes = Optional.ofNullable(getBytes()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            List<Map<Long, Integer>> delay = Optional.ofNullable(getDelay()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            List<Map<Long, Integer>> dbCost = Optional.ofNullable(getDbCost()).map(ApiMetricsDelayUtil::fixDelayAsMap).orElse(new ArrayList<>());
            setReqCount(reqCount + rawReqCount);
            setErrorCount(errorCount + rawErrorCount);
            setBytes(ApiMetricsDelayUtil.merge(bytes, rawBytes));
            setDelay(ApiMetricsDelayUtil.merge(delay, rawDelay));
            setDbCost(ApiMetricsDelayUtil.merge(dbCost, rawDbCost));
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