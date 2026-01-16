package com.tapdata.tm.v2.api.monitor.main.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
     * 0: 5S级别
     * 1: 1分钟级别
     * 2: 1小时级别
     */
    private int timeGranularity;

    /**
     * 时间：
     * timeGranularity = 0 时，表示5S级别的时间戳
     * timeGranularity = 1 时，表示1分钟级别的时间戳
     * timeGranularity = 2 时，表示1小时级别的时间戳
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

    private ObjectId callId;

    public static ApiMetricsRaw instance(String serverId, String apiId, Long bucketMin, int type) {
        ApiMetricsRaw item = new ApiMetricsRaw();
        item.setId(new ObjectId());
        item.setTimeStart(bucketMin);
        item.setProcessId(serverId);
        item.setApiId(apiId);
        item.setTimeGranularity(type);
        item.setReqCount(0L);
        item.setErrorCount(0L);
        item.setRps(0d);
        item.setBytes(new ArrayList<>());
        item.setDelay(new ArrayList<>());
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
    }

    public void merge(boolean isOk, long reqBytes, long requestCost, long dbCost) {
        setReqCount(Optional.ofNullable(getReqCount()).orElse(0L) + 1L);
        setErrorCount(Optional.ofNullable(getErrorCount()).orElse(0L) + (isOk ? 0L : 1L));
        setBytes(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getBytes()).orElse(new ArrayList<>()), reqBytes));
        setDelay(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getDelay()).orElse(new ArrayList<>()), requestCost));
        setDbCost(ApiMetricsDelayUtil.addDelay(Optional.ofNullable(getDbCost()).orElse(new ArrayList<>()), dbCost));
        calcRps();
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