package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.entity.WorkerInfo;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsCompressValueUtil;
import com.tapdata.tm.v2.api.monitor.utils.TimeRangeUtil;
import org.bson.types.ObjectId;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:41 Create
 * @description
 */
public class ParticleSizeAnalyzer {
    private ParticleSizeAnalyzer() {

    }

    public static List<ApiMetricsRaw> apiMetricsRaws(List<ApiMetricsRaw> apiMetricsRaws, QueryBase query) {
        if (query.getGranularity() == TimeGranularity.SECOND_FIVE) {
            List<ApiMetricsRaw> sub = new ArrayList<>();
            for (ApiMetricsRaw raw : apiMetricsRaws) {
                Map<Long, ApiMetricsRaw> subMetrics = raw.getSubMetrics();
                if (!CollectionUtils.isEmpty(subMetrics)) {
                    subMetrics.values()
                            .stream()
                            .filter(e -> e.getTimeStart() >= query.getFixStart() && e.getTimeStart() < query.getFixEnd())
                            .sorted(Comparator.comparing(ApiMetricsRaw::getTimeStart))
                            .forEach(e -> {
                                e.setApiId(raw.getApiId());
                                e.setProcessId(raw.getProcessId());
                                sub.add(e);
                            });
                }
            }
            return sub;
        }
        return apiMetricsRaws;
    }

    public static List<ApiMetricsRaw> secondFiveMetricsRaws(List<ApiMetricsRaw> apiMetricsRaws, Predicate<ApiMetricsRaw> filter) {
        List<ApiMetricsRaw> sub = new ArrayList<>();
        for (ApiMetricsRaw raw : apiMetricsRaws) {
            Map<Long, ApiMetricsRaw> subMetrics = raw.getSubMetrics();
            if (!CollectionUtils.isEmpty(subMetrics)) {
                subMetrics.values()
                        .stream()
                        .filter(filter)
                        .sorted(Comparator.comparing(ApiMetricsRaw::getTimeStart))
                        .forEach(e -> {
                            e.setReqPath(raw.getReqPath());
                            e.setApiId(raw.getApiId());
                            e.setProcessId(raw.getProcessId());
                            sub.add(e);
                        });
            }
        }
        return sub;
    }

    public static Set<Long> asMinute(List<TimeRange> rangesOfSecondFive) {
        Set<Long> minutes = new HashSet<>();
        for (TimeRange point : rangesOfSecondFive) {
            long s = point.getStart();
            while (s < point.getEnd()) {
                minutes.add(TimeGranularity.MINUTE.fixTime(s));
                s += TimeGranularity.SECOND_FIVE.getSeconds();
            }
        }
        return minutes;
    }

    public static void checkQueryTime(QueryBase query) {
        long now = System.currentTimeMillis() / 1000L - 60L;
        QueryBase.TimeType type = QueryBase.TimeType.parse(query.getType());
        if (Objects.nonNull(query.getStartAt()) && Objects.nonNull(query.getEndAt())) {
            long range = query.getEndAt() - query.getStartAt();
            if (range > TimeRangeUtil.MAX_QUERY_RANGE) {
                throw new BizException("query.range.too.large", TimeRangeUtil.MAX_QUERY_RANGE_DAY);
            }
        }
        long step = Optional.ofNullable(query.getStep()).orElse(type.getDefaultStep());
        if (type != QueryBase.TimeType.RANGE) {
            query.setStartAt(now - type.getDefaultSecond() * step);
            query.setEndAt(now);
        }
        Long end = query.getEndAt();
        if (null == end) {
            end = now;
            query.setEndAt(end);
        }
        Long start = query.getStartAt();
        if (null == start) {
            start = end - type.getDefaultSecond() * type.getDefaultStep();
            query.setStartAt(start);
        }
        if (end > now) {
            end = now;
            query.setEndAt(end);
        }
        if (start >= end) {
            start = end - type.getDefaultSecond() * type.getDefaultStep();
            query.setStartAt(start);
        }
    }

    public static void parseToMetric(List<ApiCallEntity> entity, Consumer<ApiMetricsRaw> consumer) {
        for (ApiCallEntity apiCallEntity : entity) {
            String reqPath = Optional.ofNullable(apiCallEntity.getReq_path()).orElse("");
            if (MetricInstanceFactory.IGNORE_PATH.contains(reqPath)) {
                continue;
            }
            ApiMetricsRaw row = new ApiMetricsRaw();
            row.setProcessId(apiCallEntity.getApi_gateway_uuid());
            row.setApiId(apiCallEntity.getAllPathId());
            long errorCount = apiCallEntity.isSucceed() ? 0L : 1L;
            Optional.ofNullable(apiCallEntity.getWorkOid()).ifPresent(oId -> {
                List<WorkerInfo> workerInfos = new ArrayList<>();
                WorkerInfo info = new WorkerInfo();
                info.setWorkerOid(oId);
                info.setReqCount(1L);
                info.setErrorCount(errorCount);
                workerInfos.add(info);
                row.setWorkerInfoMap(workerInfos);
            });
            row.setTimeGranularity(-1);
            row.setTimeStart(Optional.ofNullable(apiCallEntity.getReqTime()).map(t -> t / 1000L).orElse(null));
            row.setReqCount(1L);
            row.setErrorCount(errorCount);
            row.setRps(1D / 60D);
            row.setBytes(ApiMetricsCompressValueUtil.asInit(apiCallEntity.getReqBytes()));
            row.setDelay(ApiMetricsCompressValueUtil.asInit(apiCallEntity.getLatency()));
            row.setSubMetrics(new HashMap<>());
            row.setId(new ObjectId());
            consumer.accept(row);
        }
    }
}
