package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:41 Create
 * @description
 */
public class ParticleSizeAnalyzer {
    private ParticleSizeAnalyzer() {

    }

    public static Criteria of(ValueBase valueBase, QueryBase query) {
        Criteria criteria = of(query);
        valueBase.setQueryFrom(query.getStartAt());
        valueBase.setQueryEnd(query.getEndAt());
        valueBase.setGranularity(query.getGranularity());
        return criteria;
    }

    public static Criteria of(QueryBase query) {
        checkQueryTime(query);
        long start = query.getStartAt();
        long end = query.getEndAt();
        long range = end - start;

        long qStart = start;
        if (range <= 60L * 60L) {
            query.setGranularity(0);
        } else if (range <= 60L * 60L * 24L) {
            query.setGranularity(1);
        } else {
            query.setGranularity(2);
        }
        long s;
        long e;
        long qs;
        long qe;
        int qg = 1;
        if (query.getGranularity() == 0) {
            s = (start + 4L) / 5L * 5L;
            qs = s / 60L * 60L - 5 * 60L;
            qStart = s - 5 * 60L;
            e = end / 5L * 5L;
            qe = (e + 59L) / 60L * 60L;
        } else if (query.getGranularity() == 1) {
            query.setGranularity(1);
            s = (start + 59L) / 60L * 60L;
            qStart = qs = ((qStart + 59L) / 60L * 60L);
            e = qe = ((end + 59L) / 60L * 60L);
        } else {
            query.setGranularity(2);
            s = (start + 3599L) / 3600L * 3600L;
            qStart = qs = ((qStart + 3599L) / 3600L * 3600L);
            e = qe = ((end + 3599L) / 3600L * 3600L);
            qg = 2;
        }
        query.setQStart(qStart);
        query.getQueryParam().setStart(start);
        query.getQueryParam().setEnd(end);
        query.setStartAt(s);
        query.setEndAt(e);
        if (query.getGranularity() != 0) {
            points(qStart, end, query);
        }
        return Criteria.where("timeGranularity").is(qg).andOperator(
                Criteria.where("timeStart").gte(qs),
                Criteria.where("timeStart").lt(qe)
        );
    }

    static void points(long startOrigin, long endOrigin, QueryBase query) {
        Long startAt = query.getStartAt();
        Long endAt = query.getEndAt();
        long right = ofPoint(startOrigin, startAt, -1, 2, e -> query.getQueryParam().getHourPoint().add(e));
        right = ofPoint(startOrigin, right, -1, 1, e -> query.getQueryParam().getMinutePoint().add(e));
        right = ofPoint(startOrigin, right, -1, 0, e -> query.getQueryParam().getSecond5Point().add(e));
        ofPoint(startOrigin, right, -1, -1, e -> query.getQueryParam().getSecondPoint().add(e));

        long letf = ofPoint(endAt, endOrigin, 1, 2, e -> query.getQueryParam().getHourPoint().add(e));
        letf = ofPoint(letf, endOrigin, 1, 1, e -> query.getQueryParam().getMinutePoint().add(e));
        letf = ofPoint(letf, endOrigin, 1, 0, e -> query.getQueryParam().getSecond5Point().add(e));
        ofPoint(letf, endOrigin, 1, -1, e -> query.getQueryParam().getSecondPoint().add(e));
    }

    static long ofPoint(long left, long right, int direction, int granularity, Consumer<QueryBase.Point> consumer) {
        long step = switch (granularity) {
            case -1 -> 1L;
            case 0 -> 5L;
            case 1 -> 60L;
            default -> 3600L;
        };
        if (left <= 0L || right <= 0L) {
            return -1L;
        }
        long range = right - left;
        if (range <= 0) {
            return -1L;
        }
        long ofCount = range / step;
        if (ofCount > 0L) {
            QueryBase.Point point;
            if (direction > 0) {
                long start = left;
                left = left + ofCount * step;
                point = QueryBase.Point.of(start, left, granularity);
            } else {
                long end = right;
                right = right - ofCount * step;
                point = QueryBase.Point.of(right, end, granularity);
            }
            consumer.accept(point);
        }
        return direction > 0 ? left : right;
    }

    public static List<ApiMetricsRaw> apiMetricsRaws(List<ApiMetricsRaw> apiMetricsRaws, QueryBase query) {
        if (query.getGranularity() == 0) {
            List<ApiMetricsRaw> sub = new ArrayList<>();
            for (ApiMetricsRaw raw : apiMetricsRaws) {
                Map<Long, ApiMetricsRaw> subMetrics = raw.getSubMetrics();
                if (!CollectionUtils.isEmpty(subMetrics)) {
                    subMetrics.values()
                            .stream()
                            .filter(e -> e.getTimeStart() >= query.getQStart() && e.getTimeStart() < query.getEndAt())
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

    public static void checkQueryTime(QueryBase query) {
        if (Objects.nonNull(query.getStartAt()) && Objects.nonNull(query.getEndAt())) {
            long range = query.getEndAt() - query.getStartAt();
            if (range > 31L * 24L * 60L * 60L) {
                throw new BizException("query.range.too.large", 31);
            }
        }
        Long end = query.getEndAt();
        if (null == end) {
            end = System.currentTimeMillis() / 1000L;
            query.setEndAt(end);
        }
        Long start = query.getStartAt();
        if (null == start) {
            start = end - 5L * 60L;
            query.setStartAt(start);
        }
    }

    public static void parseToMetric(List<ApiCallEntity> entity, Consumer<ApiMetricsRaw> consumer) {
        for (ApiCallEntity apiCallEntity : entity) {
            ApiMetricsRaw row = new ApiMetricsRaw();
            row.setProcessId(apiCallEntity.getApi_gateway_uuid());
            row.setApiId(apiCallEntity.getAllPathId());
            row.setTimeGranularity(-1);
            row.setTimeStart(Optional.ofNullable(apiCallEntity.getReqTime()).map(t -> t / 1000L).orElse(null));
            row.setReqCount(1L);
            row.setErrorCount(ApiMetricsDelayInfoUtil.checkByCode(apiCallEntity.getCode(), apiCallEntity.getHttpStatus()) ? 0L: 1L);
            row.setRps(1D / 60D);
            row.setBytes(new ArrayList<>(List.of(apiCallEntity.getReqBytes())));
            row.setDelay(new ArrayList<>(List.of(apiCallEntity.getLatency())));
            row.setSubMetrics(new HashMap<>());
            row.setCallId(apiCallEntity.getId());
            row.setId(new ObjectId());
            consumer.accept(row);
        }
    }
}
