package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        start = start / 5L * 5L;
        end = end / 5L * 5L;
        long range = end - start;
        long s = start;
        long e = end;
        if (range < 60L * 60L) {
            query.setGranularity(0);
        } else if (range < 60L * 60L * 24L) {
            query.setGranularity(1);
            s = (start / 60L * 60L);
            e = (end / 60L * 60L);
        } else {
            query.setGranularity(2);
            s = (start / 3600L * 3600L);
            e = (end / 3600L * 3600L);
        }
        query.setStartAt(s);
        query.setEndAt(e);
        return Criteria.where("timeGranularity").is(query.getGranularity() == 0 ? 1 : 0).andOperator(
                Criteria.where("timeStart").gte(s),
                Criteria.where("timeStart").lte(e)
        );
    }

    public static List<ApiMetricsRaw> apiMetricsRaws(List<ApiMetricsRaw> apiMetricsRaws, QueryBase query) {
        if (query.getGranularity() == 0) {
            List<ApiMetricsRaw> sub = new ArrayList<>();
            for (ApiMetricsRaw raw : apiMetricsRaws) {
                long timeStart = raw.getTimeStart();
                if (timeStart >= query.getStartAt() && timeStart <= query.getEndAt()) {
                    Map<Long, ApiMetricsRaw> subMetrics = raw.getSubMetrics();
                    subMetrics.values()
                            .stream()
                            .filter(e -> e.getTimeStart() >= query.getStartAt() && e.getTimeStart() <= query.getEndAt())
                            .sorted(Comparator.comparing(ApiMetricsRaw::getTimeStart))
                            .forEach(sub::add);
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
}
