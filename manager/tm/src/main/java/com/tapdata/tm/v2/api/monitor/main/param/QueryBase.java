package com.tapdata.tm.v2.api.monitor.main.param;

import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.service.ParticleSizeAnalyzer;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:16 Create
 * @description
 */
@Data
public class QueryBase {
    /**
     * 前端传参
     */
    Long startAt;
    Long endAt;

    /**
     * 筛选范围区间
     */
    long queryStart;
    long queryEnd;

    /**
     * 返回结果点位区间
     */
    long fixStart;
    long fixEnd;


    Long qStart;

    int granularity;

    long batchStart;

    SortInfo sortInfo;

    QueryParam queryParam = new QueryParam();

    Map<TimeGranularity, List<ParticleSizeAnalyzer.TimeRange>> queryRange;

    @Data
    public static class SortInfo {
        String field;
        String order = "DESC";

        public static SortInfo parse(String orderBy) {
            if (orderBy == null) {
                return new SortInfo();
            }
            String[] fields = orderBy.split(" ");
            SortInfo sortInfo = new SortInfo();
            if (fields.length > 0 && StringUtils.isNotBlank(fields[0])) {
                sortInfo.field = fields[0];
            }
            if (fields.length > 1 && !StringUtils.isBlank(fields[1])) {
                sortInfo.order = fields[1].trim().toUpperCase(Locale.ROOT);
            }
            return sortInfo;
        }

        public <T>void order(List<T> result, Comparator<T> comparing) {
            if ("DESC".equals(order)) {
                result.sort(comparing.reversed());
            } else {
                result.sort(comparing);
            }
        }
    }

    @Data
    public static class QueryParam {
        List<Point> secondPoint = new ArrayList<>();
        List<Point> second5Point = new ArrayList<>();
        List<Point> minutePoint = new ArrayList<>();
        List<Point> hourPoint = new ArrayList<>();

        Long start;
        Long end;
    }

    @Data
    public static class Point {
        long start;
        long end;
        int granularity;

        public static Point of(long start, long end, int granularity) {
            Point point = new Point();
            point.setStart(start);
            point.setEnd(end);
            point.setGranularity(granularity);
            return point;
        }
    }
}
