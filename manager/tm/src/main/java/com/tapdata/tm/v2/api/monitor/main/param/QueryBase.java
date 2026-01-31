package com.tapdata.tm.v2.api.monitor.main.param;

import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

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
     * Front end parameter transmission (and interval repair using data reporting timing task delay)
     */
    Long startAt;
    Long endAt;

    /**
     * Interval repair for unused data reporting scheduled task delay, required for querying CPU&memory curves
     * */
    Long realStart;
    Long realEnd;

    /**
     * @see TimeType
     */
    String type;
    Long step;

    /**
     * Filter range interval
     */
    long queryStart;
    long queryEnd;

    /**
     * Return result point interval
     */
    long fixStart;
    long fixEnd;

    TimeGranularity granularity;

    SortInfo sortInfo;

    /**
     * Start time of sliding window, used for statistical chart calculation of p95/p99
     * */
    long windowsStart;
    Map<TimeGranularity, List<TimeRange>> queryRange;

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

        public <T> void order(List<T> result, Comparator<T> comparing) {
            if ("DESC".equals(order)) {
                result.sort(comparing.reversed());
            } else {
                result.sort(comparing);
            }
        }
    }

    @Getter
    public enum TimeType {
        MINUTE("minute", 60L, 5L),
        HOURS("hours", 60L * 60L, 1L),
        DAYS("days", 24L * 60L * 60L, 1L),
        RANGE("range", 60L * 60L, 1L);
        final String key;
        final long defaultSecond;
        final long defaultStep;

        TimeType(String key, long defaultSecond, long defaultStep) {
            this.key = key;
            this.defaultSecond = defaultSecond;
            this.defaultStep = defaultStep;
        }

        public static TimeType parse(String key) {
            for (TimeType value : values()) {
                if (value.key.equalsIgnoreCase(key)) {
                    return value;
                }
            }
            return MINUTE;
        }
    }
}
