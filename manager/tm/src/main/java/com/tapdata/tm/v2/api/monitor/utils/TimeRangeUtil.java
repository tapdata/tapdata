package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/24 10:47 Create
 * @description
 */
public class TimeRangeUtil {

    private TimeRangeUtil() {}

    public static void rangeOf(ValueBase valueBase, QueryBase query, long delay, boolean compress) {
        rangeOf(query, delay, compress);
        valueBase.setQueryFrom(query.getStartAt());
        valueBase.setQueryEnd(query.getEndAt());
        valueBase.setGranularity(query.getGranularity().getType());
    }

    public static void rangeOf(QueryBase query, long delay, boolean compress) {
        checkQueryTime(query, delay);
        Long startAt = query.getStartAt();
        Long endAt = query.getEndAt();
        long originEnd = endAt;
        long range = endAt - startAt;

        long queryStartAt = startAt;
        long queryEndAt = endAt;
        if (range < 60L * 60L) {
            query.setGranularity(TimeGranularity.SECOND_FIVE);
        } else if (range < 60L * 60L * 24L) {
            query.setGranularity(TimeGranularity.MINUTE);
        } else {
            query.setGranularity(TimeGranularity.HOUR);
        }
        query.setQueryStart(queryStartAt);
        query.setQueryEnd(originEnd);
        query.setFixStart(query.getGranularity().fixTime(startAt));
        query.setFixEnd(query.getGranularity().fixTime(originEnd));
        if (!compress) {
            query.setQueryRange(doNotCompress(query, queryStartAt, queryEndAt));
        } else {
            query.setQueryRange(doCompress(queryStartAt, queryEndAt));
        }

    }

    static Map<TimeGranularity, List<TimeRange>> doCompress(long queryStartAt, long queryEndAt) {
        List<TimeRange> split = split(queryStartAt, queryEndAt);
        return split.stream().collect(
                Collectors.groupingBy(TimeRange::getUnit)
        );
    }

    static Map<TimeGranularity, List<TimeRange>> doNotCompress(QueryBase query, long startAt, long endAt) {
        TimeGranularity granularity = query.getGranularity();
        long fixStart = query.getFixStart();
        long fixEnd = granularity.fixTime(query.getEndAt() + granularity.getSeconds() - 1);
        //滑动窗口
        long windowsStart = fixStart - granularity.getSupplement();
        query.setWindowsStart(windowsStart);
        Map<TimeGranularity, List<TimeRange>> timeGranularityListMap = new HashMap<>();
        if (startAt < fixStart) {
            split(startAt, fixStart).forEach(timeRange -> {
                List<TimeRange> ranges = timeGranularityListMap.computeIfAbsent(timeRange.getUnit(), k -> new ArrayList<>());
                ranges.add(timeRange);
            });
        }
        List<TimeRange> rangesGranularity = timeGranularityListMap.computeIfAbsent(granularity, k -> new ArrayList<>());
        rangesGranularity.add(new TimeRange(fixStart, fixEnd, granularity));
        if (fixEnd < endAt) {
            split(fixEnd, endAt).forEach(timeRange -> {
                List<TimeRange> ranges = timeGranularityListMap.computeIfAbsent(timeRange.getUnit(), k -> new ArrayList<>());
                ranges.add(timeRange);
            });
        }
        if (windowsStart < fixStart) {
            List<TimeRange> ranges = timeGranularityListMap.computeIfAbsent(granularity, k -> new ArrayList<>());
            boolean supplemented = false;
            for (TimeRange range : ranges) {
                if (range.getStart() == fixStart) {
                    supplemented = true;
                    range.setStart(windowsStart);
                    break;
                }
            }
            if (!supplemented) {
                ranges.add(new TimeRange(windowsStart, fixStart, granularity));
            }
        }
        return timeGranularityListMap;
    }

    public static List<TimeRange> split(long startAt, long endAt) {
        List<TimeRange> result = new ArrayList<>();
        if (startAt >= endAt) {
            return result;
        }
        // 根据时间范围确定最大允许的粒度
        long range = endAt - startAt;
        TimeGranularity maxAllowedUnit;
        if (range > 86400L) {
            maxAllowedUnit = TimeGranularity.DAY;
        } else if (range > 3600L) {
            maxAllowedUnit = TimeGranularity.HOUR;
        } else if (range > 60L) {
            maxAllowedUnit = TimeGranularity.MINUTE;
        } else if (range > 5L) {
            maxAllowedUnit = TimeGranularity.SECOND_FIVE;
        } else {
            maxAllowedUnit = TimeGranularity.SECOND;
        }
        long cursor = startAt;
        TimeGranularity lastUnit = null;
        long last = startAt;
        while (cursor < endAt) {
            TimeGranularity unit = chooseBestUnit(cursor);
            // 限制粒度不超过最大允许值
            while (unit.getType() > maxAllowedUnit.getType()) {
                unit = unit.getLowerOne();
            }
            TimeGranularity nextUnit = chooseNextUnit(cursor, unit, endAt);
            if (null == nextUnit) {
                break;
            }
            if (lastUnit == null) {
                lastUnit = unit;
            }
            long nextCursor = cursor + nextUnit.getSeconds();
            // 情况1: 粒度升级（从小粒度到大粒度）
            if (lastUnit != unit && unit.getType() > lastUnit.getType()) {
                TimeRange current = new TimeRange(last, cursor, lastUnit);
                result.add(current);
                lastUnit = unit;
                last = cursor;
            }
            // 情况2: 粒度降级（从大粒度到小粒度）
            else if (unit != nextUnit && nextUnit.getType() < lastUnit.getType()) {
                TimeRange current = new TimeRange(last, cursor, lastUnit);
                result.add(current);
                lastUnit = nextUnit;
                last = cursor;
            }
            cursor = nextCursor;
        }
        // 处理最后一个区间
        if (last < endAt && lastUnit != null) {
            TimeRange current = new TimeRange(last, cursor, lastUnit);
            result.add(current);
        }
        return result;
    }

    private static TimeGranularity chooseBestUnit(long t) {
        Instant instant = Instant.ofEpochSecond(t);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        if (localDateTime.getSecond() == 0 && localDateTime.getMinute() == 0 && localDateTime.getHour() == 0) {
            return TimeGranularity.DAY;
        }
        if (localDateTime.getSecond() == 0 && localDateTime.getMinute() == 0) {
            return TimeGranularity.HOUR;
        }
        if (localDateTime.getSecond() == 0) {
            return TimeGranularity.MINUTE;
        }
        if (localDateTime.getSecond() % 5 == 0) {
            return TimeGranularity.SECOND_FIVE;
        }
        return TimeGranularity.SECOND;
    }

    private static TimeGranularity chooseNextUnit(long t, TimeGranularity unit, long endAt) {
        if (t > endAt) {
            return null;
        }
        TimeGranularity candidate = unit;
        while (candidate != null) {
            long nextTime = t + candidate.getSeconds();
            // 如果这个粒度不会超过终点，或者已经是最小粒度，就使用它
            if (nextTime <= endAt || candidate.getLowerOne() == null) {
                return candidate;
            }
            // 否则尝试更小的粒度
            candidate = candidate.getLowerOne();
        }
        return null;
    }

    public static void checkQueryTime(QueryBase query, long delay) {
        long timestamp = System.currentTimeMillis() / 1000L;
        delay = delay / 1000L;
        long now = timestamp - delay;
        QueryBase.TimeType type = QueryBase.TimeType.parse(query.getType());
        if (Objects.nonNull(query.getStartAt()) && Objects.nonNull(query.getEndAt())) {
            long range = query.getEndAt() - query.getStartAt();
            if (range > 30L * 24L * 60L * 60L) {
                throw new BizException("query.range.too.large", 31);
            }
        }
        query.setRealStart(query.getStartAt());
        query.setRealEnd(query.getEndAt());
        long step = Optional.ofNullable(query.getStep()).orElse(type.getDefaultStep());
        if (type != QueryBase.TimeType.RANGE) {
            now = now / 5L * 5L;
            query.setStartAt(now - type.getDefaultSecond() * step);
            query.setEndAt(now);

            timestamp = timestamp / 5L * 5L;
            query.setRealStart(timestamp - type.getDefaultSecond() * step);
            query.setRealEnd(timestamp);
        }
        Long end = query.getEndAt();
        if (null == end) {
            end = now;
            query.setEndAt(end);
            query.setRealEnd(timestamp);
        }
        Long start = query.getStartAt();
        if (null == start) {
            start = end - type.getDefaultSecond() * type.getDefaultStep();
            query.setStartAt(start);

            query.setRealStart(start);
        }
        if (end > now) {
            end = now;
            query.setEndAt(end);

            query.setRealEnd(timestamp);
        }
        if (start >= end) {
            start = end - type.getDefaultSecond() * type.getDefaultStep();
            query.setStartAt(start);

            query.setRealStart(start);
        }
    }
}
