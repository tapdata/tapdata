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
import java.util.Comparator;
import java.util.EnumMap;
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
    public static final long MAX_QUERY_RANGE = 30L * 24L * 60L * 60L;
    public static final long MAX_QUERY_RANGE_DAY = 30L;

    private TimeRangeUtil() {
    }

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
        if (range < TimeGranularity.HOUR.getSeconds()) {
            query.setGranularity(TimeGranularity.SECOND_FIVE);
        } else if (range < TimeGranularity.DAY.getSeconds()) {
            query.setGranularity(TimeGranularity.MINUTE);
        } else {
            query.setGranularity(TimeGranularity.HOUR);
        }
        query.setQueryStart(queryStartAt);
        query.setQueryEnd(originEnd);
        query.setFixStart(query.getGranularity().fixTime(startAt));
        query.setFixEnd(query.getGranularity().fixTime(originEnd));
        Map<TimeGranularity, List<TimeRange>> timeGranularityListMap = doNotCompress(query, queryStartAt, queryEndAt);
        if (!compress) {
            query.setQueryRange(timeGranularityListMap);
        } else {
            query.setQueryRange(compress(timeGranularityListMap));
        }
    }

    static Map<TimeGranularity, List<TimeRange>> compress(Map<TimeGranularity, List<TimeRange>> timeGranularity) {
        Map<TimeGranularity, List<TimeRange>> compressMap = new EnumMap<>(TimeGranularity.class);
        timeGranularity.forEach((type, ranges) -> {
            ranges.sort(Comparator.comparing(TimeRange::getStart));
            TimeRange r = null;
            for (TimeRange range : ranges) {
                if (null == r) {
                    r = range;
                } else {
                   long e = r.getEnd();
                   if (e == range.getStart()) {
                       r.setEnd(range.getEnd());
                   } else {
                       List<TimeRange> temp = compressMap.computeIfAbsent(type, k -> new ArrayList<>());
                       temp.add(r);
                       r = range;
                   }
                }
            }
            List<TimeRange> temp = compressMap.computeIfAbsent(type, k -> new ArrayList<>());
            if (null != r) {
                temp.add(r);
            }
        });
        return compressMap;
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
        //sliding window
        long windowsStart = fixStart - granularity.getSupplement();
        query.setWindowsStart(windowsStart);
        Map<TimeGranularity, List<TimeRange>> timeGranularityListMap = new EnumMap<>(TimeGranularity.class);
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

    public static TimeGranularity rangeOf(long range) {
        if (range >= TimeGranularity.DAY.getSeconds()) {
            return TimeGranularity.DAY;
        } else if (range >= TimeGranularity.HOUR.getSeconds()) {
            return TimeGranularity.HOUR;
        } else if (range >= TimeGranularity.MINUTE.getSeconds()) {
            return TimeGranularity.MINUTE;
        } else if (range >= TimeGranularity.SECOND_FIVE.getSeconds()) {
            return TimeGranularity.SECOND_FIVE;
        } else {
            return TimeGranularity.SECOND;
        }
    }

    public static List<TimeRange> split(long startAt, long endAt) {
        List<TimeRange> result = new ArrayList<>();
        if (startAt >= endAt) {
            return result;
        }
        //Determine the maximum allowable granularity based on the time range
        long range = endAt - startAt;
        TimeGranularity maxAllowedUnit = rangeOf(range);
        long cursor = startAt;
        TimeGranularity lastUnit = null;
        long last = startAt;
        while (cursor < endAt) {
            TimeGranularity unit = chooseBestUnit(cursor);
            // Limit granularity to no more than the maximum allowable value
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
            // Scenario 1: Granularity upgrade (from small granularity to large granularity)
            if (lastUnit != unit && unit.getType() > lastUnit.getType()) {
                TimeRange current = new TimeRange(last, cursor, lastUnit);
                result.add(current);
                lastUnit = unit;
                last = cursor;
            }
            // Scenario 2: Granularity degradation (from large to small granularity)
            else if (unit != nextUnit && nextUnit.getType() < lastUnit.getType()) {
                TimeRange current = new TimeRange(last, cursor, lastUnit);
                result.add(current);
                lastUnit = nextUnit;
                last = cursor;
            }
            cursor = nextCursor;
        }
        // Process the last interval
        if (lastUnit != null) {
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
        if (localDateTime.getSecond() % TimeGranularity.SECOND_FIVE.getSeconds() == 0) {
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
            // If this granularity does not exceed the endpoint or is already the minimum granularity, use it
            if (nextTime <= endAt || candidate.getLowerOne() == null) {
                return candidate;
            }
            // Otherwise, try smaller granularity
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
            if (range > MAX_QUERY_RANGE) {
                throw new BizException("query.range.too.large", MAX_QUERY_RANGE_DAY);
            }
        }
        query.setRealStart(query.getStartAt());
        query.setRealEnd(query.getEndAt());
        long step = Optional.ofNullable(query.getStep()).orElse(type.getDefaultStep());
        if (type != QueryBase.TimeType.RANGE) {
            now = TimeGranularity.SECOND_FIVE.fixTime(now);
            query.setStartAt(now - type.getDefaultSecond() * step);
            query.setEndAt(now);
            timestamp = TimeGranularity.SECOND_FIVE.fixTime(timestamp);
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
