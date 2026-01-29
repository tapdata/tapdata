package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TimeRangeUtilTest {

    @Nested
    class SpiltTimeTest {
        @Test
        void split() {
            List<TimeRange> split = TimeRangeUtil.split(1768960357L, 1768963897L);
            Assertions.assertEquals(5, split.size());
            Map<TimeGranularity, List<TimeRange>> collect = split.stream().collect(Collectors.groupingBy(TimeRange::getUnit));
            Assertions.assertNull(collect.get(TimeGranularity.HOUR));
            Assertions.assertNotNull(collect.get(TimeGranularity.MINUTE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND_FIVE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND));
            Assertions.assertEquals(1, collect.get(TimeGranularity.MINUTE).size());
            List<TimeRange> timeRanges = collect.get(TimeGranularity.MINUTE);
            TimeRange timeRangeMinute = timeRanges.get(0);
            Assertions.assertNotNull(timeRangeMinute);
            Assertions.assertEquals( 1768960380L, timeRangeMinute.getStart());
            Assertions.assertEquals( 1768963860L, timeRangeMinute.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.SECOND_FIVE).size());
            List<TimeRange> timeRanges5Sec = collect.get(TimeGranularity.SECOND_FIVE);
            TimeRange timeRange5Sec1 = timeRanges5Sec.get(0);
            Assertions.assertNotNull(timeRange5Sec1);
            Assertions.assertEquals( 1768960360L, timeRange5Sec1.getStart());
            Assertions.assertEquals( 1768960380L, timeRange5Sec1.getEnd());

            TimeRange timeRange5Sec2 = timeRanges5Sec.get(1);
            Assertions.assertNotNull(timeRange5Sec2);
            Assertions.assertEquals( 1768963860L, timeRange5Sec2.getStart());
            Assertions.assertEquals( 1768963895L, timeRange5Sec2.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.SECOND).size());
            List<TimeRange> timeRangesSec = collect.get(TimeGranularity.SECOND);
            TimeRange timeRangeSec1 = timeRangesSec.get(0);
            Assertions.assertNotNull(timeRangeSec1);
            Assertions.assertEquals( 1768960357L, timeRangeSec1.getStart());
            Assertions.assertEquals( 1768960360L, timeRangeSec1.getEnd());

            TimeRange timeRangeSec2 = timeRangesSec.get(1);
            Assertions.assertNotNull(timeRangeSec2);
            Assertions.assertEquals( 1768963895L, timeRangeSec2.getStart());
            Assertions.assertEquals( 1768963897L, timeRangeSec2.getEnd());

        }

        //@Test
        void split1() {
            //2026-01-27 10:38:50
            //2026-01-28 10:38:50

            //2026-01-27 10:38:50 ~ 2026-01-27 10:39:00 s
            //2026-01-27 10:39:00 ~ 2026-01-27 11:00:00 m
            //2026-01-27 11:00:00 ~ 2026-01-28 10:00:00 h
            //2026-01-28 10:00:00 ~ 2026-01-28 10:38:00 m
            //2026-01-28 10:38:00 ~ 2026-01-28 10:38:50 s
            List<TimeRange> split = TimeRangeUtil.split(1769481530L, 1769567930L);
            Assertions.assertEquals(5, split.size());
            Map<TimeGranularity, List<TimeRange>> collect = split.stream().collect(Collectors.groupingBy(TimeRange::getUnit));
            Assertions.assertNull(collect.get(TimeGranularity.HOUR));
            Assertions.assertNotNull(collect.get(TimeGranularity.MINUTE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND_FIVE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND));

            Assertions.assertEquals(1, collect.get(TimeGranularity.HOUR).size());
            List<TimeRange> timeRangesH = collect.get(TimeGranularity.HOUR);
            TimeRange timeRangeH = timeRangesH.get(0);
            Assertions.assertNotNull(timeRangeH);
            Assertions.assertEquals( 1769482800L, timeRangeH.getStart());
            Assertions.assertEquals( 1769565600L, timeRangeH.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.MINUTE).size());
            List<TimeRange> timeRanges = collect.get(TimeGranularity.MINUTE);
            TimeRange timeRangeMinute = timeRanges.get(0);
            Assertions.assertNotNull(timeRangeMinute);
            Assertions.assertEquals( 1769481540L, timeRangeMinute.getStart());
            Assertions.assertEquals( 1769482800L, timeRangeMinute.getEnd());
            TimeRange timeRangeMinute1 = timeRanges.get(1);
            Assertions.assertNotNull(timeRangeMinute1);
            Assertions.assertEquals( 1769565600L, timeRangeMinute1.getStart());
            Assertions.assertEquals( 1769567880L, timeRangeMinute1.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.SECOND_FIVE).size());
            List<TimeRange> timeRangesSec = collect.get(TimeGranularity.SECOND_FIVE);
            TimeRange timeRangeS = timeRangesSec.get(0);
            Assertions.assertNotNull(timeRangeS);
            Assertions.assertEquals( 1769481530L, timeRangeS.getStart());
            Assertions.assertEquals( 1769481540L, timeRangeS.getEnd());
            TimeRange timeRangeS1 = timeRangesSec.get(1);
            Assertions.assertNotNull(timeRangeS1);
            Assertions.assertEquals( 1769567880L, timeRangeS1.getStart());
            Assertions.assertEquals( 1769567930L, timeRangeS1.getEnd());
        }
    }
}