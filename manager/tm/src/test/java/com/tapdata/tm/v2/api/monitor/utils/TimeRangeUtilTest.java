package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.v2.api.common.main.dto.TimeRange;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
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
    }
}