package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ParticleSizeAnalyzerTest {

    @Nested
    class OfWithValueBaseTest {
        @Test
        void testOfWithValueBase() {
            ValueBase valueBase = new ValueBase();
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(2000L);
            
            Criteria criteria = ParticleSizeAnalyzer.of(valueBase, query);
            
            assertNotNull(criteria);
            assertEquals(1000L, valueBase.getQueryFrom());
            assertEquals(2000L, valueBase.getQueryEnd());
            assertNotNull(valueBase.getGranularity());
        }
    }

    @Nested
    class OfWithQueryBaseTest {
        @Test
        void testOfWithSmallRange() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(2000L); // Range < 1 hour
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertEquals(0, query.getGranularity());
            assertEquals(1000L, query.getStartAt());
            assertEquals(2000L, query.getEndAt());
        }

        @Test
        void testOfWithMediumRange() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(1000L + 60L * 60L * 2L); // 2 hours
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertEquals(1, query.getGranularity());
            assertEquals(960L, query.getStartAt());
            assertEquals(8160L, query.getEndAt());
        }

        @Test
        void testOfWithLargeRange() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(1000L + 60L * 60L * 25L); // 25 hours
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertEquals(2, query.getGranularity());
            assertEquals(0L, query.getStartAt());
            assertEquals(90000L, query.getEndAt());
        }

        @Test
        void testOfWithNullEndTime() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(null);
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertNotNull(query.getEndAt());
            assertTrue(query.getEndAt() > 0);
        }

        @Test
        void testOfWithNullStartTime() {
            QueryBase query = new QueryBase();
            long currentTime = System.currentTimeMillis() / 1000L;
            query.setStartAt(null);
            query.setEndAt(currentTime);
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertNotNull(query.getStartAt());
        }

        @Test
        void testOfWithBothNullTimes() {
            QueryBase query = new QueryBase();
            query.setStartAt(null);
            query.setEndAt(null);
            
            Criteria criteria = ParticleSizeAnalyzer.of(query);
            
            assertNotNull(criteria);
            assertNotNull(query.getStartAt());
            assertNotNull(query.getEndAt());
            assertEquals(5L * 60L, query.getEndAt() - query.getStartAt());
        }
    }

    @Nested
    class ApiMetricsRawsTest {
        @Test
        void testApiMetricsRawsWithGranularity0() {
            QueryBase query = new QueryBase();
            query.setGranularity(0);
            query.setStartAt(1000L);
            query.setEndAt(2000L);
            
            ApiMetricsRaw raw1 = new ApiMetricsRaw();
            raw1.setTimeStart(1500L);
            Map<Long, ApiMetricsRaw> subMetrics1 = new HashMap<>();
            
            ApiMetricsRaw sub1 = new ApiMetricsRaw();
            sub1.setTimeStart(1200L);
            ApiMetricsRaw sub2 = new ApiMetricsRaw();
            sub2.setTimeStart(1800L);
            ApiMetricsRaw sub3 = new ApiMetricsRaw();
            sub3.setTimeStart(2500L); // Outside range
            
            subMetrics1.put(1200L, sub1);
            subMetrics1.put(1800L, sub2);
            subMetrics1.put(2500L, sub3);
            raw1.setSubMetrics(subMetrics1);
            
            List<ApiMetricsRaw> input = Arrays.asList(raw1);
            
            List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(input, query);
            
            assertEquals(2, result.size());
            assertEquals(1200L, result.get(0).getTimeStart());
            assertEquals(1800L, result.get(1).getTimeStart());
        }

        @Test
        void testApiMetricsRawsWithGranularity1() {
            QueryBase query = new QueryBase();
            query.setGranularity(1);
            
            ApiMetricsRaw raw1 = new ApiMetricsRaw();
            List<ApiMetricsRaw> input = Arrays.asList(raw1);
            
            List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(input, query);
            
            assertEquals(input, result);
        }

        @Test
        void testApiMetricsRawsWithEmptySubMetrics() {
            QueryBase query = new QueryBase();
            query.setGranularity(0);
            query.setStartAt(1000L);
            query.setEndAt(2000L);
            
            ApiMetricsRaw raw1 = new ApiMetricsRaw();
            raw1.setTimeStart(1500L);
            raw1.setSubMetrics(new HashMap<>());
            
            List<ApiMetricsRaw> input = Arrays.asList(raw1);
            
            List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(input, query);
            
            assertTrue(result.isEmpty());
        }

        @Test
        void testApiMetricsRawsWithRawOutsideRange() {
            QueryBase query = new QueryBase();
            query.setGranularity(0);
            query.setStartAt(1000L);
            query.setEndAt(2000L);
            
            ApiMetricsRaw raw1 = new ApiMetricsRaw();
            raw1.setTimeStart(3000L); // Outside range
            Map<Long, ApiMetricsRaw> subMetrics1 = new HashMap<>();
            raw1.setSubMetrics(subMetrics1);
            
            List<ApiMetricsRaw> input = Arrays.asList(raw1);
            
            List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(input, query);
            
            assertTrue(result.isEmpty());
        }
    }

    @Nested
    class CheckQueryTimeTest {
        @Test
        void testCheckQueryTimeWithValidRange() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(2000L);
            
            assertDoesNotThrow(() -> ParticleSizeAnalyzer.checkQueryTime(query));
        }

        @Test
        void testCheckQueryTimeWithTooLargeRange() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(1000L + 32L * 24L * 60L * 60L); // 32 days
            
            BizException exception = assertThrows(BizException.class, 
                () -> ParticleSizeAnalyzer.checkQueryTime(query));
            assertEquals("query.range.too.large", exception.getErrorCode());
        }

        @Test
        void testCheckQueryTimeWithNullEnd() {
            QueryBase query = new QueryBase();
            query.setStartAt(1000L);
            query.setEndAt(null);
            
            ParticleSizeAnalyzer.checkQueryTime(query);
            
            assertNotNull(query.getEndAt());
            assertTrue(query.getEndAt() > 0);
        }

        @Test
        void testCheckQueryTimeWithNullStart() {
            QueryBase query = new QueryBase();
            long currentTime = System.currentTimeMillis() / 1000L;
            query.setStartAt(null);
            query.setEndAt(currentTime);
            
            ParticleSizeAnalyzer.checkQueryTime(query);
            
            assertNotNull(query.getStartAt());
            assertEquals(currentTime - 5L * 60L, query.getStartAt());
        }

        @Test
        void testCheckQueryTimeWithBothNull() {
            QueryBase query = new QueryBase();
            query.setStartAt(null);
            query.setEndAt(null);
            
            ParticleSizeAnalyzer.checkQueryTime(query);
            
            assertNotNull(query.getStartAt());
            assertNotNull(query.getEndAt());
            assertEquals(5L * 60L, query.getEndAt() - query.getStartAt());
        }
    }

    @Nested
    class SpiltTimeTest {
        @Test
        void split() {
            List<ParticleSizeAnalyzer.TimeRange> split = ParticleSizeAnalyzer.split(1768960357L, 1768963897L);
            Assertions.assertEquals(5, split.size());
            Map<TimeGranularity, List<ParticleSizeAnalyzer.TimeRange>> collect = split.stream().collect(Collectors.groupingBy(ParticleSizeAnalyzer.TimeRange::getUnit));
            Assertions.assertNull(collect.get(TimeGranularity.HOUR));
            Assertions.assertNotNull(collect.get(TimeGranularity.MINUTE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND_FIVE));
            Assertions.assertNotNull(collect.get(TimeGranularity.SECOND));
            Assertions.assertEquals(1, collect.get(TimeGranularity.MINUTE).size());
            List<ParticleSizeAnalyzer.TimeRange> timeRanges = collect.get(TimeGranularity.MINUTE);
            ParticleSizeAnalyzer.TimeRange timeRangeMinute = timeRanges.get(0);
            Assertions.assertNotNull(timeRangeMinute);
            Assertions.assertEquals( 1768960380L, timeRangeMinute.getStart());
            Assertions.assertEquals( 1768963860L, timeRangeMinute.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.SECOND_FIVE).size());
            List<ParticleSizeAnalyzer.TimeRange> timeRanges5Sec = collect.get(TimeGranularity.SECOND_FIVE);
            ParticleSizeAnalyzer.TimeRange timeRange5Sec1 = timeRanges5Sec.get(0);
            Assertions.assertNotNull(timeRange5Sec1);
            Assertions.assertEquals( 1768960360L, timeRange5Sec1.getStart());
            Assertions.assertEquals( 1768960380L, timeRange5Sec1.getEnd());

            ParticleSizeAnalyzer.TimeRange timeRange5Sec2 = timeRanges5Sec.get(1);
            Assertions.assertNotNull(timeRange5Sec2);
            Assertions.assertEquals( 1768963860L, timeRange5Sec2.getStart());
            Assertions.assertEquals( 1768963895L, timeRange5Sec2.getEnd());

            Assertions.assertEquals(2, collect.get(TimeGranularity.SECOND).size());
            List<ParticleSizeAnalyzer.TimeRange> timeRangesSec = collect.get(TimeGranularity.SECOND);
            ParticleSizeAnalyzer.TimeRange timeRangeSec1 = timeRangesSec.get(0);
            Assertions.assertNotNull(timeRangeSec1);
            Assertions.assertEquals( 1768960357L, timeRangeSec1.getStart());
            Assertions.assertEquals( 1768960360L, timeRangeSec1.getEnd());

            ParticleSizeAnalyzer.TimeRange timeRangeSec2 = timeRangesSec.get(1);
            Assertions.assertNotNull(timeRangeSec2);
            Assertions.assertEquals( 1768963895L, timeRangeSec2.getStart());
            Assertions.assertEquals( 1768963897L, timeRangeSec2.getEnd());

        }
    }
}