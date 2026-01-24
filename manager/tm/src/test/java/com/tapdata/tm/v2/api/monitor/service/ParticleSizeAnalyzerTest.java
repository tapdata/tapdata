package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParticleSizeAnalyzerTest {


    @Nested
    class ApiMetricsRawsTest {
        @Test
        void testApiMetricsRawsWithGranularity0() {
            QueryBase query = new QueryBase();
            query.setGranularity(TimeGranularity.SECOND_FIVE);
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
            query.setGranularity(TimeGranularity.MINUTE);
            
            ApiMetricsRaw raw1 = new ApiMetricsRaw();
            List<ApiMetricsRaw> input = Arrays.asList(raw1);
            
            List<ApiMetricsRaw> result = ParticleSizeAnalyzer.apiMetricsRaws(input, query);
            
            assertEquals(input, result);
        }

        @Test
        void testApiMetricsRawsWithEmptySubMetrics() {
            QueryBase query = new QueryBase();
            query.setGranularity(TimeGranularity.SECOND_FIVE);
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
            query.setGranularity(TimeGranularity.SECOND_FIVE);
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
}