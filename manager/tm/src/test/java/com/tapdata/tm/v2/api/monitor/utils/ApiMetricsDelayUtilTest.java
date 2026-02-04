package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ApiMetricsDelayUtilTest {

    @Nested
    class MergeTest {
        @Test
        void testMergeWithEmptyLists() {
            List<Map<String, Number>> result = ApiMetricsDelayUtil.merge();

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void testMergeWithSingleList() {
            List<Map<String, Number>> list1 = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 5),
                    ApiMetricsDelayUtil.toMap(200L, 10)
            );

            List<Map<String, Number>> result = ApiMetricsDelayUtil.merge(list1);

            assertNotNull(result);
            assertEquals(2, result.size());
        }

        @Test
        void testMergeWithNullMapsInList() {
            List<Map<String, Number>> list1 = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 5),
                    null,
                    ApiMetricsDelayUtil.toMap(200L, 10)
            );

            List<Map<String, Number>> result = ApiMetricsDelayUtil.merge(list1);

            assertNotNull(result);
            assertEquals(2, result.size());
        }
    }

    @Nested
    class SumTest {
        @Test
        void testSumWithEmptyList() {
            Double result = ApiMetricsDelayUtil.sum(Collections.emptyList()).getTotal();
            assertEquals(0L, result);
        }

        @Test
        void testSumWithNullList() {
            Double result = ApiMetricsDelayUtil.sum(null).getTotal();
            assertEquals(0L, result);
        }
    }

    @Nested
    class PercentileTest {
        private List<Map<String, Number>> createTestData() {
            return Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10),
                    ApiMetricsDelayUtil.toMap(200L, 20),
                    ApiMetricsDelayUtil.toMap(300L, 30),
                    ApiMetricsDelayUtil.toMap(400L, 40)
            );
        }

        @Test
        void testP95WithValidData() {
            List<Map<String, Number>> data = createTestData();
            Double result = ApiMetricsDelayUtil.p95(data, 100L);
            assertNotNull(result);
        }

        @Test
        void testP95WithInsufficientTotal() {
            List<Map<String, Number>> data = createTestData();
            Double result = ApiMetricsDelayUtil.p95(data, 20L); // <= 20
            assertNull(result);
        }

        @Test
        void testP95WithInsufficientSize() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10)
            );
            Double result = ApiMetricsDelayUtil.p95(data, 100L);
            assertNull(result);
        }

        @Test
        void testP99WithValidData() {
            List<Map<String, Number>> data = createTestData();
            Double result = ApiMetricsDelayUtil.p99(data, 100L);
            assertNotNull(result);
        }

        @Test
        void testP99WithInsufficientTotal() {
            List<Map<String, Number>> data = createTestData();
            Double result = ApiMetricsDelayUtil.p99(data, 10L); // <= 10
            assertNull(result);
        }

        @Test
        void testP99WithInsufficientSize() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10)
            );
            Double result = ApiMetricsDelayUtil.p99(data, 100L);
            assertNull(result);
        }

        @Test
        void testP50WithValidData() {
            List<Map<String, Number>> data = createTestData();

            Double result = ApiMetricsDelayUtil.p50(data, 100L);

            assertNotNull(result);
        }

        @Test
        void testP50WithInsufficientSize() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10)
            );

            Double result = ApiMetricsDelayUtil.p50(data, 100L);

            assertNull(result);
        }
    }

    @Nested
    class PTest {
        @Test
        void testPWithValidData() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10),
                    ApiMetricsDelayUtil.toMap(200L, 20),
                    ApiMetricsDelayUtil.toMap(300L, 30)
            );

            // Test p method through reflection or create a public wrapper
            Double result = ApiMetricsDelayUtil.p50(data, 60L); // This calls p internally

            assertNotNull(result);
        }

        @Test
        void testPWithTargetReachedEarly() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 50), // Large count
                    ApiMetricsDelayUtil.toMap(200L, 10)
            );

            Double result = ApiMetricsDelayUtil.p50(data, 60L);

            assertNotNull(result);
            assertEquals(100L, result); // Should return first key as target reached
        }

        @Test
        void testPWithTargetNotReached() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 5),
                    ApiMetricsDelayUtil.toMap(200L, 5)
            );

            Double result = ApiMetricsDelayUtil.p95(data, 100L); // Target = 95, sum = 10

            assertNull(result);
        }

        @Test
        void testPWithEmptyKeySet() {
            // Create a map with empty key set (edge case)
            List<Map<String, Number>> data = Arrays.asList(
                    new HashMap<>() // Empty map
            );

            Double result = ApiMetricsDelayUtil.p50(data, 50L);

            // Should handle gracefully
            assertNull(result);
        }
    }

    @Nested
    class ReadMaxAndMinTest {
        @Test
        void testReadMaxAndMinWithValidData() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(100L, 10),
                    ApiMetricsDelayUtil.toMap(300L, 20),
                    ApiMetricsDelayUtil.toMap(200L, 15)
            );

            DoubleConsumer maxConsumer = mock(DoubleConsumer.class);
            DoubleConsumer minConsumer = mock(DoubleConsumer.class);

            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);

            verify(maxConsumer).accept(300L);
            verify(minConsumer).accept(100L);
        }

        @Test
        void testReadMaxAndMinWithSingleValue() {
            List<Map<String, Number>> data = Arrays.asList(
                    ApiMetricsDelayUtil.toMap(150L, 10)
            );

            DoubleConsumer maxConsumer = mock(DoubleConsumer.class);
            DoubleConsumer minConsumer = mock(DoubleConsumer.class);

            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);

            verify(maxConsumer).accept(150D);
            verify(minConsumer).accept(150D);
        }
    }
}