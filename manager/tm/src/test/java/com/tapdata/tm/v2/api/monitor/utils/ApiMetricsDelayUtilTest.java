package com.tapdata.tm.v2.api.monitor.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedStatic;
import org.slf4j.Logger;

import java.util.*;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ApiMetricsDelayUtilTest {

    @Nested
    class FixDelayAsMapTest {
        @Test
        void testFixDelayAsMapWithEmptyList() {
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.fixDelayAsMap(Collections.emptyList());
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void testFixDelayAsMapWithNullList() {
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.fixDelayAsMap(null);
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void testFixDelayAsMapWithMapEntries() {
            Map<Long, Integer> map1 = new HashMap<>();
            map1.put(100L, 5);
            map1.put(200L, 10);
            
            Map<String, String> map2 = new HashMap<>();
            map2.put("300", "15");
            
            List<Object> input = Arrays.asList(map1, map2);
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.fixDelayAsMap(input);
            
            assertNotNull(result);
            assertEquals(3, result.size());
            
            // Verify the results contain expected values
            Set<Long> keys = new HashSet<>();
            Set<Integer> values = new HashSet<>();
            for (Map<Long, Integer> map : result) {
                keys.addAll(map.keySet());
                values.addAll(map.values());
            }
            
            assertTrue(keys.contains(100L));
            assertTrue(keys.contains(200L));
            assertTrue(keys.contains(300L));
            assertTrue(values.contains(5));
            assertTrue(values.contains(10));
            assertTrue(values.contains(15));
        }

        @Test
        void testFixDelayAsMapWithNumberEntries() {
            List<Object> input = Arrays.asList(100L, 200, 300.5);
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.fixDelayAsMap(input);
            
            assertNotNull(result);
            assertEquals(3, result.size());
            
            // Each number should be converted to a map with value 1
            for (Map<Long, Integer> map : result) {
                assertEquals(1, map.size());
                assertTrue(map.values().contains(1));
            }
        }

        @Test
        void testFixDelayAsMapWithMixedEntries() {
            Map<Long, Integer> map1 = new HashMap<>();
            map1.put(100L, 5);
            
            List<Object> input = Arrays.asList(map1, 200L, "not a number");
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.fixDelayAsMap(input);
            
            assertNotNull(result);
            assertEquals(2, result.size()); // map1 entry + 200L, "not a number" ignored
        }
    }

    @Nested
    class EachTest {
        @Test
        void testEachWithNumberKeyAndNumberValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            ApiMetricsDelayUtil.each(100L, 5, result);
            
            assertEquals(1, result.size());
            assertEquals(5, result.get(0).get(100L));
        }

        @Test
        void testEachWithStringKeyAndNumberValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            ApiMetricsDelayUtil.each("100", 5, result);
            
            assertEquals(1, result.size());
            assertEquals(5, result.get(0).get(100L));
        }

        @Test
        void testEachWithStringKeyAndStringValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            ApiMetricsDelayUtil.each("100", "5", result);
            
            assertEquals(1, result.size());
            assertEquals(5, result.get(0).get(100L));
        }

        @Test
        void testEachWithNumberKeyAndStringValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            ApiMetricsDelayUtil.each(100L, "5", result);
            
            assertEquals(1, result.size());
            assertEquals(5, result.get(0).get(100L));
        }

        @Test
        void testEachWithInvalidStringKey() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should log a warning but not add to result
            ApiMetricsDelayUtil.each("invalid", 5, result);
            
            assertTrue(result.isEmpty());
        }

        @Test
        void testEachWithInvalidStringValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should log a warning but not add to result
            ApiMetricsDelayUtil.each("100", "invalid", result);
            
            assertTrue(result.isEmpty());
        }

        @Test
        void testEachWithInvalidStringKeyAndValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should log a warning but not add to result
            ApiMetricsDelayUtil.each("invalid", "invalid", result);
            
            assertTrue(result.isEmpty());
        }

        @Test
        void testEachWithNumberKeyAndInvalidStringValue() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should log a warning but not add to result
            ApiMetricsDelayUtil.each(100L, "invalid", result);
            
            assertTrue(result.isEmpty());
        }

        @Test
        void testEachWithUnsupportedTypes() {
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should log a warning but not add to result
            ApiMetricsDelayUtil.each(new Object(), new Object(), result);
            
            assertTrue(result.isEmpty());
        }
    }

    @Nested
    class MergeTest {
        @Test
        void testMergeWithEmptyLists() {
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge();
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void testMergeWithNullLists() {
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge((List<Map<Long, Integer>>[]) null);
            
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        void testMergeWithSingleList() {
            List<Map<Long, Integer>> list1 = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 5),
                ApiMetricsDelayUtil.toMap(200L, 10)
            );
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1);
            
            assertNotNull(result);
            assertEquals(2, result.size());
        }

        @Test
        void testMergeWithMultipleLists() {
            List<Map<Long, Integer>> list1 = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 5),
                ApiMetricsDelayUtil.toMap(200L, 10)
            );
            
            List<Map<Long, Integer>> list2 = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 3), // Same key, should merge
                ApiMetricsDelayUtil.toMap(300L, 15)
            );
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1, list2);
            
            assertNotNull(result);
            assertEquals(3, result.size());
            
            // Check merged values
            Map<Long, Integer> merged = new HashMap<>();
            for (Map<Long, Integer> map : result) {
                merged.putAll(map);
            }
            
            assertEquals(8, merged.get(100L)); // 5 + 3
            assertEquals(10, merged.get(200L));
            assertEquals(15, merged.get(300L));
        }

        @Test
        void testMergeWithNullMapsInList() {
            List<Map<Long, Integer>> list1 = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 5),
                null,
                ApiMetricsDelayUtil.toMap(200L, 10)
            );
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1);
            
            assertNotNull(result);
            assertEquals(2, result.size());
        }

        @Test
        void testMergeWithNullKeysInMap() {
            Map<Long, Integer> mapWithNullKey = new HashMap<>();
            mapWithNullKey.put(100L, 5);
            mapWithNullKey.put(null, 10); // This should be ignored
            
            List<Map<Long, Integer>> list1 = Arrays.asList(mapWithNullKey);
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertFalse(result.get(0).containsKey(null));
        }

        @Test
        void testMergeWithNullValuesInMap() {
            Map<Long, Integer> mapWithNullValue = new HashMap<>();
            mapWithNullValue.put(100L, null); // Should be treated as 0
            
            List<Map<Long, Integer>> list1 = Arrays.asList(mapWithNullValue);
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(100L));
        }

        @Test
        void testMergeWithMixedNullLists() {
            List<Map<Long, Integer>> list1 = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 5)
            );
            
            List<Map<Long, Integer>> result = ApiMetricsDelayUtil.merge(list1, null, list1);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(10, result.get(0).get(100L)); // 5 + 5
        }
    }

    @Nested
    class SumTest {
        @Test
        void testSumWithEmptyList() {
            Long result = ApiMetricsDelayUtil.sum(Collections.emptyList());
            
            assertEquals(0L, result);
        }

        @Test
        void testSumWithNullList() {
            Long result = ApiMetricsDelayUtil.sum(null);
            
            assertEquals(0L, result);
        }

        @Test
        void testSumWithMapEntries() {
            Map<Long, Integer> map1 = new HashMap<>();
            map1.put(100L, 5); // 100 * 5 = 500
            map1.put(200L, 3); // 200 * 3 = 600
            
            Map<Long, Integer> map2 = new HashMap<>();
            map2.put(50L, 2); // 50 * 2 = 100
            
            List<Object> input = Arrays.asList(map1, map2);
            
            Long result = ApiMetricsDelayUtil.sum(input);
            
            assertEquals(1200L, result); // 500 + 600 + 100
        }

        @Test
        void testSumWithNumberEntries() {
            List<Object> input = Arrays.asList(100L, 200, 50.5);
            
            Long result = ApiMetricsDelayUtil.sum(input);
            
            assertEquals(350L, result); // 100 + 200 + 50
        }

        @Test
        void testSumWithMixedEntries() {
            Map<Long, Integer> map1 = new HashMap<>();
            map1.put(100L, 2); // 100 * 2 = 200
            
            List<Object> input = Arrays.asList(map1, 150L, "not a number");
            
            Long result = ApiMetricsDelayUtil.sum(input);
            
            assertEquals(350L, result); // 200 + 150, string ignored
        }

        @Test
        void testSumWithMapContainingNonNumbers() {
            Map<Object, Object> map1 = new HashMap<>();
            map1.put(100L, 2); // Valid: 100 * 2 = 200
            map1.put("invalid", 3); // Invalid key
            map1.put(200L, "invalid"); // Invalid value
            map1.put("invalid", "invalid"); // Both invalid
            
            List<Object> input = Arrays.asList(map1);
            
            Long result = ApiMetricsDelayUtil.sum(input);
            
            assertEquals(200L, result); // Only valid entry counted
        }
    }

    @Nested
    class PercentileTest {
        private List<Map<Long, Integer>> createTestData() {
            return Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10),
                ApiMetricsDelayUtil.toMap(200L, 20),
                ApiMetricsDelayUtil.toMap(300L, 30),
                ApiMetricsDelayUtil.toMap(400L, 40)
            );
        }

        @Test
        void testP95WithValidData() {
            List<Map<Long, Integer>> data = createTestData();
            
            Long result = ApiMetricsDelayUtil.p95(data, 100L);
            
            assertNotNull(result);
        }

        @Test
        void testP95WithInsufficientTotal() {
            List<Map<Long, Integer>> data = createTestData();
            
            Long result = ApiMetricsDelayUtil.p95(data, 20L); // <= 20
            
            assertNull(result);
        }

        @Test
        void testP95WithInsufficientSize() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10)
            );
            
            Long result = ApiMetricsDelayUtil.p95(data, 100L);
            
            assertNull(result);
        }

        @Test
        void testP99WithValidData() {
            List<Map<Long, Integer>> data = createTestData();
            
            Long result = ApiMetricsDelayUtil.p99(data, 100L);
            
            assertNotNull(result);
        }

        @Test
        void testP99WithInsufficientTotal() {
            List<Map<Long, Integer>> data = createTestData();
            
            Long result = ApiMetricsDelayUtil.p99(data, 10L); // <= 10
            
            assertNull(result);
        }

        @Test
        void testP99WithInsufficientSize() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10)
            );
            
            Long result = ApiMetricsDelayUtil.p99(data, 100L);
            
            assertNull(result);
        }

        @Test
        void testP50WithValidData() {
            List<Map<Long, Integer>> data = createTestData();
            
            Long result = ApiMetricsDelayUtil.p50(data, 100L);
            
            assertNotNull(result);
        }

        @Test
        void testP50WithInsufficientSize() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10)
            );
            
            Long result = ApiMetricsDelayUtil.p50(data, 100L);
            
            assertNull(result);
        }
    }

    @Nested
    class PTest {
        @Test
        void testPWithValidData() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10),
                ApiMetricsDelayUtil.toMap(200L, 20),
                ApiMetricsDelayUtil.toMap(300L, 30)
            );
            
            // Test p method through reflection or create a public wrapper
            Long result = ApiMetricsDelayUtil.p50(data, 60L); // This calls p internally
            
            assertNotNull(result);
        }

        @Test
        void testPWithTargetReachedEarly() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 50), // Large count
                ApiMetricsDelayUtil.toMap(200L, 10)
            );
            
            Long result = ApiMetricsDelayUtil.p50(data, 60L);
            
            assertNotNull(result);
            assertEquals(100L, result); // Should return first key as target reached
        }

        @Test
        void testPWithTargetNotReached() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 5),
                ApiMetricsDelayUtil.toMap(200L, 5)
            );
            
            Long result = ApiMetricsDelayUtil.p95(data, 100L); // Target = 95, sum = 10
            
            assertNull(result);
        }

        @Test
        void testPWithEmptyKeySet() {
            // Create a map with empty key set (edge case)
            List<Map<Long, Integer>> data = Arrays.asList(
                new HashMap<>() // Empty map
            );
            
            Long result = ApiMetricsDelayUtil.p50(data, 50L);
            
            // Should handle gracefully
            assertNull(result);
        }
    }

    @Nested
    class ReadMaxAndMinTest {
        @Test
        void testReadMaxAndMinWithValidData() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(100L, 10),
                ApiMetricsDelayUtil.toMap(300L, 20),
                ApiMetricsDelayUtil.toMap(200L, 15)
            );
            
            LongConsumer maxConsumer = mock(LongConsumer.class);
            LongConsumer minConsumer = mock(LongConsumer.class);
            
            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);
            
            verify(maxConsumer).accept(300L);
            verify(minConsumer).accept(100L);
        }

        @Test
        void testReadMaxAndMinWithEmptyData() {
            List<Map<Long, Integer>> data = Collections.emptyList();
            
            LongConsumer maxConsumer = mock(LongConsumer.class);
            LongConsumer minConsumer = mock(LongConsumer.class);
            
            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);
            
            verifyNoInteractions(maxConsumer);
            verifyNoInteractions(minConsumer);
        }

        @Test
        void testReadMaxAndMinWithEmptyMaps() {
            List<Map<Long, Integer>> data = Arrays.asList(
                new HashMap<>(),
                new HashMap<>()
            );
            
            LongConsumer maxConsumer = mock(LongConsumer.class);
            LongConsumer minConsumer = mock(LongConsumer.class);
            
            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);
            
            verifyNoInteractions(maxConsumer);
            verifyNoInteractions(minConsumer);
        }

        @Test
        void testReadMaxAndMinWithSingleValue() {
            List<Map<Long, Integer>> data = Arrays.asList(
                ApiMetricsDelayUtil.toMap(150L, 10)
            );
            
            LongConsumer maxConsumer = mock(LongConsumer.class);
            LongConsumer minConsumer = mock(LongConsumer.class);
            
            ApiMetricsDelayUtil.readMaxAndMin(data, maxConsumer, minConsumer);
            
            verify(maxConsumer).accept(150L);
            verify(minConsumer).accept(150L);
        }
    }

    @Nested
    class AddDelayTest {
        @Test
        void testAddDelayWithNullList() {
            List<Object> result = ApiMetricsDelayUtil.addDelay(null, 100L);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.get(0) instanceof Map);
            
            @SuppressWarnings("unchecked")
            Map<Long, Integer> map = (Map<Long, Integer>) result.get(0);
            assertEquals(1, map.get(100L));
        }

        @Test
        void testAddDelayWithEmptyList() {
            List<Object> input = new ArrayList<>();
            
            List<Object> result = ApiMetricsDelayUtil.addDelay(input, 100L);
            
            assertSame(input, result);
            assertEquals(1, result.size());
            assertTrue(result.get(0) instanceof Map);
        }

        @Test
        void testAddDelayWithExistingMapEntry() {
            Map<Object, Integer> existingMap = new HashMap<>();
            existingMap.put(100L, 5);
            
            List<Object> input = new ArrayList<>();
            input.add(existingMap);
            
            List<Object> result = ApiMetricsDelayUtil.addDelay(input, 100L);
            
            assertSame(input, result);
            assertEquals(1, result.size());
            assertEquals(6, existingMap.get(100L)); // Incremented from 5 to 6
        }

        @Test
        void testAddDelayWithExistingNumberEntry() {
            List<Object> input = new ArrayList<>();
            input.add(100L);
            
            List<Object> result = ApiMetricsDelayUtil.addDelay(input, 100L);
            
            assertSame(input, result);
            assertEquals(1, result.size());
            assertTrue(result.get(0) instanceof Map);
            
            @SuppressWarnings("unchecked")
            Map<Long, Integer> map = (Map<Long, Integer>) result.get(0);
            assertEquals(2, map.get(100L)); // Number converted to map with count 2
        }

        @Test
        void testAddDelayWithNonMatchingEntries() {
            Map<Object, Integer> existingMap = new HashMap<>();
            existingMap.put(200L, 5);
            
            List<Object> input = new ArrayList<>();
            input.add(existingMap);
            input.add(300L);
            
            List<Object> result = ApiMetricsDelayUtil.addDelay(input, 100L);
            
            assertSame(input, result);
            assertEquals(3, result.size()); // Original 2 + new entry
            assertTrue(result.get(2) instanceof Map);
            
            @SuppressWarnings("unchecked")
            Map<Long, Integer> newMap = (Map<Long, Integer>) result.get(2);
            assertEquals(1, newMap.get(100L));
        }

        @Test
        void testAddDelayWithMixedTypes() {
            List<Object> input = new ArrayList<>();
            input.add("not a map or number");
            input.add(200L); // Different number
            
            List<Object> result = ApiMetricsDelayUtil.addDelay(input, 100L);
            
            assertSame(input, result);
            assertEquals(3, result.size()); // Original 2 + new entry
        }
    }

    @Nested
    class ToMapTest {
        @Test
        void testToMapWithValidKeyValue() {
            Map<String, Integer> result = ApiMetricsDelayUtil.toMap("key", 42);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(42, result.get("key"));
        }

        @Test
        void testToMapWithNullKey() {
            Map<String, Integer> result = ApiMetricsDelayUtil.toMap(null, 42);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(42, result.get(null));
        }

        @Test
        void testToMapWithNullValue() {
            Map<String, Integer> result = ApiMetricsDelayUtil.toMap("key", null);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertNull(result.get("key"));
        }

        @Test
        void testToMapWithDifferentTypes() {
            Map<Long, Double> result = ApiMetricsDelayUtil.toMap(100L, 3.14);
            
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(3.14, result.get(100L));
        }
    }

    @Nested
    class LoggingTest {
        @Test
        void testLoggingInEachMethod() {
            // Test that logging occurs for invalid inputs
            List<Map<Long, Integer>> result = new ArrayList<>();
            
            // This should trigger logging
            ApiMetricsDelayUtil.each("invalid", 5, result);
            ApiMetricsDelayUtil.each("100", "invalid", result);
            ApiMetricsDelayUtil.each(100L, "invalid", result);
            ApiMetricsDelayUtil.each("invalid", "invalid", result);
            ApiMetricsDelayUtil.each(new Object(), new Object(), result);
            
            // All should result in empty result due to invalid inputs
            assertTrue(result.isEmpty());
        }
    }
}