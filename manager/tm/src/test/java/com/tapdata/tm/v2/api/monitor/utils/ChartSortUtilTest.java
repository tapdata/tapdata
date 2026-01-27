package com.tapdata.tm.v2.api.monitor.utils;

import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ChartSortUtilTest {

    // Mock implementation of ValueBase.Item for testing
    static class TestItem extends ValueBase.Item {
        private long ts;
        
        public TestItem(long ts) {
            this.ts = ts;
        }
        
        @Override
        public long getTs() {
            return ts;
        }
        
        public void setTs(long ts) {
            this.ts = ts;
        }
    }

    @Nested
    class FixAndSortTest {
        @Test
        void testFixAndSortWithGranularity0() {
            Map<Long, TestItem> items = new HashMap<>();
            items.put(1000L, new TestItem(1000L));
            items.put(1010L, new TestItem(1010L));
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items, 1000L, 1020L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // With granularity 0, step = 5, so we should have timestamps: 1000, 1005, 1010, 1015
            assertEquals(4, items.size());
            assertTrue(items.containsKey(1000L));
            assertTrue(items.containsKey(1005L));
            assertTrue(items.containsKey(1010L));
            assertTrue(items.containsKey(1015L));
            
            // Verify mapping was called for each item in sorted order
            ArgumentCaptor<TestItem> captor = ArgumentCaptor.forClass(TestItem.class);
            verify(mapping, times(4)).accept(captor.capture());
            
            List<TestItem> capturedItems = captor.getAllValues();
            // Should be sorted by timestamp
            for (int i = 1; i < capturedItems.size(); i++) {
                assertTrue(capturedItems.get(i-1).getTs() <= capturedItems.get(i).getTs());
            }
        }

        @Test
        void testFixAndSortWithGranularity2() {
            Map<Long, TestItem> items = new HashMap<>();
            items.put(3600L, new TestItem(3600L)); // 1 hour

            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            // Test with timestamps that need alignment
            long tsFrom = 3500L; // Not aligned to hour boundary
            long tsEnd = 10900L;  // Not aligned to hour boundary
            
            ChartSortUtil.fixAndSort(items, tsFrom, tsEnd, TimeGranularity.HOUR, emptyGetter, mapping);
            
            // With granularity 2, step = 3600 (1 hour)
            // tsFrom should be aligned to 3600 (3500 / 3600 * 3600 = 0, but we start from 3600)
            // tsEnd should be aligned to 14400 ((10900 / 3600 + 1) * 3600 = 14400)
            assertTrue(items.size() >= 3); // Should have 3600, 7200, 10800, 14400
            assertTrue(items.containsKey(3600L));
            assertTrue(items.containsKey(7200L));
            assertTrue(items.containsKey(10800L));
            
            verify(mapping, atLeast(3)).accept(any(TestItem.class));
        }

        @Test
        void testFixAndSortWithGranularity2AndAlignment() {
            Map<Long, TestItem> items = new HashMap<>();

            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            // Test with timestamps that need alignment
            long tsFrom = 1800L; // 0.5 hour, should align to 0
            long tsEnd = 5400L;  // 1.5 hours, should align to 7200 (2 hours)
            
            ChartSortUtil.fixAndSort(items, tsFrom, tsEnd, TimeGranularity.HOUR, emptyGetter, mapping);
            
            // tsFrom should be aligned to 0 (1800 / 3600 * 3600 = 0)
            // tsEnd should be aligned to 7200 ((5400 / 3600 + 1) * 3600 = 7200)
            assertTrue(items.containsKey(0L));
            assertTrue(items.containsKey(3600L));
            assertFalse(items.containsKey(7200L));
            
            verify(mapping, times(2)).accept(any(TestItem.class));
        }

        @Test
        void testFixAndSortWithExistingItems() {
            Map<Long, TestItem> items = new HashMap<>();
            TestItem existingItem = new TestItem(1000L);
            items.put(1000L, existingItem);
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items, 1000L, 1010L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Should not replace existing item
            assertSame(existingItem, items.get(1000L));
            
            // Should add missing timestamps
            assertTrue(items.containsKey(1005L));
            
            verify(emptyGetter, times(1)).apply(1005L); // Only called for missing timestamp
            verify(mapping, times(2)).accept(any(TestItem.class));
        }

        @Test
        void testFixAndSortWithEmptyItems() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items,  1000L, 1015L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Should create all missing items
            assertEquals(3, items.size()); // 1000, 1005, 1010
            assertTrue(items.containsKey(1000L));
            assertTrue(items.containsKey(1005L));
            assertTrue(items.containsKey(1010L));
            
            verify(emptyGetter, times(3)).apply(anyLong());
            verify(mapping, times(3)).accept(any(TestItem.class));
        }

        @Test
        void testFixAndSortWithSameStartAndEnd() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items,  1000L, 1000L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Should not create any items since tsFrom >= tsEnd
            assertTrue(items.isEmpty());
            
            verifyNoInteractions(emptyGetter);
            verifyNoInteractions(mapping);
        }

        @Test
        void testFixAndSortWithReverseRange() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            ChartSortUtil.fixAndSort(items,  1010L, 1000L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Should not create any items since tsFrom > tsEnd
            assertTrue(items.isEmpty());
            
            verifyNoInteractions(emptyGetter);
            verifyNoInteractions(mapping);
        }

        @Test
        void testFixAndSortSortsCorrectly() {
            Map<Long, TestItem> items = new HashMap<>();
            items.put(1010L, new TestItem(1010L));
            items.put(1000L, new TestItem(1000L));
            items.put(1005L, new TestItem(1005L));
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            ChartSortUtil.fixAndSort(items,  1000L, 1015L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Verify items are passed to mapping in sorted order
            ArgumentCaptor<TestItem> captor = ArgumentCaptor.forClass(TestItem.class);
            verify(mapping, times(3)).accept(captor.capture());
            
            List<TestItem> capturedItems = captor.getAllValues();
            assertEquals(1000L, capturedItems.get(0).getTs());
            assertEquals(1005L, capturedItems.get(1).getTs());
            assertEquals(1010L, capturedItems.get(2).getTs());
        }

        @Test
        void testFixAndSortWithLargeRange() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            // Test with a larger range to ensure performance is acceptable
            ChartSortUtil.fixAndSort(items, 0L, 300L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            // Should create 60 items (0, 5, 10, ..., 295)
            assertEquals(60, items.size());
            
            verify(emptyGetter, times(60)).apply(anyLong());
            verify(mapping, times(60)).accept(any(TestItem.class));
        }

        @Test
        void testFixAndSortWithNullEmptyGetter() {
            Map<Long, TestItem> items = new HashMap<>();
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            // Should handle null emptyGetter gracefully
            assertThrows(NullPointerException.class, () -> {
                ChartSortUtil.fixAndSort(items,  1000L, 1010L, TimeGranularity.SECOND_FIVE, null, mapping);
            });
        }

        @Test
        void testFixAndSortWithNullMapping() {
            Map<Long, TestItem> items = new HashMap<>();
            items.put(1000L, new TestItem(1000L));
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            
            // Should handle null mapping gracefully
            assertThrows(NullPointerException.class, () -> {
                ChartSortUtil.fixAndSort(items, 1000L, 1010L, TimeGranularity.SECOND_FIVE, emptyGetter, null);
            });
        }
    }

    @Nested
    class EdgeCasesTest {
        @Test
        void testWithZeroTimestamps() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items,  0L, 10L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            assertTrue(items.containsKey(0L));
            assertTrue(items.containsKey(5L));
            
            verify(mapping, times(2)).accept(any(TestItem.class));
        }

        @Test
        void testWithNegativeTimestamps() {
            Map<Long, TestItem> items = new HashMap<>();
            
            LongFunction<TestItem> emptyGetter = mock(LongFunction.class);
            Consumer<TestItem> mapping = mock(Consumer.class);
            
            when(emptyGetter.apply(anyLong())).thenAnswer(invocation -> 
                new TestItem(invocation.getArgument(0)));
            
            ChartSortUtil.fixAndSort(items,  -10L, 0L, TimeGranularity.SECOND_FIVE, emptyGetter, mapping);
            
            assertTrue(items.containsKey(-10L));
            assertTrue(items.containsKey(-5L));
            
            verify(mapping, times(2)).accept(any(TestItem.class));
        }
    }
}