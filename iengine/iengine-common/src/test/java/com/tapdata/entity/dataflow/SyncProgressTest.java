package com.tapdata.entity.dataflow;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SyncProgressTest {
    SyncProgress syncProgress;
    @BeforeEach
    void init() {
        syncProgress = mock(SyncProgress.class);
    }

    @Test
    void testParams() {
        Assertions.assertEquals("task_batch_table_offset_point", SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT);
        Assertions.assertEquals("task_batch_table_offset_status", SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS);
        Assertions.assertEquals("over", TableBatchReadStatus.OVER.name());
        Assertions.assertEquals("running", TableBatchReadStatus.RUNNING.name());
    }

    @Nested
    @DisplayName("test all method which about batch offset")
    class BatchOffsetTest {
        String tableId;
        Map<String, Object> batchOffsetObj;
        Map<String, Object> batchOffset;
        @BeforeEach
        void init() {
            tableId = "id";
            batchOffsetObj = mock(Map.class);
            batchOffset = mock(Map.class);
            ReflectionTestUtils.setField(syncProgress, "batchOffsetObj", batchOffsetObj);
        }

        @Nested
        @DisplayName("method batchIsOverOfTable test")
        class BatchIsOverOfTableTest {
            @BeforeEach
            void init() {
                when(syncProgress.batchIsOverOfTable(tableId)).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                when(syncProgress.getTableOffsetInfo(tableId)).thenReturn(batchOffset);
                when(syncProgress.isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS)).thenReturn(true);
                when(batchOffset.get(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS)).thenReturn(TableBatchReadStatus.OVER.name());

                Assertions.assertTrue(syncProgress.batchIsOverOfTable(tableId));
                verify(syncProgress, times(1)).getTableOffsetInfo(tableId);
                verify(syncProgress, times(1)).isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS);
                verify(batchOffset, times(1)).get(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS);
            }

            @Test
            void testNotIsBatchOffsetMap() {
                when(syncProgress.getTableOffsetInfo(tableId)).thenReturn(batchOffset);
                when(syncProgress.isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS)).thenReturn(false);
                when(batchOffset.get(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS)).thenReturn(TableBatchReadStatus.OVER.name());

                Assertions.assertFalse(syncProgress.batchIsOverOfTable(tableId));
                verify(syncProgress, times(1)).getTableOffsetInfo(tableId);
                verify(syncProgress, times(1)).isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS);
                verify(batchOffset, times(0)).get(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS);
            }
        }

        @Nested
        @DisplayName("method getTableOffsetInfo test")
        class GetTableOffsetInfoTest {
            @BeforeEach
            void init() {
                when(syncProgress.getTableOffsetInfo(tableId)).thenCallRealMethod();
                when(batchOffsetObj.get(tableId)).thenReturn(0);
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> syncProgress.getTableOffsetInfo(tableId));
                verify(batchOffsetObj, times(1)).get(tableId);
            }

            @Test
            void testBatchOffsetObjNotMap() {
                ReflectionTestUtils.setField(syncProgress, "batchOffsetObj", mock(List.class));
                Assertions.assertDoesNotThrow(() -> syncProgress.getTableOffsetInfo(tableId));
                verify(batchOffsetObj, times(0)).get(tableId);
            }
        }

        @Nested
        @DisplayName("method isBatchOffsetMap test")
        class IsBatchOffsetMapTest {
            String key;
            @BeforeEach
            void init() {
                key = "key";
                when(syncProgress.isBatchOffsetMap(batchOffset, key)).thenCallRealMethod();
                when(batchOffset.containsKey(key)).thenReturn(true);
            }

            @Test
            void testNormal() {
                Assertions.assertTrue(syncProgress.isBatchOffsetMap(batchOffset, key));
                verify(batchOffset, times(1)).containsKey(key);
            }

            @Test
            void testNotContainsKey() {
                when(batchOffset.containsKey(key)).thenReturn(false);
                Assertions.assertFalse(syncProgress.isBatchOffsetMap(batchOffset, key));
                verify(batchOffset, times(1)).containsKey(key);
            }

            @Test
            void testBatchOffsetNotMap() {
                List mock = mock(List.class);
                when(syncProgress.isBatchOffsetMap(mock, key)).thenCallRealMethod();
                Assertions.assertFalse(syncProgress.isBatchOffsetMap(mock, key));
                verify(batchOffsetObj, times(0)).containsKey(key);
            }
        }

        @Nested
        @DisplayName("method getBatchOffsetOfTable test")
        class GetBatchOffsetOfTableTest {
            @BeforeEach
            void init() {
                when(syncProgress.getBatchOffsetOfTable(tableId)).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                when(syncProgress.getTableOffsetInfo(tableId)).thenReturn(batchOffset);
                when(syncProgress.isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT)).thenReturn(true);
                when(batchOffset.get(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT)).thenReturn(TableBatchReadStatus.OVER.name());

                Assertions.assertDoesNotThrow(() -> syncProgress.getBatchOffsetOfTable(tableId));
                verify(syncProgress, times(1)).getTableOffsetInfo(tableId);
                verify(syncProgress, times(1)).isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT);
                verify(batchOffset, times(1)).get(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT);
            }

            @Test
            void testNotIsBatchOffsetMap() {
                when(syncProgress.getTableOffsetInfo(tableId)).thenReturn(batchOffset);
                when(syncProgress.isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT)).thenReturn(false);
                when(batchOffset.get(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT)).thenReturn(TableBatchReadStatus.OVER.name());

                Assertions.assertDoesNotThrow(() -> syncProgress.getBatchOffsetOfTable(tableId));
                verify(syncProgress, times(1)).getTableOffsetInfo(tableId);
                verify(syncProgress, times(1)).isBatchOffsetMap(batchOffset, SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT);
                verify(batchOffset, times(0)).get(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT);
            }
        }

        @Nested
        @DisplayName("method updateBatchOffset test")
        class UpdateBatchOffsetTest {
            Map<String, Object> batchOffsetObjTemp;
            Map<String, Object> tableOffsetObjTemp;
            Object offset;
            String isOverTag;
            @BeforeEach
            void init() {
                offset = mock(Object.class);
                isOverTag = TableBatchReadStatus.OVER.name();
                batchOffsetObjTemp = mock(Map.class);
                tableOffsetObjTemp = mock(Map.class);
                when(syncProgress.putIfAbsentBatchOffsetObj()).thenReturn(batchOffsetObjTemp);
                when(batchOffsetObjTemp.computeIfAbsent(anyString(), any())).thenAnswer(a -> {
                    String key = a.getArgument(0, String.class);
                    Function argument = a.getArgument(1, Function.class);
                    Object apply = argument.apply(key);
                    return tableOffsetObjTemp;
                });
                when(tableOffsetObjTemp.put(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT, offset)).thenReturn(offset);
                when(tableOffsetObjTemp.put(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS, isOverTag)).thenReturn(isOverTag);
                doCallRealMethod().when(syncProgress).updateBatchOffset(tableId, offset, isOverTag);
            }
            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> syncProgress.updateBatchOffset(tableId, offset, isOverTag));
                verify(syncProgress, times(1)).putIfAbsentBatchOffsetObj();
                verify(batchOffsetObjTemp, times(1)).computeIfAbsent(anyString(), any());
                verify(tableOffsetObjTemp, times(1)).put(SyncProgress.TASK_BATCH_TABLE_OFFSET_POINT, offset);
                verify(tableOffsetObjTemp, times(1)).put(SyncProgress.TASK_BATCH_TABLE_OFFSET_STATUS, isOverTag);
            }
        }

        @Nested
        @DisplayName("method putIfAbsentBatchOffsetObj test")
        class PutIfAbsentBatchOffsetObjTest {
            @BeforeEach
            void init() {
                doNothing().when(syncProgress).setBatchOffsetObj(any(Map.class));
                when(syncProgress.putIfAbsentBatchOffsetObj()).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(syncProgress::putIfAbsentBatchOffsetObj);
                verify(syncProgress, times(0)).setBatchOffsetObj(any(Map.class));
            }

            @Test
            void testBatchOffsetObj() {
                ReflectionTestUtils.setField(syncProgress, "batchOffsetObj", null);
                Assertions.assertDoesNotThrow(syncProgress::putIfAbsentBatchOffsetObj);
                verify(syncProgress, times(1)).setBatchOffsetObj(any(Map.class));
            }
        }
    }
}