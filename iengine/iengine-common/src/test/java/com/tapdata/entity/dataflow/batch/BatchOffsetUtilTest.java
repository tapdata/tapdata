package com.tapdata.entity.dataflow.batch;


import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffset;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BatchOffsetUtilTest {
    SyncProgress syncProgress;
    String tableId;
    Map<String, Object> batchOffsetObj;
    BatchOffset batchOffset;

    @BeforeEach
    void init() {
        syncProgress = mock(SyncProgress.class);

        tableId = "id";
        batchOffset = new BatchOffset();
        batchOffset.setStatus(TableBatchReadStatus.OVER.name());
        batchOffset.setOffset(0L);
        batchOffset = new BatchOffset(0L, TableBatchReadStatus.OVER.name());
        batchOffsetObj = mock(Map.class);
        when(syncProgress.getBatchOffsetObj()).thenReturn(batchOffsetObj);
    }

    @Test
    void testParams() {
        Assertions.assertEquals("OVER", TableBatchReadStatus.OVER.name());
        Assertions.assertEquals("RUNNING", TableBatchReadStatus.RUNNING.name());
    }

    @Nested
    @DisplayName("method batchIsOverOfTable test")
    class BatchIsOverOfTableTest {

        @Test
        void testNormal() {
            try (MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenCallRealMethod();
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenReturn(batchOffset);
                Assertions.assertTrue(BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId));
            }
        }

        @Test
        void testNotIsBatchOffsetMap() {
            try (MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenCallRealMethod();
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenReturn(batchOffsetObj);
                Assertions.assertFalse(BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId));
            }
        }
    }

    @Nested
    @DisplayName("method getTableOffsetInfo test")
    class GetTableOffsetInfoTest {

        @BeforeEach
        void init() {
            when(batchOffsetObj.get(tableId)).thenReturn(0);
        }

        @Test
        void testNormal() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(batchOffsetObj);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId));
                verify(batchOffsetObj, times(1)).get(tableId);
            }
        }

        @Test
        void testBatchOffsetObjNotMap() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(0);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId));
                verify(batchOffsetObj, times(0)).get(tableId);
            }
        }
    }

    @Nested
    @DisplayName("method getBatchOffsetOfTable test")
    class GetBatchOffsetOfTableTest {
        @Test
        void testNormal() {
            try (MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenReturn(batchOffset);
                bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId));
            }
        }

        @Test
        void testNotIsBatchOffsetMap() {
            try (MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.getTableOffsetInfo(syncProgress, tableId)).thenReturn(batchOffsetObj);
                bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId));
            }
        }
    }

    @Nested
    @DisplayName("method updateBatchOffset test")
    class UpdateBatchOffsetTest {
        Map<String, Object> batchOffsetObjTemp;
        BatchOffset tableOffsetObjTemp;
        Object offset;
        String isOverTag;
        @BeforeEach
        void init() {
            offset = mock(Object.class);
            isOverTag = TableBatchReadStatus.OVER.name();
            batchOffsetObjTemp = mock(Map.class);

            tableOffsetObjTemp = mock(BatchOffset.class);
            when(syncProgress.getBatchOffsetObj()).thenReturn(batchOffsetObjTemp);
            doNothing().when(tableOffsetObjTemp).setStatus(isOverTag);
            doNothing().when(tableOffsetObjTemp).setOffset(offset);
        }

        @Test
        void testNormal() {
            when(batchOffsetObjTemp.computeIfAbsent(anyString(), any())).thenAnswer(a -> {
                String key = a.getArgument(0, String.class);
                Function argument = a.getArgument(1, Function.class);
                Object apply = argument.apply(key);
                return tableOffsetObjTemp;
            });
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObjTemp, times(1)).computeIfAbsent(anyString(), any());
            }
        }

        @Test
        void offsetNotMap() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(0);
            when(batchOffsetObjTemp.computeIfAbsent(anyString(), any())).thenAnswer(a -> {
                String key = a.getArgument(0, String.class);
                Function argument = a.getArgument(1, Function.class);
                Object apply = argument.apply(key);
                return tableOffsetObjTemp;
            });
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObjTemp, times(0)).computeIfAbsent(anyString(), any());
            }
        }

        @Test
        void testTableOffsetNotBatchOffset() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(batchOffsetObjTemp);
            when(batchOffsetObjTemp.computeIfAbsent(anyString(), any())).thenReturn(0);
            when(batchOffsetObjTemp.put(anyString(), any(BatchOffset.class))).thenReturn(0);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, offset, isOverTag));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObjTemp, times(1)).computeIfAbsent(anyString(), any());
                verify(batchOffsetObjTemp, times(1)).put(anyString(), any(BatchOffset.class));
            }
        }
    }

    @Nested
    class TableUpdateNameTest {
        String oldName, newName;
        @BeforeEach
        void init() {
            oldName = "oldName";
            newName = "newName";

            when(batchOffsetObj.containsKey(oldName)).thenReturn(true);
            when(batchOffsetObj.get(oldName)).thenReturn(0);
            when(batchOffsetObj.remove(oldName)).thenReturn(0);
            when(batchOffsetObj.put(newName, 0)).thenReturn(0);
        }
        @Test
        void testNormal() {
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObj, times(1)).containsKey(oldName);
                verify(batchOffsetObj, times(1)).get(oldName);
                verify(batchOffsetObj, times(1)).remove(oldName);
                verify(batchOffsetObj, times(1)).put(newName, 0);
            }
        }
        @Test
        void testOffsetNotMap() {
            when(syncProgress.getBatchOffsetObj()).thenReturn(0);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObj, times(0)).containsKey(oldName);
                verify(batchOffsetObj, times(0)).get(oldName);
                verify(batchOffsetObj, times(0)).remove(oldName);
                verify(batchOffsetObj, times(0)).put(newName, 0);
            }
        }
        @Test
        void testOffsetNotContainsKey() {
            when(batchOffsetObj.containsKey(oldName)).thenReturn(false);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.tableUpdateName(syncProgress, oldName, newName));
                verify(syncProgress, times(1)).getBatchOffsetObj();
                verify(batchOffsetObj, times(1)).containsKey(oldName);
                verify(batchOffsetObj, times(0)).get(oldName);
                verify(batchOffsetObj, times(0)).remove(oldName);
                verify(batchOffsetObj, times(0)).put(newName, 0);
            }
        }
    }

    @Nested
    class UpdateBatchOffsetWhenTableRenameTest {
        TapRenameTableEvent tapRenameTableEvent;
        List<ValueChange<String>> nameChanges;
        ValueChange<String> valueChange;
        @BeforeEach
        void init() {
            tapRenameTableEvent = mock(TapRenameTableEvent.class);
            nameChanges = new ArrayList<>();
            valueChange = mock(ValueChange.class);
            nameChanges.add(valueChange);
            when(valueChange.getBefore()).thenReturn("before");
            when(valueChange.getAfter()).thenReturn("after");

            when(tapRenameTableEvent.getNameChanges()).thenReturn(nameChanges);
        }
        @Test
        void testNormal() {
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.tableUpdateName(syncProgress, "before", "after")).thenAnswer(a->null);
                bou.when(() -> BatchOffsetUtil.updateBatchOffsetWhenTableRename(syncProgress, tapRenameTableEvent)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.updateBatchOffsetWhenTableRename(syncProgress, tapRenameTableEvent));
                verify(tapRenameTableEvent, times(1)).getNameChanges();
                verify(valueChange, times(1)).getAfter();
                verify(valueChange, times(1)).getBefore();
            }
        }
        @Test
        void testEventNotTapRenameTableEvent() {
            TapAlterFieldNameEvent mock = mock(TapAlterFieldNameEvent.class);
            try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
                bou.when(() -> BatchOffsetUtil.tableUpdateName(syncProgress, "before", "after")).thenAnswer(a->null);
                bou.when(() -> BatchOffsetUtil.updateBatchOffsetWhenTableRename(syncProgress, mock)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> BatchOffsetUtil.updateBatchOffsetWhenTableRename(syncProgress, mock));
                verify(tapRenameTableEvent, times(0)).getNameChanges();
                verify(valueChange, times(0)).getAfter();
                verify(valueChange, times(0)).getBefore();
            }
        }

    }
}