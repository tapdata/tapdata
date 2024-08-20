package com.tapdata.tm.utils;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionTableUtilTest {

    @Nested
    class checkIsMasterPartitionTable {
        TapTable table;
        @BeforeEach
        void init() {
            table = mock(TapTable.class);
        }

        @Test
        void testIsPartition1() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn("id");
            when(table.getId()).thenReturn("id");
            Assertions.assertTrue(PartitionTableUtil.checkIsMasterPartitionTable(table));
        }
        @Test
        void testIsPartition2() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn(null);
            when(table.getId()).thenReturn("id");
            Assertions.assertTrue(PartitionTableUtil.checkIsMasterPartitionTable(table));
        }

        @Test
        void testNotPartition1() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn("id");
            when(table.getId()).thenReturn("id1");
            Assertions.assertFalse(PartitionTableUtil.checkIsMasterPartitionTable(table));
        }
        @Test
        void testNotPartition2() {
            when(table.getPartitionInfo()).thenReturn(null);
            when(table.getPartitionMasterTableId()).thenReturn(null);
            when(table.getId()).thenReturn("id");
            Assertions.assertFalse(PartitionTableUtil.checkIsMasterPartitionTable(table));
        }
    }

    @Nested
    class checkIsSubPartitionTable {
        TapTable table;
        @BeforeEach
        void init() {
            table = mock(TapTable.class);
        }

        @Test
        void testIsPartition1() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn("id");
            when(table.getId()).thenReturn("1id");
            Assertions.assertTrue(PartitionTableUtil.checkIsSubPartitionTable(table));
        }
        @Test
        void testNotPartition1() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn(null);
            when(table.getId()).thenReturn("id");
            Assertions.assertFalse(PartitionTableUtil.checkIsSubPartitionTable(table));
        }

        @Test
        void testNotPartition2() {
            when(table.getPartitionInfo()).thenReturn(mock(TapPartition.class));
            when(table.getPartitionMasterTableId()).thenReturn("id");
            when(table.getId()).thenReturn("id");
            Assertions.assertFalse(PartitionTableUtil.checkIsSubPartitionTable(table));
        }

        @Test
        void testNotPartition3() {
            when(table.getPartitionInfo()).thenReturn(null);
            when(table.getPartitionMasterTableId()).thenReturn("id");
            when(table.getId()).thenReturn("id");
            Assertions.assertFalse(PartitionTableUtil.checkIsSubPartitionTable(table));
        }
    }
}