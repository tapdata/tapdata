package io.tapdata.flow.engine.V2.monitor.impl;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import io.tapdata.pdk.apis.functions.connector.source.QueryPartitionTablesByParentName;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableMonitorTest {
    TableMonitor tableMonitor;

    TapTableMap<String, TapTable> tapTableMap;
    Predicate<String> dynamicTableFilter;
    TableMonitor.TableResult tableResult;
    Set<String> removeTables;

    @BeforeEach
    void init() {
        tableMonitor = mock(TableMonitor.class);
        tapTableMap = mock(TapTableMap.class);
        dynamicTableFilter = mock(Predicate.class);
        tableResult = mock(TableMonitor.TableResult.class);
        removeTables = new HashSet<>();
        ReflectionTestUtils.setField(tableMonitor, "tapTableMap", tapTableMap);
        ReflectionTestUtils.setField(tableMonitor, "dynamicTableFilter", dynamicTableFilter);
        ReflectionTestUtils.setField(tableMonitor, "tableResult", tableResult);
        ReflectionTestUtils.setField(tableMonitor, "removeTables", removeTables);
    }

    @Nested
    class WithSyncSourcePartitionTableEnableTest {
        @Test
        void testNormal() {
            when(tableMonitor.withSyncSourcePartitionTableEnable(anyBoolean())).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> tableMonitor.withSyncSourcePartitionTableEnable(true));
        }
    }

    @Nested
    class PartitionTableInfoSetTest {
        Set<String> masterTables;
        Set<String> existsSubTable;
        Map<String, Set<String>> parentTableAndSubIdMap;
        Iterator<Entry<TapTable>> iterator;

        Entry<TapTable> next;

        TapTable table;
        TapPartition partition;
        List<TapSubPartitionTableInfo> subPartitionTableInfoList;
        TapSubPartitionTableInfo info;
        @BeforeEach
        void init() {
            iterator = mock(Iterator.class);
            masterTables = new HashSet<>();
            existsSubTable = new HashSet<>();
            parentTableAndSubIdMap = new HashMap<>();
            partition = mock(TapPartition.class);
            next = mock(Entry.class);
            subPartitionTableInfoList = new ArrayList<>();
            info = mock(TapSubPartitionTableInfo.class);
            when(info.getTableName()).thenReturn("name");
            subPartitionTableInfoList.add(info);
            subPartitionTableInfoList.add(null);

            table = mock(TapTable.class);

            when(next.getKey()).thenReturn("id");
            when(next.getValue()).thenReturn(table);

            when(table.getPartitionInfo()).thenReturn(partition);
            when(partition.getSubPartitionTableInfo()).thenReturn(subPartitionTableInfoList);
            when(tapTableMap.iterator()).thenReturn(iterator);
            when(iterator.hasNext()).thenReturn(true, false);
            when(iterator.next()).thenReturn(next);
            when(tableMonitor.partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap)).thenCallRealMethod();
        }

        @Test
        void testNotMasterTable() {
            when(table.checkIsMasterPartitionTable()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> tableMonitor.partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap));

        }
        @Test
        void testMasterTablePartitionInfoIsNull() {
            when(table.getPartitionInfo()).thenReturn(null);
            when(table.checkIsMasterPartitionTable()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> tableMonitor.partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap));
        }
        @Test
        void testSubPartitionTableInfoHasNull() {
            when(table.checkIsMasterPartitionTable()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> tableMonitor.partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap));
        }
    }

    @Nested
    class FilterIfTableNeedRemoveTest {
        List<String> finalTapTableNames;
        Set<String> masterTables;
        Set<String> existsSubTable;
        String dbTableName;
        @BeforeEach
        void init() {
            finalTapTableNames = mock(List.class);
            masterTables = mock(Set.class);
            existsSubTable = mock(Set.class);
            dbTableName = "id";
            when(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenCallRealMethod();
        }

        @Test
        void testNotFilterPartitionTableIfNeedRemove() {
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(false);
            when(dynamicTableFilter.test(dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testTestOK() {
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(false);
            when(dynamicTableFilter.test(dbTableName)).thenReturn(true);
            Assertions.assertFalse(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testIsFilterPartitionTableIfNeedRemove() {
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(true);
            when(dynamicTableFilter.test(dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
    }

    @Nested
    class FilterPartitionTableIfNeedRemoveTest {
        List<String> finalTapTableNames;
        Set<String> masterTables;
        Set<String> existsSubTable;
        String dbTableName;
        @BeforeEach
        void init() {
            finalTapTableNames = mock(List.class);
            masterTables = mock(Set.class);
            existsSubTable = mock(Set.class);
            dbTableName = "id";
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenCallRealMethod();
        }
        @Test
        void testFinalTapTableNamesNotContains() {
            when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
            when(existsSubTable.contains(dbTableName)).thenReturn(false);
            when(masterTables.contains(dbTableName)).thenReturn(false);
            Assertions.assertFalse(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testFinalTapTableNamesContains() {
            when(finalTapTableNames.contains(dbTableName)).thenReturn(true);
            when(existsSubTable.contains(dbTableName)).thenReturn(false);
            when(masterTables.contains(dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testExistsSubTableContains() {
            when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
            when(existsSubTable.contains(dbTableName)).thenReturn(true);
            when(masterTables.contains(dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testMasterTablesContains() {
            when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
            when(existsSubTable.contains(dbTableName)).thenReturn(false);
            when(masterTables.contains(dbTableName)).thenReturn(true);
            Assertions.assertFalse(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
        @Test
        void testSyncSourcePartitionTableEnableIsFalse() {
            try {
                when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
                when(existsSubTable.contains(dbTableName)).thenReturn(false);
                when(masterTables.contains(dbTableName)).thenReturn(true);
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", false);
                Assertions.assertTrue(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            } finally {
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", null);
            }
        }
        @Test
        void testSyncSourcePartitionTableEnableIsTrue() {
            try {
                when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
                when(existsSubTable.contains(dbTableName)).thenReturn(false);
                when(masterTables.contains(dbTableName)).thenReturn(true);
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", true);
                Assertions.assertFalse(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            } finally {
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", null);
            }
        }
        @Test
        void testSyncSourcePartitionTableEnableIsFalseButMasterNotContains() {
            try {
                when(finalTapTableNames.contains(dbTableName)).thenReturn(false);
                when(existsSubTable.contains(dbTableName)).thenReturn(false);
                when(masterTables.contains(dbTableName)).thenReturn(false);
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", false);
                Assertions.assertFalse(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            } finally {
                ReflectionTestUtils.setField(tableMonitor, "syncSourcePartitionTableEnable", null);
            }
        }
    }

    @Nested
    class FilterTableTest {
        List<String> finalTapTableNames;
        Set<String> masterTables;
        Set<String> existsSubTable;
        String dbTableName;

        @BeforeEach
        void init() {
            finalTapTableNames = mock(List.class);
            masterTables = mock(Set.class);
            existsSubTable = mock(Set.class);
            dbTableName = "id";
            when(tableMonitor.filterTable(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenCallRealMethod();

            when(finalTapTableNames.remove(dbTableName)).thenReturn(true);
            when(tableResult.add(dbTableName)).thenReturn(tableResult);
        }

        @Test
        void testFilterIfTableNeedRemove() {
            when(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(true);
            Assertions.assertFalse(tableMonitor.filterTable(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            verify(tableResult, times(0)).add(dbTableName);
        }
        @Test
        void testNotFilterIfTableNeedRemove() {
            when(tableMonitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterTable(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            verify(tableResult, times(1)).add(dbTableName);
        }
    }

    @Nested
    class FilterPartitionTableTest {
        List<String> finalTapTableNames;
        Set<String> masterTables;
        Set<String> existsSubTable;
        String dbTableName;
        @BeforeEach
        void init() {
            finalTapTableNames = mock(List.class);
            masterTables = mock(Set.class);
            existsSubTable = mock(Set.class);
            dbTableName = "id";
            when(tableMonitor.filterPartitionTable(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenCallRealMethod();

            when(finalTapTableNames.remove(dbTableName)).thenReturn(true);
            when(tableResult.add(dbTableName)).thenReturn(tableResult);
        }
        @Test
        void testFilterIfTableNeedRemove() {
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(true);
            Assertions.assertFalse(tableMonitor.filterPartitionTable(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            verify(tableResult, times(0)).add(dbTableName);
        }
        @Test
        void testNotFilterIfTableNeedRemove() {
            when(tableMonitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(false);
            Assertions.assertTrue(tableMonitor.filterPartitionTable(finalTapTableNames, masterTables, existsSubTable, dbTableName));
            verify(tableResult, times(1)).add(dbTableName);
        }
    }

    @Nested
    class testLoadSubTableByPartitionTableTest {
        ConnectorNode connectorNode;
        List<TapTable> masterTapTables;
        Map<String, Set<String>> parentTableAndSubIdMap;
        List<String> finalTapTableNames;
        Set<String> existsSubTable;

        ConnectorFunctions connectorFunctions;
        QueryPartitionTablesByParentName function;
        TapConnectorContext connectorContext;
        Collection<TapPartitionResult> results;
        @BeforeEach
        void init() throws Exception {
            results = new ArrayList<>();
            connectorContext = mock(TapConnectorContext.class);
            connectorNode = mock(ConnectorNode.class);
            masterTapTables = new ArrayList<>();
            parentTableAndSubIdMap = new HashMap<>();
            finalTapTableNames = new ArrayList<>();
            existsSubTable = new HashSet<>();
            connectorFunctions = mock(ConnectorFunctions.class);
            function = mock(QueryPartitionTablesByParentName.class);
            when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
            when(connectorFunctions.getQueryPartitionTablesByParentName()).thenReturn(function);
            when(connectorNode.getConnectorContext()).thenReturn(connectorContext);

            results.add(null);
            when(tableResult.add(anyString())).thenReturn(tableResult);

            TapTable table = mock(TapTable.class);
            when(table.getId()).thenReturn("id");
            masterTapTables.add(table);

            doAnswer(a -> {
                Consumer<Collection<TapPartitionResult>> argument = (Consumer<Collection<TapPartitionResult>>)a.getArgument(2, Consumer.class);
                argument.accept(results);
                return null;
            }).when(function).query(any(TapConnectorContext.class), anyList(), any(Consumer.class));
            doCallRealMethod().when(tableMonitor).loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, finalTapTableNames, existsSubTable);
        }

        @Test
        void testParentTableAndSubIdMapIsEmpty() {
            Assertions.assertDoesNotThrow(() -> tableMonitor.loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, finalTapTableNames, existsSubTable));
            verify(connectorNode, times(0)).getConnectorFunctions();
            verify(connectorFunctions, times(0)).getQueryPartitionTablesByParentName();
        }

        @Test
        void testQueryPartitionTablesByParentNameIsNull() {
            parentTableAndSubIdMap.put("id", new HashSet<>());
            when(connectorFunctions.getQueryPartitionTablesByParentName()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> tableMonitor.loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, finalTapTableNames, existsSubTable));
            verify(connectorNode).getConnectorFunctions();
            verify(connectorFunctions).getQueryPartitionTablesByParentName();
        }

        @Test
        void testNormal() {
            TapPartitionResult result1 = new TapPartitionResult();
            result1.setMasterTableName("id");
            List<String> sub1 = new ArrayList<>();
            sub1.add("sub");
            sub1.add("sub1");
            result1.setSubPartitionTableNames(sub1);
            results.add(result1);

            TapPartitionResult result2 = new TapPartitionResult();
            result2.setMasterTableName("id1");
            List<String> sub2 = new ArrayList<>();
            sub2.add("sub");
            sub2.add("sub1");
            result2.setSubPartitionTableNames(sub2);
            results.add(result2);

            TapPartitionResult result3 = new TapPartitionResult();
            result3.setMasterTableName("id");
            List<String> sub3 = new ArrayList<>();
            sub3.add("sub");
            result3.setSubPartitionTableNames(sub3);
            results.add(result3);

            Set<String> oldSubTableIds = new HashSet<>();
            oldSubTableIds.add("sub");
            parentTableAndSubIdMap.put("id", oldSubTableIds);
            try(MockedStatic<PDKInvocationMonitor> pdk = mockStatic(PDKInvocationMonitor.class)) {
                pdk.when(() -> PDKInvocationMonitor.invoke(any(ConnectorNode.class), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString())).thenAnswer(a -> {
                    CommonUtils.AnyError argument = a.getArgument(2, CommonUtils.AnyError.class);
                    argument.run();
                    return null;
                });
                Assertions.assertDoesNotThrow(() -> tableMonitor.loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, finalTapTableNames, existsSubTable));
                verify(connectorNode).getConnectorFunctions();
                verify(connectorFunctions).getQueryPartitionTablesByParentName();
            }
        }
    }
}