package io.tapdata.flow.engine.V2.monitor.impl;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionTableMonitorTest {
    PartitionTableMonitor monitor;
    Set<String> removeTables;
    TableMonitor.TableResult tableResult;

    @BeforeEach
    void init() {
        removeTables = new HashSet<>();
        removeTables.add("id");
        tableResult = mock(TableMonitor.TableResult.class);
        monitor = mock(PartitionTableMonitor.class);

        ReflectionTestUtils.setField(monitor, "tableResult", tableResult);
        ReflectionTestUtils.setField(monitor, "removeTables", removeTables);
    }

    @Nested
    class FilterIfTableNeedRemoveTest {
        List<String> finalTapTableNames;
        Set<String> masterTables;
        Set<String> existsSubTable;
        String dbTableName;

        @Test
        void testFilterIfTableNeedRemove() {
            finalTapTableNames = mock(List.class);
            masterTables = mock(Set.class);
            existsSubTable = mock(Set.class);
            dbTableName = "id";
            when(monitor.filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenReturn(true);
            when(monitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> monitor.filterIfTableNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName));
        }
    }

    @Nested
    class MonitorTest {
        ConnectorNode connectorNode;

        List<TapTable> masterTapTables;
        @BeforeEach
        void init() {
            connectorNode = mock(ConnectorNode.class);
            when(monitor.partitionTableInfoSet(anySet(), anySet(), anyMap())).thenAnswer(a -> {
                Set<String> argument = a.getArgument(0, Set.class);
                argument.add("id");
                argument.add("name");
                return masterTapTables;
            });
            //doNothing().when(monitor).loadSubTableByPartitionTable(any(ConnectorNode.class), );
        }
    }
}