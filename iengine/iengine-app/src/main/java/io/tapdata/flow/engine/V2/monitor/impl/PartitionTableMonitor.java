package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PartitionTableMonitor extends TableMonitor {

    public PartitionTableMonitor(TapTableMap<String, TapTable> tapTableMap, String associateId, TaskDto taskDto, Connections connections, Predicate<String> dynamicTableFilter) {
        super(tapTableMap, associateId, taskDto, connections, dynamicTableFilter);
    }

    @Override
    protected boolean filterIfTableNeedRemove(List<String> finalTapTableNames,
                                              Set<String> masterTables,
                                              Set<String> existsSubTable,
                                              String dbTableName) {
        return filterPartitionTableIfNeedRemove(finalTapTableNames, masterTables, existsSubTable, dbTableName);
    }

    @Override
    protected void monitor(ConnectorNode connectorNode) throws IOException {
        final Set<String> existsSubTable = new HashSet<>();
        final Map<String, Set<String>> parentTableAndSubIdMap = new HashMap<>();
        final Set<String> masterTables = new HashSet<>();
        final List<TapTable> masterTapTables = partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap);
        List<String> tapTableNames = new ArrayList<>(masterTables)
                .stream()
                .filter(name -> !removeTables.contains(name))
                .collect(Collectors.toList());
        if (Boolean.TRUE.equals(syncSourcePartitionTableEnable) && !masterTables.isEmpty()) {
            loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, tapTableNames, existsSubTable);
        }
        if (CollectionUtils.isNotEmpty(tapTableNames)) {
            tableResult.removeAll(tapTableNames);
            removeTables.addAll(tapTableNames);
        }

        removeTables.removeAll(masterTables);
        tableResult.getRemoveList().removeAll(masterTables);
    }
}