package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.utils.PartitionTableUtil;
import io.tapdata.Runnable.LoadSchemaRunner;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;

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
        //initMasterTable();
    }

    public void initMasterTable() {
        final Map<String, TapTable> masterTables = new HashMap<>(); //主表
        ConnectorNode connectorNode = ConnectorNodeService.getInstance().getConnectorNode(associateId);
        Iterator<Entry<TapTable>> iterator = tapTableMap.iterator();
        while (iterator.hasNext()) {
            Entry<TapTable> next = iterator.next();
            TapTable table = next.getValue();
            if (PartitionTableUtil.checkIsMasterPartitionTable(table)) {
                String id = next.getKey();
                masterTables.put(id, table);
            }
        }
        try {
            if (masterTables.isEmpty()) return;
            LoadSchemaRunner.pdkDiscoverSchema(connectorNode, new ArrayList<>(masterTables.keySet()), tapTable -> {
                TapTable table = masterTables.get(tapTable.getId());
                table.setPartitionInfo(tapTable.getPartitionInfo());
            });
        } catch (Exception e) {

        }
    }

    @Override
    protected boolean filterIfTableNeedRemove(List<String> finalTapTableNames, Set<String> masterTables, Set<String> existsSubTable, String dbTableName) {
        return finalTapTableNames.contains(dbTableName)
                || existsSubTable.contains(dbTableName)
                || (null != syncSourcePartitionTableEnable && !syncSourcePartitionTableEnable && masterTables.contains(dbTableName));
    }

    @Override
    protected void monitor(ConnectorNode connectorNode) throws IOException {
        final Set<String> existsSubTable = new HashSet<>();//子表
        final Map<String, Set<String>> parentTableAndSubIdMap = new HashMap<>();
        final Set<String> masterTables = new HashSet<>(); //主表
        final List<TapTable> masterTapTables = partitionTableInfoSet(masterTables, existsSubTable, parentTableAndSubIdMap);
        List<String> tapTableNames = new ArrayList<>(masterTables)
                .stream()
                .filter(name -> !removeTables.contains(name))
                .collect(Collectors.toList());
        /**
         * Dynamically add tables and load newly added sub tables based on the main table
         * */
        if (Boolean.TRUE.equals(syncSourcePartitionTableEnable) && !masterTables.isEmpty()) {
            loadSubTableByPartitionTable(connectorNode, masterTapTables, parentTableAndSubIdMap, tapTableNames, existsSubTable);
        }
        if (CollectionUtils.isNotEmpty(tapTableNames)) {
            tableResult.removeAll(tapTableNames);
            removeTables.addAll(tapTableNames);
        }
        tableResult.getAddList().removeAll(masterTables);
        removeTables.removeAll(masterTables);
        tableResult.getRemoveList().removeAll(masterTables);
    }
}