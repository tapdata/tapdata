package io.tapdata.connector.dws.bean;

import io.tapdata.connector.dws.DwsJdbcContext;
import io.tapdata.entity.schema.TapTable;

import java.util.*;
import java.util.stream.Collectors;

public class DwsTapTable {
    private TapTable tapTable;
    private boolean isPartition;
    private List<String> distributedKeys;

    public DwsTapTable(TapTable tapTable, boolean isPartition, List<String> distributedKeys) {
        this.tapTable = tapTable;
        this.isPartition = isPartition;
        this.distributedKeys = distributedKeys;
    }

    public Set<String> buildConflictKeys(){
        Collection<String> primaryKeys = tapTable.primaryKeys(false);
        if (null != primaryKeys && !primaryKeys.isEmpty()){
            return primaryKeys.stream().collect(Collectors.toSet());
        }
        Set<String> conflictKeys = new HashSet<>();
        conflictKeys.addAll(distributedKeys);
        Collection<String> logicPrimaryKeys = tapTable.primaryKeys(true);
        conflictKeys.addAll(logicPrimaryKeys);
        return conflictKeys;
    }

    public boolean isPartition() {
        return isPartition;
    }

    public void setPartition(boolean partition) {
        isPartition = partition;
    }

    public List<String> getDistributedKeys() {
        return distributedKeys;
    }

    public void setDistributedKeys(List<String> distributedKeys) {
        this.distributedKeys = distributedKeys;
    }

    public TapTable getTapTable() {
        return tapTable;
    }
}
