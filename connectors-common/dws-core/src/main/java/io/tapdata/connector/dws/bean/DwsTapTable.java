package io.tapdata.connector.dws.bean;

import io.tapdata.connector.dws.DwsJdbcContext;
import io.tapdata.entity.schema.TapTable;

import java.util.Collections;
import java.util.List;

public class DwsTapTable {
    private TapTable tapTable;
    private boolean isPartition;
    private List<String> distributedKeys;

    public DwsTapTable(TapTable tapTable, boolean isPartition, List<String> distributedKeys) {
        this.tapTable = tapTable;
        this.isPartition = isPartition;
        this.distributedKeys = distributedKeys;
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
