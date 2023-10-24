package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.schema.TapTable;

import java.util.List;

public abstract class TapTableList {
    private List<String> tableIds;

    /**
     * Table is be cached in KV memory/storage
     *
     * @param tableId
     * @return
     */
    public abstract TapTable getTable(String tableId);

    public List<String> getTableIds() {
        return tableIds;
    }

    public void setTableIds(List<String> tableIds) {
        this.tableIds = tableIds;
    }
}
