package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;

public class ConnectionConfigWithTables {
    public static ConnectionConfigWithTables create() {
        return new ConnectionConfigWithTables();
    }
    private DataMap connectionConfig;
    public ConnectionConfigWithTables connectionConfig(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }
    private List<String> tables;
    public ConnectionConfigWithTables tables(List<String> tables) {
        this.tables = tables;
        return this;
    }
    public ConnectionConfigWithTables table(String table) {
        if(this.tables == null)
            this.tables = new ArrayList<>();
        if(!this.tables.contains(table))
            this.tables.add(table);
        return this;
    }

    public DataMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
