package io.tapdata.sybase.cdc.dto.analyse.filter;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.sybase.cdc.CdcRoot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ReadFilter {
    public static final int LOG_CDC_QUERY_READ_LOG = 0;
    public static final int LOG_CDC_QUERY_READ_SOURCE = 1;

    protected CdcRoot root;

    public static ReadFilter stage(int readType, CdcRoot root) {
        if (readType == LOG_CDC_QUERY_READ_SOURCE) {
            return new ReadSourceFilter().init(root);
        }
        return new ReadLogFilter().init(root);
    }

    public ReadFilter init(CdcRoot root){
        this.root = root;
        return this;
    }


    public List<TapEvent> readFilter(List<TapEvent> events, TapTable tapTable, Set<String> blockFields, String fullTableName) {
        return events;
    }

    public static DataMap dispatchConnectionConfig(Map<String, List<ConnectionConfigWithTables>> connectionConfigOfTable, String databaseSchemaName, String tableName){
        if (null == connectionConfigOfTable || connectionConfigOfTable.isEmpty()) return null;
        List<ConnectionConfigWithTables> withTables = connectionConfigOfTable.get(databaseSchemaName);
        for (ConnectionConfigWithTables withTable : withTables) {
            if (null == withTable) continue;
            List<String> tables = withTable.getTables();
            if (null != tableName && !tableName.isEmpty() && tables.contains(tableName)) {
                //@todo
                DataMap connectionConfig = withTable.getConnectionConfig();
                if (null != connectionConfig && !connectionConfig.isEmpty()) {
                    Integer logCdcQuery = connectionConfig.getInteger("logCdcQuery");
                    if (null != logCdcQuery && ReadFilter.LOG_CDC_QUERY_READ_SOURCE == logCdcQuery) {
                        return connectionConfig;
                    }
                }
            }
        }
        return null;
    }

    public static Map<String, List<ConnectionConfigWithTables>> groupConnectionConfigWithTables(CdcRoot root) {
        if (null == root || null == root.getConnectionConfigWithTables()) return new HashMap<>();
        List<ConnectionConfigWithTables> connectionConfigWithTables = root.getConnectionConfigWithTables();
        return connectionConfigWithTables.stream()
                .filter(f -> null != f && null != f.getConnectionConfig() && null != f.getTables())
                .collect(Collectors.groupingBy(f -> {
                    DataMap connectionConfig = f.getConnectionConfig();
                    return String.format("%s.%s", connectionConfig.getString("database"), connectionConfig.getString("schema"));
        }));
    }
}
