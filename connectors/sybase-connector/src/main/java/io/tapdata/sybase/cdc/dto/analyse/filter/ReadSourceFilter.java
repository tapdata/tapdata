package io.tapdata.sybase.cdc.dto.analyse.filter;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.extend.SybaseConfig;
import io.tapdata.sybase.extend.SybaseContext;
import io.tapdata.sybase.util.ConnectorUtil;
import io.tapdata.sybase.util.MultiThreadFactory;

import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.toJson;

class ReadSourceFilter extends ReadFilter {
    Log log;
    Map<String, List<ConnectionConfigWithTables>> connectionConfigOfTable = new HashMap<>();

    @Override
    public ReadFilter init(CdcRoot root) {
        super.init(root);
        this.log = root.getContext().getLog();
        List<ConnectionConfigWithTables> connectionConfigWithTables = root.getConnectionConfigWithTables();
        connectionConfigOfTable = connectionConfigWithTables.stream().collect(Collectors.groupingBy(f -> {
            ConnectionConfig c = new ConnectionConfig(f.getConnectionConfig());
            return String.format("%s.%s", c.getDatabase(), c.getSchema());
        }));
        return this;
    }

    public List<TapEvent> readFilter(List<TapEvent> events, TapTable tapTable, Set<String> blockFields, String fullTableName) {
        DataMap dataMap = dispatchConnectionConfig(fullTableName);
        if (null == dataMap || dataMap.isEmpty()) {
            log.info("Can not get connection config to create jdbc connection, fail to get bol from source, full table name: {}", fullTableName);
            return events;
        }
        if (null == events || events.isEmpty()) return events;
        List<Map<String, Object>> primaryKeyValues = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>(tapTable.primaryKeys(true));
        if (primaryKeys.isEmpty()) {
            log.info("Not fund any primary key in table {}, it's mean can not read from source of this table, auto read from log of this table now", tapTable.getId());
            return events;
        }

        long start = System.currentTimeMillis();
        String sql = null;
        try {
            //step 1: collect primary key's value
            List<TapEvent> collect = events.stream().filter(e -> Objects.nonNull(e) && !(e instanceof TapDeleteRecordEvent)).collect(Collectors.toList());
            if (collect.isEmpty()) {
                return events;
            }
            collect.forEach(e -> {
                Map<String, Object> after = null;
                if (e instanceof TapInsertRecordEvent) {
                    after = ((TapInsertRecordEvent) e).getAfter();
                } else if (e instanceof TapUpdateRecordEvent) {
                    after = ((TapUpdateRecordEvent) e).getAfter();
                }
                if (null != after && !after.isEmpty()) {
                    Map<String, Object> primaryKey = new HashMap<>();
                    for (String key : primaryKeys) {
                        primaryKey.put(key, after.get(key));
                    }
                    primaryKeyValues.add(primaryKey);
                }
            });
            if (primaryKeyValues.isEmpty()) {
                return events;
            }

            //step 2; query blockFields's value by jdbc connection
            List<Map<String, Object>> queryResult = new ArrayList<>();

            Set<String> queryColumns = new HashSet<>(blockFields);
            queryColumns.addAll(primaryKeys);
            String columns = queryColumns.stream().map(c -> " " + c + " ").collect(Collectors.joining(","));
            List<Object> prepareParams = new ArrayList<>();
            final boolean onlyOnePrimaryKey = primaryKeys.size() == 1;
            String whereSql = onlyOnePrimaryKey ?
                    primaryKeyValues.stream().map(kv -> kv.keySet().stream()
                            .map(c -> {
                                prepareParams.add(kv.get(c));
                                return "?";
                            }).collect(Collectors.joining(""))
                    ).collect(Collectors.joining(","))
                    : primaryKeyValues.stream()
                    .map(kv -> kv.keySet().stream().map(c -> {
                                prepareParams.add(kv.get(c));
                                return c + "=?";
                            }).collect(Collectors.joining(" AND "))
                    ).collect(Collectors.joining(") OR ("));
            sql = onlyOnePrimaryKey ?
                    String.format("SELECT %s FROM %s where %s in (%s)", columns, fullTableName, primaryKeys.get(0), whereSql)
                    : String.format("SELECT %s FROM %s where (%s)", columns, fullTableName, whereSql);

            final Set<String> dateTypeSet = ConnectorUtil.dateFields(tapTable);

            NodeConfig nodeConfig = root.getNodeConfig();
            boolean needEncode = nodeConfig.isAutoEncode();
            String encode = needEncode ? Optional.ofNullable(nodeConfig.getEncode()).orElse("cp850") : null;
            String decode = needEncode ? Optional.ofNullable(nodeConfig.getDecode()).orElse("big5") : null;
            //String outCode = needEncode ? Optional.ofNullable(nodeConfig.getOutDecode()).orElse("utf-8") : null;
            try {
                SybaseContext sybaseContext = new SybaseContext(new SybaseConfig().load(dataMap));
                sybaseContext.prepareQuery(sql, prepareParams, resultSet -> {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    Map<String, String> typeAndNameFromMetaData = new HashMap<>();
                    int columnCount = metaData.getColumnCount();
                    for (int index = 1; index < columnCount + 1; index++) {
                        String type = metaData.getColumnTypeName(index);
                        if (null == type) continue;
                        typeAndNameFromMetaData.put(metaData.getColumnName(index), type.toUpperCase(Locale.ROOT));
                    }
                    while (root.getIsAlive().test(null) && resultSet.next()) {
                        queryResult.add(SybaseConnector.filterTimeForMysql0(resultSet, typeAndNameFromMetaData, dateTypeSet, needEncode, encode, decode));
                    }
                });
            } catch (Exception e) {
                log.error("Query blockFields's value by jdbc connection failed, full table name: {}, error msg: {}", fullTableName, e.getMessage());
            }


            //step 3: assemble blockFields's value into tap event
            MultiThreadFactory<TapEvent> multiThreadFactory = new MultiThreadFactory<>(Math.max(Math.min(collect.size() / 50, 5), 1), 50);
            multiThreadFactory.handel(collect, e -> {
                synchronized (this) {
                    for (TapEvent tapEvent : e) {
                        Map<String, Object> after = null;
                        if (tapEvent instanceof TapInsertRecordEvent) {
                            after = ((TapInsertRecordEvent) tapEvent).getAfter();
                        } else if (tapEvent instanceof TapUpdateRecordEvent) {
                            after = ((TapUpdateRecordEvent) tapEvent).getAfter();
                        }
                        if (null != after) {
                            for (Map<String, Object> result : queryResult) {
                                boolean isThisRecord = false;
                                for (String key : primaryKeys) {
                                    Object afterValue = after.get(key);
                                    Object resultValue = result.get(key);
                                    if (!(isThisRecord =
                                            (null == afterValue && resultValue == null)
                                                    || (null != afterValue && afterValue.equals(resultValue)))) break;
                                }
                                if (isThisRecord) {
                                    after.putAll(result);
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        } finally {
            if (null != sql) {
                log.debug("Read source with table {} for an batch recodes {} cost time: {}ms, sql: {}, all line's primary keys: {}",
                        fullTableName,
                        events.size(),
                        System.currentTimeMillis() - start,
                        sql,
                        toJson(primaryKeyValues));
            }
        }
        return events;
    }

    private DataMap dispatchConnectionConfig(String fullTableName){
        if (null == fullTableName || "".equals(fullTableName.trim())) return null;
        String[] split = fullTableName.split("\\.");
        if (split.length != 3) return null;
        String databaseSchema = String.format("%s.%s", split[0], split[1]);
        String tableName = split[2];
        if (null == connectionConfigOfTable || connectionConfigOfTable.isEmpty()) return null;
        List<ConnectionConfigWithTables> withTables = connectionConfigOfTable.get(databaseSchema);
        for (ConnectionConfigWithTables withTable : withTables) {
            if (null == withTable) continue;
            List<String> tables = withTable.getTables();
            if (null != tableName && !tableName.isEmpty() && tables.contains(tableName)) {
                return withTable.getConnectionConfig();
            }
        }
        return null;
    }
}
