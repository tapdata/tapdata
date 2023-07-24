package io.tapdata.connector.gbase8s;

import com.gbasedbt.jdbcx.IfxConnectionPoolDataSource;
import com.gbasedbt.jdbcx.IfxDataSource;
import com.google.common.collect.Lists;
import com.informix.stream.cdc.IfxCDCEngine;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.gbase8s.bean.Gbase8sColumn;
import io.tapdata.connector.gbase8s.cdc.Gbase8sCdcRunner;
import io.tapdata.connector.gbase8s.cdc.Gbase8sOffset;
import io.tapdata.connector.gbase8s.config.Gbase8sConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_gbase8s.json")
public class Gbase8sConnector extends CommonDbConnector {

    private Gbase8sConfig gbase8sConfig;
    private Gbase8sTest gbase8sTest;
    private Gbase8sJdbcContext gbase8sJdbcContext;
    private Gbase8sCdcRunner gbase8sCdcRunner;

    private void initConnection(TapConnectionContext connectorContext) {
        gbase8sConfig = (Gbase8sConfig) new Gbase8sConfig().load(connectorContext.getConnectionConfig());
        gbase8sTest = new Gbase8sTest(gbase8sConfig, testItem -> {
        });
        gbase8sJdbcContext = new Gbase8sJdbcContext(gbase8sConfig);
        commonDbConfig = gbase8sConfig;
        jdbcContext = gbase8sJdbcContext;
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (EmptyKit.isNotNull(gbase8sTest)) {
            gbase8sTest.close();
        }
        if (EmptyKit.isNotNull(gbase8sJdbcContext)) {
            gbase8sJdbcContext.close();
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> gbase8sJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);

        codecRegistry.registerFromTapValue(TapRawValue.class, "TEXT", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "TEXT", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "TEXT", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) {
//        if (null == startTime) {
//            Gbase8sConfig cdcConfig = (Gbase8sConfig) new Gbase8sConfig().load(tapConnectorContext.getConnectionConfig());
//            cdcConfig.setDatabase("syscdcv1");
//            AtomicReference<Long> sequenceId = new AtomicReference<>();
//            AtomicReference<IfxCDCEngine> engine = new AtomicReference<>();
//            try (Gbase8sJdbcContext cdcJdbcContext = new Gbase8sJdbcContext(cdcConfig);
//            ) {
//                cdcJdbcContext.execute("create table if not exists informix.tapdata_cdc(a1 int)");
//                AtomicReference<Exception> exception = new AtomicReference<>();
//                AtomicReference<Boolean> isStarted = new AtomicReference<>(false);
//                new Thread(() -> {
//                    try {
//                        IfxConnectionPoolDataSource dataSource = new IfxConnectionPoolDataSource();
//                        dataSource.setServerName(cdcConfig.getDatabaseUrl());
//                        dataSource.setUser(cdcConfig.getUser());
//                        dataSource.setPassword(cdcConfig.getPassword());
//                        IfxDataSource ds = null;
//                        try {
//                            ds = new IfxDataSource(dataSource);
//
//                        } catch (SQLException throwables) {
//                            throwables.printStackTrace();
//                        }
////                    IfxDataSource ds = new IfxDataSource(cdcConfig.getDatabaseUrl() + "user=" + cdcConfig.getUser() + ";password=" + cdcConfig.getPassword() + ";");
//                        IfxCDCEngine.Builder builder = new IfxCDCEngine.Builder(ds);
//                        builder.watchTable("syscdcv1:informix.tapdata_cdc", "a1");
//                        builder.timeout(5);
//                        engine.set(builder.build());
//
//                        //TODO
//                        isStarted.set(true);
//                    } catch (SQLException throwables) {
//                        throwables.printStackTrace();
//                    }
//
//                }).start();
//            } catch (SQLException throwables) {
//                throwables.printStackTrace();
//            }
//        }
        //TODO

        Gbase8sOffset gbase8sOffset = new Gbase8sOffset();
        gbase8sOffset.setPendingScn(LocalDateTime.now().toEpochSecond(java.time.ZoneOffset.UTC));
        gbase8sOffset.setLastScn(LocalDateTime.now().toEpochSecond(java.time.ZoneOffset.UTC));
        return gbase8sOffset;
    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if (gbase8sCdcRunner == null) {
            gbase8sCdcRunner = new Gbase8sCdcRunner(gbase8sJdbcContext, firstConnectorId, tapLogger).init(
                    tableList,
                    tapConnectorContext.getTableMap(),
                    offsetState,
                    recordSize,
                    consumer
            );
        }
        gbase8sCdcRunner.startCdcRunner();
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = gbase8sJdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("tabname")).collect(Collectors.toList());
            List<DataMap> columnList = gbase8sJdbcContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = gbase8sJdbcContext.queryAllIndexes(subTableNames);
            subList.forEach(subTable -> {
                //2、table name/comment
                String table = subTable.getString("tabname");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("comments"));
                //3、primary key and table index
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("tabname")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("idxname"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v -> "YES".equals(v.getString("isprimary")))) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("colname")).collect(Collectors.toList()));
                    }
                    TapIndex index = new TapIndex();
                    index.setName(key);
                    List<TapIndexField> fieldList = TapSimplify.list();
                    value.forEach(v -> {
                        TapIndexField field = new TapIndexField();
                        field.setFieldAsc(true);
                        field.setName(v.getString("colname"));
                        fieldList.add(field);
                    });
                    index.setUnique(value.stream().anyMatch(v -> "YES".equals(v.getString("isunique"))));
                    index.setPrimary(value.stream().anyMatch(v -> "YES".equals(v.getString("isprimary"))));
                    index.setIndexFields(fieldList);
                    tapIndexList.add(index);
                });
                //4、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("tabname")))
                        .forEach(col -> {
                            TapField tapField = new Gbase8sColumn(col).getTapField();
                            tapField.setPos(keyPos.incrementAndGet());
                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            tapTable.add(tapField);
                        });
                tapTable.setIndexList(tapIndexList);
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        });
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        gbase8sConfig = (Gbase8sConfig) new Gbase8sConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(gbase8sConfig.getConnectionString());
        try (
                Gbase8sTest gbase8sTest = new Gbase8sTest(gbase8sConfig, consumer)
        ) {
            gbase8sTest.testOneByOne();
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return gbase8sJdbcContext.queryAllTables(null).size();
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        String tableName = tapClearTableEvent.getTableId();
        if (!gbase8sJdbcContext.isTableNameCaseSensitive()) {
            tableName = tableName.toLowerCase();
        }
        if (gbase8sJdbcContext.queryAllTables(Collections.singletonList(tableName)).size() > 0) {
            gbase8sJdbcContext.execute("TRUNCATE TABLE \"" + gbase8sConfig.getSchema() + "\".\"" + tableName + "\"");
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        gbase8sJdbcContext.execute("DROP TABLE IF EXISTS \"" + gbase8sConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
    }

    protected CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (gbase8sJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS \"" + gbase8sConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" + new CommonSqlMaker().buildColumnDefinition(tapTable, false);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        sql += ") ";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + gbase8sConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + gbase8sConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            gbase8sJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        new Gbase8sRecordWriter(gbase8sJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, consumer);
    }

    protected void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + tapTable.getId() + "\" WHERE " + new CommonSqlMaker().buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                gbase8sJdbcContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = gbase8sTest.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = gbase8sTest.testConnection();
        consumer.accept(testConnection);
    }
}
