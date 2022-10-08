package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_postgres.json")
public class PostgresConnector extends ConnectorBase {

    private PostgresConfig postgresConfig;
    private PostgresJdbcContext postgresJdbcContext;
    private PostgresTest postgresTest;
    private PostgresCdcRunner cdcRunner; //only when task start-pause this variable can be shared
    private Object slotName; //must be stored in stateMap
    private String postgresVersion;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlGenerator ddlSqlGenerator;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = postgresJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("table_name")).collect(Collectors.toList());
            List<DataMap> columnList = postgresJdbcContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = postgresJdbcContext.queryAllIndexes(subTableNames);
            //make up tapTable
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("table_name");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("comment"));
                //2、primary key and table index (find primary key from index info)
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("table_name")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v -> (boolean) v.get("is_primary"))) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("column_name")).collect(Collectors.toList()));
                    }
                    TapIndex index = index(key);
                    value.forEach(v -> index.indexField(indexField(v.getString("column_name")).fieldAsc("A".equals(v.getString("asc_or_desc")))));
                    index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
                    index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
                    tapIndexList.add(index);
                });
                //3、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                        .forEach(col -> {
                            TapField tapField = new PostgresColumn(col).getTapField(); //make up fields
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

    // TODO: 2022/6/9 need to improve test items
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                PostgresTest postgresTest = new PostgresTest(postgresConfig)
        ) {
            TestItem testHostPort = postgresTest.testHostPort();
            consumer.accept(testHostPort);
            if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            }
            TestItem testConnect = postgresTest.testConnect();
            consumer.accept(testConnect);
            if (testConnect.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            }
            consumer.accept(postgresTest.testPrivilege());
            consumer.accept(postgresTest.testReplication());
            consumer.accept(postgresTest.testLogPlugin());
            return connectionOptions;
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return postgresJdbcContext.queryAllTables(null).size();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        //need to clear resource outer
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        // target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        // source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        postgresJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) throws SQLException {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        postgresJdbcContext.batchExecute(sqls);
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(postgresConfig, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(postgresConfig, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(postgresConfig, tapAlterFieldNameEvent);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(postgresConfig, tapNewFieldEvent);
    }

    //clear resource outer and jdbc context
    private void onDestroy(TapConnectorContext connectorContext) throws Throwable {
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner();
            cdcRunner = null;
        }
        if (EmptyKit.isNotNull(slotName)) {
            clearSlot();
        }
        //stateMap will be cleared by engine
    }

    //clear postgres slot
    private void clearSlot() throws Throwable {
        postgresJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "' AND active='false'", resultSet -> {
            if (resultSet.getInt(1) > 0) {
                postgresJdbcContext.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            }
        });
    }

    private void buildSlot() throws Throwable {
        slotName = "tapdata_cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
        postgresJdbcContext.execute("SELECT pg_create_logical_replication_slot('" + slotName + "','" + postgresConfig.getLogPluginName() + "')");
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner();
            cdcRunner = null;
        }
        if (EmptyKit.isNotNull(postgresTest)) {
            postgresTest.close();
            postgresTest = null;
        }
        if (EmptyKit.isNotNull(postgresJdbcContext)) {
            postgresJdbcContext.finish(connectionContext.getId());
        }
    }

    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectorContext) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectorContext.getConnectionConfig());
        postgresTest = new PostgresTest(postgresConfig);
        if (EmptyKit.isNull(postgresJdbcContext) || postgresJdbcContext.isFinish()) {
            postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class, connectorContext.getId());
        }
        isConnectorStarted(connectorContext, tapConnectorContext -> slotName = tapConnectorContext.getStateMap().get("tapdata_pg_slot"));
        postgresVersion = postgresJdbcContext.queryVersion();
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    //one filter can only match one record
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\" WHERE " + CommonSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                postgresJdbcContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM \"" + postgresConfig.getSchema() + "\".\"" + table.getId() + "\" " + CommonSqlMaker.buildSqlByAdvanceFilter(filter);
        postgresJdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (resultSet.next()) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    //create table with info which comes from tapTable
    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" + CommonSqlMaker.buildColumnDefinition(tapTable, false);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            postgresJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                postgresJdbcContext.execute("TRUNCATE TABLE \"" + postgresConfig.getSchema() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (postgresJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                postgresJdbcContext.execute("DROP TABLE IF EXISTS \"" + postgresConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) {
        try {
            List<String> sqls = TapSimplify.list();
            if (EmptyKit.isNotEmpty(createIndexEvent.getIndexList())) {
                if (Integer.parseInt(postgresVersion) > 90500) {
                    createIndexEvent.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                    (EmptyKit.isNotNull(i.getName()) ? "IF NOT EXISTS \"" + i.getName() + "\"" : "") + " ON \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                } else {
                    List<String> existsIndexes = TapSimplify.list();
                    postgresJdbcContext.query("SELECT relname FROM pg_class WHERE relname IN (" +
                                    createIndexEvent.getIndexList().stream().map(i -> "'" + (EmptyKit.isNotNull(i.getName()) ? i.getName() : "") + "'").collect(Collectors.joining(",")) + ") AND relkind = 'i'",
                            resultSet -> existsIndexes.addAll(DbKit.getDataFromResultSet(resultSet).stream().map(v -> v.getString("relname")).collect(Collectors.toList())));
                    createIndexEvent.getIndexList().stream().filter(i -> !i.isPrimary() && !existsIndexes.contains(i.getName())).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                    (EmptyKit.isNotNull(i.getName()) ? "\"" + i.getName() + "\"" : "") + " ON \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                }
            }
            postgresJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Indexes for " + tapTable.getId() + " Failed! " + e.getMessage());
        }

    }

    //write records as all events, prepared
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new PostgresRecordWriter(postgresJdbcContext, tapTable)
                .setVersion(postgresVersion)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, writeListResultConsumer);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\"";
        postgresJdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        //test streamRead log plugin
        if (postgresTest.testLogPlugin().getResult() == TestItem.RESULT_SUCCESSFULLY && EmptyKit.isNull(slotName)) {
            buildSlot();
            tapConnectorContext.getStateMap().put("tapdata_pg_slot", slotName);
        }
        PostgresOffset postgresOffset;
        //beginning
        if (null == offsetState) {
            postgresOffset = new PostgresOffset(CommonSqlMaker.getOrderByUniqueKey(tapTable), 0L);
        }
        //with offset
        else {
            postgresOffset = (PostgresOffset) offsetState;
        }
        String sql = "SELECT * FROM \"" + postgresConfig.getSchema() + "\".\"" + tapTable.getId() + "\"" + postgresOffset.getSortString() + " OFFSET " + postgresOffset.getOffsetValue();
        postgresJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + eventBatchSize);
                    eventsOffsetConsumer.accept(tapEvents, postgresOffset);
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                postgresOffset.setOffsetValue(postgresOffset.getOffsetValue() + tapEvents.size());
                eventsOffsetConsumer.accept(tapEvents, postgresOffset);
            }
        });

    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if (EmptyKit.isNull(cdcRunner)) {
            cdcRunner = new PostgresCdcRunner(postgresJdbcContext);
            if (EmptyKit.isNull(slotName)) {
                buildSlot();
                nodeContext.getStateMap().put("tapdata_pg_slot", slotName);
            }
            cdcRunner.useSlot(slotName.toString()).watch(tableList).offset(offsetState).registerConsumer(consumer, recordSize);
        }
        cdcRunner.startCdcRunner();
        if (EmptyKit.isNotNull(cdcRunner) && EmptyKit.isNotNull(cdcRunner.getThrowable().get())) {
            throw cdcRunner.getThrowable().get();
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return new PostgresOffset();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = postgresTest.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = postgresTest.testConnection();
        consumer.accept(testConnection);
    }

}
