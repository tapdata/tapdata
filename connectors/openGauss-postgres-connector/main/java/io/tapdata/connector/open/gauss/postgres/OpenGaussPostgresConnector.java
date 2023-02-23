package io.tapdata.connector.open.gauss.postgres;
import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.connector.open.gauss.postgres.bean.OpenGaussColumn;
import io.tapdata.connector.open.gauss.postgres.cdc.OpenGaussCdcRunner;
import io.tapdata.connector.open.gauss.postgres.cdc.offset.OpenGaussOffset;
import io.tapdata.connector.open.gauss.postgres.config.OpenGaussConfig;
import io.tapdata.connector.open.gauss.postgres.ddl.OpenGaussDDLSqlGenerator;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
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
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;


@TapConnectorClass("spec_open_gauss.json")
public class OpenGaussPostgresConnector extends ConnectorBase {

    private OpenGaussConfig openGaussConfig;
    private OpenGaussJdbcContext openGaussJdbcContext;
    private OpenGaussTest openGaussTest;
    private OpenGaussCdcRunner cdcRunner; //only when task start-pause this variable~ can be shared
    private Object slotName; //must be stored in stateMap
    private String openGaussVersion;
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
        List<DataMap> tableList = openGaussJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("table_name")).collect(Collectors.toList());
            List<DataMap> columnList = openGaussJdbcContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = openGaussJdbcContext.queryAllIndexes(subTableNames);
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
                    tapIndexList.add(makeTapIndex(key, value));
                });
                //3、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("table_name")))
                        .forEach(col -> {
                            TapField tapField = new OpenGaussColumn(col).getTapField(); //make up fields
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

    private TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = index(key);
        value.forEach(v -> index.indexField(indexField(v.getString("column_name")).fieldAsc("A".equals(v.getString("asc_or_desc")))));
        index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
        index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
        return index;
    }

    private List<TapIndex> discoverIndex(String tableName) {
        List<TapIndex> tapIndexList = TapSimplify.list();
        List<DataMap> indexList = openGaussJdbcContext.queryAllIndexes(Collections.singletonList(tableName));
        Map<String, List<DataMap>> indexMap = indexList.stream()
                .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> tapIndexList.add(makeTapIndex(key, value)));
        return tapIndexList;
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        openGaussConfig = (OpenGaussConfig) new OpenGaussConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(openGaussConfig.getConnectionString());
        try (
                OpenGaussTest openGaussTest = new OpenGaussTest(openGaussConfig, consumer).initContext()
        ) {
            openGaussTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return openGaussJdbcContext.queryAllTables(null).size();
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
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> openGaussJdbcContext.getConnection(), c));

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

        codecRegistry.registerToTapValue(PgArray.class, (value, tapType) -> new TapStringValue(toJson(value)));
        codecRegistry.registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        openGaussJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) throws SQLException {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        openGaussJdbcContext.batchExecute(sqls);
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(openGaussConfig, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(openGaussConfig, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(openGaussConfig, tapAlterFieldNameEvent);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(openGaussConfig, tapNewFieldEvent);
    }

    //clear resource outer and jdbc context
    private void onDestroy(TapConnectorContext connectorContext) throws Throwable {
        try {
            onStart(connectorContext);
            if (EmptyKit.isNotNull(cdcRunner)) {
                cdcRunner.closeCdcRunner();
                cdcRunner = null;
            }
            if (EmptyKit.isNotNull(slotName)) {
                clearSlot();
            }
        } finally {
            onStop(connectorContext);
        }
    }

    //clear postgres slot
    private void clearSlot() throws Throwable {
        openGaussJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "' AND active='false'", resultSet -> {
            if (resultSet.getInt(1) > 0) {
                openGaussJdbcContext.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            }
        });
    }

    private void buildSlot() throws Throwable {
        slotName = "tapdata_cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
        openGaussJdbcContext.execute("SELECT pg_create_logical_replication_slot('" + slotName + "','" + openGaussConfig.getLogPluginName() + "')");
    }
    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner();
            cdcRunner = null;
        }
        if (EmptyKit.isNotNull(openGaussTest)) {
            openGaussTest.close();
            openGaussTest = null;
        }
        if (EmptyKit.isNotNull(openGaussJdbcContext)) {
            openGaussJdbcContext.finish(connectionContext.getId());
        }
    }
    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectorContext) {
        openGaussConfig = (OpenGaussConfig) new OpenGaussConfig().load(connectorContext.getConnectionConfig());
        openGaussTest = new OpenGaussTest(openGaussConfig, testItem -> {
        }).initContext();
        if (EmptyKit.isNull(openGaussJdbcContext) || openGaussJdbcContext.isFinish()) {
            openGaussJdbcContext = (OpenGaussJdbcContext) DataSourcePool.getJdbcContext(openGaussConfig, OpenGaussJdbcContext.class, connectorContext.getId());
        }
        isConnectorStarted(connectorContext, tapConnectorContext -> slotName = tapConnectorContext.getStateMap().get("tapdata_pg_slot"));
        openGaussVersion = openGaussJdbcContext.queryVersion();
        ddlSqlGenerator = new OpenGaussDDLSqlGenerator();
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
            String sql = "SELECT * FROM \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\" WHERE " + new CommonSqlMaker().buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                openGaussJdbcContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        StringBuilder builder = new StringBuilder("SELECT ");
        Projection projection = filter.getProjection();
        if (EmptyKit.isNull(projection) || (EmptyKit.isEmpty(projection.getIncludeFields()) && EmptyKit.isEmpty(projection.getExcludeFields()))) {
            builder.append("*");
        } else {
            builder.append("\"");
            if (EmptyKit.isNotEmpty(filter.getProjection().getIncludeFields())) {
                builder.append(String.join("\",\"", filter.getProjection().getIncludeFields()));
            } else {
                builder.append(table.getNameFieldMap().keySet().stream()
                        .filter(tapField -> !filter.getProjection().getExcludeFields().contains(tapField)).collect(Collectors.joining("\",\"")));
            }
            builder.append("\"");
        }
        builder.append(" FROM \"").append(openGaussConfig.getSchema()).append("\".\"").append(table.getId()).append("\" ").append(new CommonSqlMaker().buildSqlByAdvanceFilter(filter));
        openGaussJdbcContext.query(builder.toString(), resultSet -> {
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
        if (openGaussJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys();
        //pgsql UNIQUE INDEX use 'UNIQUE' not 'UNIQUE KEY' but here use 'PRIMARY KEY'
        String sql = "CREATE TABLE IF NOT EXISTS \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" + new CommonSqlMaker().buildColumnDefinition(tapTable, false);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            openGaussJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (openGaussJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                openGaussJdbcContext.execute("TRUNCATE TABLE \"" + openGaussConfig.getSchema() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (openGaussJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                openGaussJdbcContext.execute("DROP TABLE IF EXISTS \"" + openGaussConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) {
        try {
            List<String> sqls = TapSimplify.list();
            List<TapIndex> indexList = createIndexEvent.getIndexList().stream().filter(v -> discoverIndex(tapTable.getId()).stream()
                    .noneMatch(i -> DbKit.ignoreCreateIndex(i, v))).collect(Collectors.toList());
            if (EmptyKit.isNotEmpty(indexList)) {
                if (Integer.parseInt(openGaussVersion) > 90500) {
                    indexList.stream().filter(i -> !i.isPrimary()).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                    (EmptyKit.isNotNull(i.getName()) ? "IF NOT EXISTS \"" + i.getName() + "\"" : "") + " ON \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                } else {
                    List<String> existsIndexes = TapSimplify.list();
                    openGaussJdbcContext.query("SELECT relname FROM pg_class WHERE relname IN (" +
                                    indexList.stream().map(i -> "'" + (EmptyKit.isNotNull(i.getName()) ? i.getName() : "") + "'").collect(Collectors.joining(",")) + ") AND relkind = 'i'",
                            resultSet -> existsIndexes.addAll(DbKit.getDataFromResultSet(resultSet).stream().map(v -> v.getString("relname")).collect(Collectors.toList())));
                    indexList.stream().filter(i -> !i.isPrimary() && !existsIndexes.contains(i.getName())).forEach(i ->
                            sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                    (EmptyKit.isNotNull(i.getName()) ? "\"" + i.getName() + "\"" : "") + " ON \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" +
                                    i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                            .collect(Collectors.joining(",")) + ')'));
                }
            }
            openGaussJdbcContext.batchExecute(sqls);
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
        new OpenGaussRecordWriter(openGaussJdbcContext, tapTable)
                .setVersion(openGaussVersion)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, writeListResultConsumer);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\"";
        openGaussJdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        //test streamRead log plugin
        boolean canCdc = EmptyKit.isNotNull(openGaussTest.testStreamRead()) && openGaussTest.testStreamRead();
        if (canCdc && EmptyKit.isNull(slotName)) {
            buildSlot();
            tapConnectorContext.getStateMap().put("tapdata_pg_slot", slotName);
        }
        OpenGaussOffset openGaussOffset;
        //beginning
        if (null == offsetState) {
            openGaussOffset = new OpenGaussOffset(new CommonSqlMaker().getOrderByUniqueKey(tapTable), 0L);
        }
        //with offset
        else {
            openGaussOffset = (OpenGaussOffset) offsetState;
        }
        String sql = "SELECT * FROM \"" + openGaussConfig.getSchema() + "\".\"" + tapTable.getId() + "\"" + openGaussOffset.getSortString() + " OFFSET " + openGaussOffset.getOffsetValue();
        openGaussJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    openGaussOffset.setOffsetValue(openGaussOffset.getOffsetValue() + eventBatchSize);
                    eventsOffsetConsumer.accept(tapEvents, openGaussOffset);
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                openGaussOffset.setOffsetValue(openGaussOffset.getOffsetValue() + tapEvents.size());
                eventsOffsetConsumer.accept(tapEvents, openGaussOffset);
            }
        });

    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if (EmptyKit.isNull(cdcRunner)) {
            cdcRunner = new OpenGaussCdcRunner(openGaussJdbcContext);
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
        return new OpenGaussOffset();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = openGaussTest.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = openGaussTest.testConnection();
        consumer.accept(testConnection);
    }

}
