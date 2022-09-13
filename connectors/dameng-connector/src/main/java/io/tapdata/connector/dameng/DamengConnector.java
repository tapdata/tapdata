package io.tapdata.connector.dameng;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.connector.dameng.bean.DamengColumn;
import io.tapdata.connector.dameng.cdc.DamengCdcRunner;
import io.tapdata.connector.dameng.cdc.DamengOffset;
import io.tapdata.connector.dameng.dml.DamengRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
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
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import oracle.sql.TIMESTAMP;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author lemon
 */
@TapConnectorClass("spec_dameng.json")
public class DamengConnector extends ConnectorBase {

    private final static String TAG = DamengConnector.class.getSimpleName();

    private DamengConfig damengConfig;

    private DamengContext damengContext;

    private DamengTest damengTest;

    private String damengVersion;

    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlGenerator ddlSqlGenerator;

    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    private DamengCdcRunner cdcRunner;


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

        damengConfig = new DamengConfig().load(connectionContext.getConnectionConfig());
        damengTest = new DamengTest(damengConfig);
        isConnectorStarted(connectionContext, tapConnectorContext -> damengConfig.load(tapConnectorContext.getNodeConfig()));
        if (EmptyKit.isNull(damengContext) || damengContext.isFinish()) {
            damengContext = (DamengContext) DataSourcePool.getJdbcContext(damengConfig, DamengContext.class, connectionContext.getId());
        }
        TimeZone.setDefault(TimeZone.getTimeZone(damengConfig.getTimezone()));
        String sysTimezone = damengContext.querySysTimezone();
        if (sysTimezone != null) {
            damengConfig.setSysZoneId(ZoneId.of(sysTimezone));
        }
        damengVersion = damengContext.queryVersion();
        //ddlSqlGenerator = new OracleDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }


    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

        if (EmptyKit.isNotNull(cdcRunner)) {
            cdcRunner.closeCdcRunner();
            cdcRunner =null;
        }

        if (EmptyKit.isNotNull(damengTest)) {
            damengTest.close();
        }
        if (EmptyKit.isNotNull(damengContext)) {
            damengContext.finish(connectionContext.getId());
        }



    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        //target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        //source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        //query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
//        //ddl
//        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
//        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
//        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);


        codecRegistry.registerFromTapValue(TapMapValue.class, "CLOB", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) {
                return toJson(tapMapValue.getValue());
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "CLOB", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return toJson(tapValue.getValue());
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "INTEGER", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue() ? 1 : 0;
            }
            return 0;
        });

        codecRegistry.registerToTapValue(TIMESTAMP.class, (value, tapType) -> {
            try {
                return new TapDateTimeValue(new DateTime(((TIMESTAMP) value).timestampValue()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        codecRegistry.registerToTapValue(CLOB.class, (value, tapType) -> {
            try {
                return new TapStringValue(((CLOB) value).stringValue());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        codecRegistry.registerToTapValue(BLOB.class, (value, tapType) -> new TapBinaryValue(DbKit.blobToBytes((BLOB) value)));

        codecRegistry.registerFromTapValue(TapTimeValue.class, "CHAR(8)", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, "CHAR(10)", tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        damengContext.queryAllTables(list(), batchSize, listConsumer);
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        new DamengRecordWriter(damengContext, tapTable).setVersion(damengVersion).write(tapRecordEvents, writeListResultConsumer);
    }

    private CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (damengContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
        for (String field : fieldMap.keySet()) {
            String fieldDeault = (String) fieldMap.get(field).getDefaultValue();
            if(StringUtils.isNotEmpty(fieldDeault)){
                if(fieldDeault.contains("'")) {
                    fieldDeault = fieldDeault.replace("'","");
                    fieldMap.get(field).setDefaultValue(fieldDeault);
                }
            }
        }

        Collection<String> primaryKeys = tapTable.primaryKeys();
        String sql = "CREATE TABLE \"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\"(" + CommonSqlMaker.buildColumnDefinition(tapTable, false);
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys())) {
            sql += "," + " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\" )";
        }
        // 达梦不支持 unsigned
        if(sql.contains("unsigned") || sql.contains("UNSIGNED")){
            sql = sql.replaceAll("unsigned","");
        }
        sql += ")";
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            damengContext.batchExecute(sqls);
        } catch (Throwable e) {
            TapLogger.error(TAG,"Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (damengContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                damengContext.execute("TRUNCATE TABLE \"" + damengConfig.getSchema() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (damengContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                damengContext.execute("DROP TABLE \"" + damengConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\"";
        damengContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        DamengOffset damengOffset;
        //beginning
        if (null == offsetState) {
            damengOffset = new DamengOffset(CommonSqlMaker.getOrderByUniqueKey(tapTable), 0L);
        } else {
            //with offset
            damengOffset = (DamengOffset) offsetState;
        }
        String sql = "SELECT * FROM (SELECT a.*,ROWNUM row_no FROM\"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\" a " + damengOffset.getSortString() + ") WHERE row_no>" + damengOffset.getOffsetValue();
        damengContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                assert dataMap != null;
                dataMap.remove("ROW_NO");
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    damengOffset.setOffsetValue(damengOffset.getOffsetValue() + eventBatchSize);
                    eventsOffsetConsumer.accept(tapEvents, damengOffset);
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                damengOffset.setOffsetValue(damengOffset.getOffsetValue() + tapEvents.size());
            }
            eventsOffsetConsumer.accept(tapEvents, damengOffset);
        });

    }


    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + damengConfig.getSchema() + "\".\"" + tapTable.getId() + "\" WHERE " + CommonSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                damengContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM (SELECT A.*,ROWNUM FROM \"" + damengConfig.getSchema() + "\".\"" + table.getId() + "\" A " + CommonSqlMaker.buildOracleSqlByAdvanceFilter(filter);
        damengContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (resultSet.next()) {
                List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                allColumn.remove("ROWNUM");
                filterResults.add(DbKit.getRowFromResultSet(resultSet, allColumn));
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

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = damengContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
            List<DataMap> columnList = damengContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = damengContext.queryAllIndexes(subTableNames);
            subList.forEach(subTable -> {
                //2、table name/comment
                String table = subTable.getString("TABLE_NAME");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("COMMENTS"));
                //3、primary key and table index
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("TABLE_NAME")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("INDEX_NAME"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v ->(Integer)v.get("IS_PK") == 1)) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("COLUMN_NAME")).collect(Collectors.toList()));
                    }
                    TapIndex index = new TapIndex();
                    index.setName(key);
                    List<TapIndexField> fieldList = TapSimplify.list();
                    value.forEach(v -> {
                        TapIndexField field = new TapIndexField();
                        field.setFieldAsc("ASC".equals(v.getString("DESCEND")));
                        field.setName(v.getString("COLUMN_NAME"));
                        fieldList.add(field);
                    });
                    index.setUnique(value.stream().anyMatch(v -> "UNIQUE".equals(v.getString("UNIQUENESS"))));
                    index.setPrimary(value.stream().anyMatch(v -> ((Integer) v.get("IS_PK")) == 1));
                    index.setIndexFields(fieldList);
                    tapIndexList.add(index);
                });
                //4、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("TABLE_NAME")))
                        .forEach(col -> {
                            TapField tapField = new DamengColumn(col).getTapField();
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
        damengConfig = new DamengConfig().load(connectionContext.getConnectionConfig());
        TimeZone.setDefault(TimeZone.getTimeZone(damengConfig.getTimezone()));
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(damengConfig.getConnectionString());
        try (
                DamengTest damengTest = new DamengTest(damengConfig)
        ) {
            TestItem testHostPort = damengTest.testHostPort();
            consumer.accept(testHostPort);
            if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            }
            TestItem testConnect = damengTest.testConnect();
            consumer.accept(testConnect);
            if (testConnect.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            }
        }
//        List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.ORACLE_CCJ_SQL_PARSER);
//        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return damengContext.queryAllTables(null).size();
    }

   private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {

        if (EmptyKit.isNull(cdcRunner)) {
            cdcRunner = new DamengCdcRunner(damengContext, nodeContext.getId()).init(
                    tableList,
                    nodeContext.getTableMap(),
                    offsetState,
                    recordSize,
                    consumer
            );
        }
        cdcRunner.startCdcRunner();

    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        DamengOffset damengOffset = new DamengOffset();
        if (EmptyKit.isNotNull(offsetStartTime)) {
            damengOffset.setTimestamp(offsetStartTime);
        } else {
            damengOffset.setTimestamp(System.currentTimeMillis());
        }
        damengOffset.setTimestamp(offsetStartTime);
        return damengOffset;
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) throws SQLException {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        damengContext.batchExecute(sqls);
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnAttr(damengConfig, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.dropColumn(damengConfig, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.alterColumnName(damengConfig, tapAlterFieldNameEvent);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlGenerator.addColumn(damengConfig, tapNewFieldEvent);
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = damengTest.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = damengTest.testConnection();
        consumer.accept(testConnection);
    }
}
