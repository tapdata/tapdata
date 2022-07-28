package io.tapdata.connector.clickhouse;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.connector.clickhouse.ddl.DDLSqlMaker;
import io.tapdata.connector.clickhouse.ddl.sqlmaker.ClickhouseDDLSqlMaker;
import io.tapdata.connector.clickhouse.dml.ClickhouseWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_clickhouse.json")
public class ClickhouseConnector extends ConnectorBase {


    public static final String TAG = ClickhouseConnector.class.getSimpleName();

    private ClickhouseConfig clickhouseConfig;

    private ClickhouseJdbcContext clickhouseJdbcContext;

    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;

    private String connectionTimezone;

    private DDLSqlMaker ddlSqlMaker;

    private ClickhouseWriter clickhouseWriter;


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
        ddlSqlMaker = new ClickhouseDDLSqlMaker();

    }

    private void initConnection(TapConnectionContext connectionContext) throws Throwable {
        clickhouseConfig= (ClickhouseConfig) new ClickhouseConfig().load(connectionContext.getConnectionConfig());
        if (EmptyKit.isNull(clickhouseJdbcContext) || clickhouseJdbcContext.isFinish()) {
            clickhouseJdbcContext = (ClickhouseJdbcContext) DataSourcePool.getJdbcContext(clickhouseConfig, ClickhouseJdbcContext.class, connectionContext.getId());
        }
//        clickhouseVersion = clickhouseJdbcContext.queryVersion();
        this.connectionTimezone = connectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = clickhouseJdbcContext.timezone();
        }
        this.clickhouseWriter = new ClickhouseWriter(clickhouseJdbcContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<DataMap> tableList = clickhouseJdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        try {
            tableLists.forEach(subList->{
                List<TapTable> tapTableList = TapSimplify.list();
                List<String> subTableNames = subList.stream().map(v -> v.getString("name")).collect(Collectors.toList());
                List<DataMap> columnList = clickhouseJdbcContext.queryAllColumns(subTableNames);

                AtomicInteger primaryPos = new AtomicInteger(1);
                subList.forEach(subTable->{
                    //1.table name/comment
                    String table = subTable.getString("name");
                    TapTable tapTable = table(table);
                    tapTable.setComment(subTable.getString("comment"));
                    List<String> primaryKey = TapSimplify.list();
                    columnList.stream().filter(col->table.equals(col.getString("table")))
                            .forEach(col->{
                                String columnName = col.getString("name");
                                String columnType = col.getString("type");
                                Boolean nullable = false;
                                if (columnType.contains("Nullable")) {
                                    columnType = columnType.replace("Nullable(", "");
                                    columnType = columnType.substring(0, columnType.length() - 1);
                                    nullable = true;
                                }
                                TapField field = TapSimplify.field(columnName, columnType);
                                field.nullable(nullable);
                                int ordinalPosition = Integer.parseInt(col.getString("position"));
                                field.pos(ordinalPosition);

    //                            String is_in_sorting_key = col.getString("is_in_sorting_key");
                                String is_in_primary_key = col.getString("is_in_primary_key");
                                if (Integer.parseInt(is_in_primary_key) == 1) {
                                    field.setPrimaryKey(true);
                                    field.primaryKeyPos(primaryPos.getAndIncrement());
                                }
                                tapTable.add(field);
                            });
                    tapTableList.add(tapTable);
                });
                if (CollectionUtils.isNotEmpty(columnList)) {
                    consumer.accept(tapTableList);
                    tapTableList.clear();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(clickhouseJdbcContext)) {
            clickhouseJdbcContext.finish(connectionContext.getId());
        }
        Optional.ofNullable(this.clickhouseWriter).ifPresent(ClickhouseWriter::onDestroy);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

        codecRegistry.registerFromTapValue(TapRawValue.class, "String", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "String", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "String", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "UInt8", TapValue::getValue);


        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "String", tapValue -> {
            if(tapValue != null && tapValue.getValue() != null) {
                return new String(Base64.encodeBase64(tapValue.getValue()));
            }
            return null;
        });

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
//        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            DateTime datetime = tapDateTimeValue.getValue();
            datetime.setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return datetime.toTimestamp();
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            DateTime datetime = tapDateValue.getValue();
            datetime.setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return datetime.toSqlDate();});

        //target
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportWriteRecord(this::writeRecord);



        //source 暂未找到 增量方案，1.x 不支持源
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        //query
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {

        TapTable tapTable = tapCreateTableEvent.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        String sql = "CREATE TABLE IF NOT EXISTS \"" + clickhouseConfig.getDatabase() + "\".\"" + tapTable.getId() + "\"(" + ClickhouseDDLSqlMaker.buildColumnDefinition(tapTable, true);
        sql = sql.substring(0, sql.length() - 1) + ") ENGINE = MergeTree ";
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sql += " PRIMARY KEY (\"" + String.join("\",\"", primaryKeys) + "\")";
        } else {
            sql += " order by tuple()";
        }
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            clickhouseJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                clickhouseJdbcContext.execute(sql);
            } catch (SQLException e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.clickhouseWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    //需要改写成ck的创建索引方式
    private void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) {
        try {
            List<String> sqls = TapSimplify.list();
            if (EmptyKit.isNotEmpty(createIndexEvent.getIndexList())) {
                createIndexEvent.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                (EmptyKit.isNotNull(i.getName()) ? "IF NOT EXISTS \"" + i.getName() + "\"" : "") + " ON \"" + clickhouseConfig.getDatabase() + "\".\"" + tapTable.getId() + "\"(" +
                                i.getIndexFields().stream().map(f -> "\"" + f.getName() + "\" " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                        .collect(Collectors.joining(",")) + ')'));
            }
            clickhouseJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Indexes for " + tapTable.getId() + " Failed! " + e.getMessage());
        }

    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = "SELECT * FROM \"" + clickhouseConfig.getDatabase() + "\".\"" + table.getId() + "\" " + CommonSqlMaker.buildSqlByAdvanceFilter(filter);
        clickhouseJdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (resultSet!=null && resultSet.next()) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                filterResults.getResults().stream().forEach(l->l.entrySet().forEach(v->{
                    if (v.getValue() instanceof String) {
                        v.setValue(((String) v.getValue()).trim());
                    }
                }));
                consumer.accept(filterResults);
            }
        });
    }


    // 不支持偏移量
    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = "SELECT * FROM \"" + clickhouseConfig.getDatabase() + "\".\"" + tapTable.getId();
        clickhouseJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(resultSet, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, null);
                    tapEvents = list();
                }
            }
            eventsOffsetConsumer.accept(tapEvents, null);
        });

    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        AtomicLong count = new AtomicLong(0);
        String sql = "SELECT COUNT(1) FROM \"" + clickhouseConfig.getDatabase() + "\".\"" + tapTable.getId() + "\"";
        clickhouseJdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
        return count.get();
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (clickhouseJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                clickhouseJdbcContext.execute("TRUNCATE TABLE \"" + clickhouseConfig.getDatabase() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (clickhouseJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                clickhouseJdbcContext.execute("DROP TABLE IF EXISTS \"" + clickhouseConfig.getDatabase() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }



    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        clickhouseConfig = (ClickhouseConfig) new ClickhouseConfig().load(connectionContext.getConnectionConfig());
        ClickhouseTest clickhouseTest = new ClickhouseTest(clickhouseConfig);
        TestItem testHostPort = clickhouseTest.testHostPort();
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testConnect = clickhouseTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        //Read test
        //TODO execute read test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        clickhouseTest.close();
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return clickhouseJdbcContext.queryAllTables(null).size();
    }
}
