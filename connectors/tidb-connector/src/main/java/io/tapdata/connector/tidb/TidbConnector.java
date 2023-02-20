package io.tapdata.connector.tidb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.kafka.KafkaService;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.mysql.SqlMaker;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.ddl.TidbSqlMaker;
import io.tapdata.connector.tidb.dml.TidbReader;
import io.tapdata.connector.tidb.dml.TidbRecordWrite;
import io.tapdata.connector.tidb.snapshot.SnapshotOffset;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.connector.tidb.util.pojo.Changefeed;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@TapConnectorClass("spec_tidb.json")
public class TidbConnector extends ConnectorBase {
    private static final String TAG = TidbConnector.class.getSimpleName();
    private TidbConfig tidbConfig;
    private TidbContext tidbContext;
    private KafkaService kafkaService;
    private TidbReader tidbReader;
    private String connectionTimezone;
    private TidbSqlMaker tidbSqlMaker;
    private TidbConnectionTest tidbConnectionTest;
    private String version;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private static final int MAX_FILTER_RESULT_SIZE = 100;

    Map<String, Object> map;
    HttpUtil httpUtil = new HttpUtil();

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        this.tidbConfig = (TidbConfig) new TidbConfig().load(tapConnectionContext.getConnectionConfig());

        this.tidbConnectionTest = new TidbConnectionTest(tidbConfig, testItem -> {
        }, null);
        if (EmptyKit.isNull(tidbContext) || tidbContext.isFinish()) {
            tidbContext = (TidbContext) DataSourcePool.getJdbcContext(tidbConfig, TidbContext.class, tapConnectionContext.getId());
        }
        this.tidbReader = new TidbReader(tidbContext);
        this.version = tidbContext.queryVersion();
        this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = tidbContext.timezone();
        }
        tidbSqlMaker = new TidbSqlMaker();

        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        // target functions
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        //connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> tidbContext.getConnection(), c));

        // source functions
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);


        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });

        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> {
            if (tapYearValue.getValue() != null && tapYearValue.getValue().getTimeZone() == null) {
                tapYearValue.getValue().setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            }
            return formatTapDateTime(tapYearValue.getValue(), "yyyy");
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        codecRegistry.registerFromTapValue(TapMapValue.class, "longtext", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "longtext", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        KafkaConfig kafkaConfig = (KafkaConfig) new KafkaConfig().load(nodeContext.getConnectionConfig());
        Changefeed changefeed = new Changefeed();
        changefeed.setSinkUri("kafka://" + kafkaConfig.getNameSrvAddr() + "/" + tidbConfig.getMqTopic() + "?" + "kafka-version=2.4.0&partition-num=1&max-message-bytes=67108864&replication-factor=1&protocol=canal-json&auto-create-topic=true");
        changefeed.setChangefeedId(tidbConfig.getChangefeedId());
        changefeed.setForceReplicate(true);
        changefeed.setSyncDdl(true);
        if (httpUtil.createChangefeed(changefeed, tidbConfig.getTicdcUrl())) {
            kafkaService = new KafkaService(kafkaConfig);
            kafkaService.streamConsume(tableList, recordSize, consumer);
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        offsetStartTime = System.currentTimeMillis();
        return (Object) offsetStartTime;

    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        SnapshotOffset snapshotOffset;
        // completely blank task
        if (null == offsetState) {
            snapshotOffset = new SnapshotOffset(TidbSqlMaker.getOrderByUniqueKey(tapTable), 0L);
        }
        //with offset
        else {
            snapshotOffset = (SnapshotOffset) offsetState;
        }

        String sql;
        if (version.contains("2008")) {
            sql = String.format("SELECT * FROM(SELECT ROW_NUMBER() OVER(%s) rowNumber, * FROM %s) myTable WHERE rowNumber > %s",
                    snapshotOffset.getSortString(), TidbSqlMaker.formatTableName(tapTable, tidbConfig), snapshotOffset.getOffsetValue());
        } else {
            // OFFSET is not supported until SQL Server 2012
            sql = "SELECT * FROM " + TidbSqlMaker.formatTableName(tapTable, tidbConfig) + snapshotOffset.getSortString();//+ " OFFSET " + snapshotOffset.getOffsetValue() + " ROWS";
        }

        tidbContext.query(sql, rs -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(rs);
            while (isAlive() && rs.next()) {

                tapEvents.add(insertRecordEvent(DbKit.getRowFromResultSet(rs, columnNames), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    long timestamp = System.currentTimeMillis();
                    snapshotOffset.setOffsetValue(snapshotOffset.getOffsetValue() + eventBatchSize);
                    snapshotOffset.tableOffset(tapTable.getId(), timestamp).timestamp(timestamp);
                    eventsOffsetConsumer.accept(tapEvents, snapshotOffset);
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                long timestamp = System.currentTimeMillis();
                snapshotOffset.setOffsetValue(snapshotOffset.getOffsetValue() + tapEvents.size());
                snapshotOffset.tableOffset(tapTable.getId(), timestamp).timestamp(timestamp);
                eventsOffsetConsumer.accept(tapEvents, snapshotOffset);
            }
        });

    }

    private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> before, Map<String, Object> after, TapTable tapTable, String op) {
        TapRecordEvent tapRecordEvent;
        switch (op) {
            case "i":
                tapRecordEvent = TapSimplify.insertRecordEvent(after, tapTable.getId());
                break;
            case "u":
                tapRecordEvent = TapSimplify.updateDMLEvent(before, after, tapTable.getId());
                break;
            case "d":
                tapRecordEvent = TapSimplify.deleteDMLEvent(before, tapTable.getId());
                break;
            default:
                throw new IllegalArgumentException("Operation " + op + " not support");
        }
        tapRecordEvent.setConnector(tapConnectorContext.getSpecification().getId());
        tapRecordEvent.setConnectorVersion(version);
        return tapRecordEvent;
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        int count;
        try {
            count = tidbContext.count(tapTable.getName());
        } catch (Exception e) {
            throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
        }
        return count;
    }


    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        tidbContext.queryAllTables(list(), batchSize, listConsumer);
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                tidbContext.execute(sql);
            } catch (Throwable e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    private List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        return tidbSqlMaker.alterColumnAttr(tapConnectorContext, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return tidbSqlMaker.dropColumn(tapConnectorContext, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return tidbSqlMaker.alterColumnName(tapConnectorContext, tapAlterFieldNameEvent);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return tidbSqlMaker.addColumn(tapConnectorContext, tapNewFieldEvent);
    }

    private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent tapCreateIndexEvent) throws Throwable {
        List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
        SqlMaker sqlMaker = new TidbSqlMaker();
        for (TapIndex tapIndex : indexList) {
            String createIndexSql;
            try {
                createIndexSql = sqlMaker.createIndex(tapConnectorContext, tapTable, tapIndex);
            } catch (Throwable e) {
                throw new RuntimeException("Get create index sql failed, message: " + e.getMessage(), e);
            }
            try {
                tidbContext.execute(createIndexSql);
            } catch (Throwable e) {
                // tidb index  less than  3072 bytes。
                if (e.getMessage() != null && e.getMessage().contains("42000 1071")) {
                    TapLogger.warn(TAG, "Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
                } else {
                    throw new RuntimeException("Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        tidbContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(tidbContext)) {
            tidbContext.finish(connectionContext.getId());
        }
        if (EmptyKit.isNotNull(tidbConnectionTest)) {
            tidbConnectionTest.close();
        }
        if (EmptyKit.isNotNull(kafkaService)) {
            kafkaService.close();
        }
        if (EmptyKit.isNotNull(connectionContext.getConnectionConfig().get("changefeedId"))) {
            try {
                httpUtil.deleteChangefeed((String) connectionContext.getConnectionConfig().get("changefeedId"),
                        (String) connectionContext.getConnectionConfig().get("ticdcUrl"));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        try {
            if (tidbContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                tidbContext.clearTable(tapClearTableEvent.getTableId());
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, e.getMessage());
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        try {
            tidbContext.dropTable(tapDropTableEvent.getTableId());

        } catch (Throwable e) {
            TapLogger.error(TAG, e.getMessage());
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            TapTable tapTable = tapCreateTableEvent.getTable();
            if (tidbContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
                createTableOptions.setTableExists(true);
                return createTableOptions;
            } else {
                String createTableSqls = tidbSqlMaker.createTable(tapConnectorContext, tapCreateTableEvent);
                try {
                    tidbContext.execute(createTableSqls);
                } catch (Throwable e) {
                    throw new Exception("Execute create table failed, sql: " + createTableSqls + ", message: " + e.getMessage(), e);
                }
            }
        } catch (Throwable t) {
            throw new Exception("Create table failed, message: " + t.getMessage(), t);
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }


    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        new TidbRecordWrite(tidbContext, tapTable).write(tapRecordEvents, consumer);
    }


    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<DataMap> tableList = tidbContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        TableFieldTypesGenerator instance = InstanceFactory.instance(TableFieldTypesGenerator.class);
        DefaultExpressionMatchingMap dataTypesMap = connectionContext.getSpecification().getDataTypesMap();
        tableLists.forEach(subList -> {
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
            List<DataMap> columnList = tidbContext.queryAllColumns(subTableNames);
            List<DataMap> indexList = tidbContext.queryAllIndexes(subTableNames);

            Map<String, List<DataMap>> columnMap = Maps.newHashMap();
            if (CollectionUtils.isNotEmpty(columnList)) {
                columnMap = columnList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
            }
            Map<String, List<DataMap>> indexMap = Maps.newHashMap();
            if (CollectionUtils.isNotEmpty(indexList)) {
                indexMap = indexList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
            }

            Map<String, List<DataMap>> finalColumnMap = columnMap;
            Map<String, List<DataMap>> finalIndexMap = indexMap;

            List<TapTable> tempList = new ArrayList<>();
            subList.forEach(table -> {
                String tableName = table.getString("TABLE_NAME");
                TapTable tapTable = TapSimplify.table(tableName);

                tidbContext.discoverFields(finalColumnMap.get(tableName), tapTable, instance, dataTypesMap);
                tidbContext.discoverIndexes(finalIndexMap.get(tableName), tapTable);
                tempList.add(tapTable);
            });

            if (CollectionUtils.isNotEmpty(columnList)) {
                consumer.accept(tempList);
                tempList.clear();
            }
        });
    }


    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
        tidbConfig = (TidbConfig) new TidbConfig().load(databaseContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(tidbConfig.getConnectionString());
        try (
                TidbConnectionTest connectionTest = new TidbConnectionTest(tidbConfig, consumer, connectionOptions)
        ) {
            connectionTest.testOneByOne();
        }
        //TODO ask Jarad!
//        List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MSSQL_CCJ_SQL_PARSER);
//        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = tidbConnectionTest.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = tidbConnectionTest.testConnection();
        consumer.accept(testConnection);
    }

    private void queryByAdvanceFilter(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            tidbReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
                filterResults.add(data);
                if (filterResults.getResults().size() == MAX_FILTER_RESULT_SIZE) {
                    consumer.accept(filterResults);
                    filterResults.getResults().clear();
                }
            });
            if (CollectionUtils.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
                filterResults.getResults().clear();
            }
        } catch (Throwable e) {
            filterResults.setError(e);
            consumer.accept(filterResults);
        }
    }
}

