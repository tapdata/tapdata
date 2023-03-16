package io.tapdata.connector.mariadb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.MysqlMaker;
import io.tapdata.connector.mysql.MysqlReader;
import io.tapdata.connector.mysql.MysqlSchemaLoader;
import io.tapdata.connector.mysql.SqlMaker;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlMaker;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
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
import io.tapdata.pdk.apis.error.NotSupportedException;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;


@TapConnectorClass("spec_mariadb.json")
public class MariadbConnector extends ConnectorBase {
    private static final String TAG = MariadbConnector.class.getSimpleName();
    private static final int MAX_FILTER_RESULT_SIZE = 100;
    private static final DDLParserType DDL_PARSER_TYPE = DDLParserType.MYSQL_CCJ_SQL_PARSER;
    private MysqlJdbcContext mysqlJdbcContext;
    private MysqlReader mysqlReader;
    private MysqlWriter mysqlWriter;
    private String version;
    private String connectionTimezone;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlMaker ddlSqlMaker;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
        this.mysqlJdbcContext = new MysqlJdbcContext(tapConnectionContext);
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext);
            this.mysqlReader = new MysqlReader(mysqlJdbcContext);
            this.version = mysqlJdbcContext.getMysqlVersion();
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.connectionTimezone = mysqlJdbcContext.timezone();
            }
        }
        ddlSqlMaker = new MysqlDDLSqlMaker(version);
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));

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

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::query);
        connectorFunctions.supportWriteRecord(this::writeRecord);
//        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> mysqlJdbcContext.getConnection(), c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
    }

    private void runRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable, int eventBatchSize, Consumer<List<TapEvent>> eventsOffsetConsumer) throws Throwable {
        mysqlJdbcContext.query(command, resultSet -> {
            List<TapEvent> tapEvents = list();
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            while (isAlive() && resultSet.next()) {
                DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                assert dataMap != null;
                tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents);
                    tapEvents = list();
                }
            }
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents);
            }
        });
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(mysqlJdbcContext);
        mysqlSchemaLoader.getTableNames(tapConnectionContext, batchSize, listConsumer);
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                mysqlJdbcContext.execute(sql);
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
        return ddlSqlMaker.alterColumnAttr(tapConnectorContext, tapAlterFieldAttributesEvent);
    }

    private List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        return ddlSqlMaker.dropColumn(tapConnectorContext, tapDropFieldEvent);
    }

    private List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        return ddlSqlMaker.alterColumnName(tapConnectorContext, tapAlterFieldNameEvent);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlMaker.addColumn(tapConnectorContext, tapNewFieldEvent);
    }

    private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent tapCreateIndexEvent) {
        List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
        SqlMaker sqlMaker = new MysqlMaker();
        for (TapIndex tapIndex : indexList) {
            String createIndexSql;
            try {
                createIndexSql = sqlMaker.createIndex(tapConnectorContext, tapTable, tapIndex);
            } catch (Throwable e) {
                throw new RuntimeException("Get create index sql failed, message: " + e.getMessage(), e);
            }
            try {
                this.mysqlJdbcContext.execute(createIndexSql);
            } catch (Throwable e) {
                throw new RuntimeException("Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        this.mysqlJdbcContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            this.mysqlJdbcContext.close();
        } catch (Exception e) {
            TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
        }
        Optional.ofNullable(this.mysqlReader).ifPresent(MysqlReader::close);
        Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if (mysqlJdbcContext.tableExists(tableId)) {
            mysqlJdbcContext.clearTable(tableId);
        } else {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", database, tableId);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        mysqlJdbcContext.dropTable(tapDropTableEvent.getTableId());
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        try {
            if (mysqlJdbcContext.tableExists(tapCreateTableEvent.getTableId())) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String mysqlVersion = mysqlJdbcContext.getMysqlVersion();
                SqlMaker sqlMaker = new MysqlMaker();
                if (null == tapCreateTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
                for (String createTableSql : createTableSqls) {
                    try {
                        mysqlJdbcContext.execute(createTableSql);
                    } catch (Throwable e) {
                        throw new Exception("Execute create table failed, sql: " + createTableSql + ", message: " + e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable t) {
            throw new Exception("Create table failed, message: " + t.getMessage(), t);
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
        MysqlSnapshotOffset mysqlSnapshotOffset;
        if (offset instanceof MysqlSnapshotOffset) {
            mysqlSnapshotOffset = (MysqlSnapshotOffset) offset;
        } else {
            mysqlSnapshotOffset = new MysqlSnapshotOffset();
        }
        List<TapEvent> tempList = new ArrayList<>();
        this.mysqlReader.readWithOffset(tapConnectorContext, tapTable, mysqlSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
            TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, data, tapTable);
            tempList.add(tapRecordEvent);
            if (tempList.size() == batchSize) {
                consumer.accept(tempList, mysqlSnapshotOffset);
                tempList.clear();
            }
        });
        if (CollectionUtils.isNotEmpty(tempList)) {
            consumer.accept(tempList, mysqlSnapshotOffset);
            tempList.clear();
        }
    }

    private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            this.mysqlReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
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

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        mysqlReader.readBinlog(tapConnectorContext, tables, offset, batchSize, DDL_PARSER_TYPE, consumer);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        int count;
        try {
            count = mysqlJdbcContext.count(tapTable.getName());
        } catch (Exception e) {
            throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
        }
        return count;
    }

    private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> after, TapTable tapTable) {
        TapRecordEvent tapRecordEvent;
        tapRecordEvent = TapSimplify.insertRecordEvent(after, tapTable.getId());
        tapRecordEvent.setConnector(tapConnectorContext.getSpecification().getId());
        tapRecordEvent.setConnectorVersion(version);
        return tapRecordEvent;
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(mysqlJdbcContext);
        mysqlSchemaLoader.discoverSchema(tables, consumer, tableSize);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
		databaseContext.getConnectionConfig().put("protocolType", "mysql");
        CommonDbConfig commonDbConfig = new CommonDbConfig();
        commonDbConfig.set__connectionType(databaseContext.getConnectionConfig().getString("__connectionType"));
        try (
                MariadbConnectionTest mariadbConnectionTest = new MariadbConnectionTest(new MysqlJdbcContext(databaseContext),
                        databaseContext, consumer, commonDbConfig, connectionOptions);
        ) {
            mariadbConnectionTest.testOneByOne();
            return connectionOptions;
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            return this.mysqlJdbcContext.readBinlogPosition();
        } else {
            throw new NotSupportedException();
        }
    }
}

