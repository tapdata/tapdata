package io.tapdata.connector.mariadb;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.mariadb.ddl.DDLFactory;
import io.tapdata.connector.mariadb.ddl.DDLParserType;
import io.tapdata.connector.mariadb.ddl.DDLSqlMaker;
import io.tapdata.connector.mariadb.ddl.sqlmaker.MariadbDDLSqlMaker;
import io.tapdata.connector.mariadb.entity.MariadbSnapshotOffset;
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
import io.tapdata.entity.utils.cache.KVMap;
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
    private static final DDLParserType DDL_PARSER_TYPE = DDLParserType.CCJ_SQL_PARSER;
    private MariadbContext mariadbContext;
    private MariadbReader mariadbReader;
    private MariadbWriter mariadbWriter;
    private String version;
    private String connectionTimezone;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlMaker ddlSqlMaker;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        this.mariadbContext = new MariadbContext(tapConnectionContext);
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mariadbWriter = new MariadbOneByOneWriter(mariadbContext);
            this.mariadbReader = new MariadbReader(mariadbContext);
            this.version = mariadbContext.getMariadbVersion();
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.connectionTimezone = mariadbContext.timezone();
            }
        }
        ddlSqlMaker = new MariadbDDLSqlMaker(version);
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
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SSSSSS"));
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
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::query);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportReleaseExternalFunction(this::releaseExternal);
    }

    private void releaseExternal(TapConnectorContext tapConnectorContext) {
        try {
            KVMap<Object> stateMap = tapConnectorContext.getStateMap();
            if (null != stateMap) {
                stateMap.clear();
            }
        } catch (Throwable throwable) {
            TapLogger.warn(TAG, "Release mysql state map failed, error: " + throwable.getMessage());
        }
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        MariadbSchemaLoader mysqlSchemaLoader = new MariadbSchemaLoader(mariadbContext);
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
                mariadbContext.execute(sql);
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
        return ddlSqlMaker.alterColumnName(tapConnectorContext, tapAlterFieldNameEvent, mariadbContext);
    }

    private List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        return ddlSqlMaker.addColumn(tapConnectorContext, tapNewFieldEvent);
    }

    private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent
            tapCreateIndexEvent) throws Throwable {
        List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
        SqlMaker sqlMaker = new MariadbMaker();
        for (TapIndex tapIndex : indexList) {
            String createIndexSql;
            try {
                createIndexSql = sqlMaker.createIndex(tapConnectorContext, tapTable, tapIndex);
            } catch (Throwable e) {
                throw new RuntimeException("Get create index sql failed, message: " + e.getMessage(), e);
            }
            try {
                this.mariadbContext.execute(createIndexSql);
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
        this.mariadbContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        try {
            this.mariadbContext.close();
        } catch (Exception e) {
            TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
        }
        Optional.ofNullable(this.mariadbReader).ifPresent(MariadbReader::close);
        Optional.ofNullable(this.mariadbWriter).ifPresent(MariadbWriter::onDestroy);
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if (mariadbContext.tableExists(tableId)) {
            mariadbContext.clearTable(tableId);
        } else {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", database, tableId);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        mariadbContext.dropTable(tapDropTableEvent.getTableId());
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        try {
            if (mariadbContext.tableExists(tapCreateTableEvent.getTableId())) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String mariadbVersion = mariadbContext.getMariadbVersion();
                SqlMaker sqlMaker = new MariadbMaker();
                if (null == tapCreateTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mariadbVersion);
                for (String createTableSql : createTableSqls) {
                    try {
                        mariadbContext.execute(createTableSql);
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
        WriteListResult<TapRecordEvent> writeListResult = this.mariadbWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
        MariadbSnapshotOffset mariadbSnapshotOffset;
        if (offset instanceof MariadbSnapshotOffset) {
			mariadbSnapshotOffset = (MariadbSnapshotOffset) offset;
        } else {
			mariadbSnapshotOffset = new MariadbSnapshotOffset();
        }
        List<TapEvent> tempList = new ArrayList<>();
        this.mariadbReader.readWithOffset(tapConnectorContext, tapTable, mariadbSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
            TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, null, data, tapTable, "i");
            tempList.add(tapRecordEvent);
            if (tempList.size() == batchSize) {
                consumer.accept(tempList, mariadbSnapshotOffset);
                tempList.clear();
            }
        });
        if (CollectionUtils.isNotEmpty(tempList)) {
            consumer.accept(tempList, mariadbSnapshotOffset);
            tempList.clear();
        }
    }

    private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter
            tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) throws Throwable {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            this.mariadbReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
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
		mariadbReader.readBinlog(tapConnectorContext, tables, offset, batchSize, DDL_PARSER_TYPE, consumer);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        int count;
        try {
            count = mariadbContext.count(tapTable.getName());
        } catch (Exception e) {
            throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
        }
        return count;
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

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        MariadbSchemaLoader mysqlSchemaLoader = new MariadbSchemaLoader(mariadbContext);
        mysqlSchemaLoader.discoverSchema(tables, consumer, tableSize);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
        onStart(databaseContext);
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        MariadbConnectionTest mysqlConnectionTest = new MariadbConnectionTest(mariadbContext);
        TestItem testHostPort = mysqlConnectionTest.testHostPort(databaseContext);
        consumer.accept(testHostPort);
        if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testConnect = mysqlConnectionTest.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem testDatabaseVersion = mysqlConnectionTest.testDatabaseVersion();
        consumer.accept(testDatabaseVersion);
        if (testDatabaseVersion.getResult() == TestItem.RESULT_FAILED) {
            return null;
        }
        TestItem binlogMode = mysqlConnectionTest.testBinlogMode();
        TestItem binlogRowImage = mysqlConnectionTest.testBinlogRowImage();
        TestItem cdcPrivileges = mysqlConnectionTest.testCDCPrivileges();
        consumer.accept(binlogMode);
        consumer.accept(binlogRowImage);
        consumer.accept(cdcPrivileges);
        consumer.accept(mysqlConnectionTest.testCreateTablePrivilege(databaseContext));
        if (binlogMode.isSuccess() && binlogRowImage.isSuccess() && cdcPrivileges.isSuccess()) {
            List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDL_PARSER_TYPE);
            ddlCapabilities.forEach(connectionOptions::capability);
        }
        return connectionOptions;
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            return this.mariadbContext.readBinlogPosition();
        } else {
            throw new NotSupportedException();
        }
    }
}

