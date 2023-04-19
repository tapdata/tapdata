package io.tapdata.connector.tencent.db.mysql;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.*;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlMaker;
import io.tapdata.connector.mysql.entity.MySqlPartitionBinlogPosition;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.connector.tencent.db.core.TDSqlDiscoverSchema;
import io.tapdata.connector.tencent.db.core.TDSqlJdbcOneByOneWriter;
import io.tapdata.connector.tencent.db.core.TDSqlReader;
import io.tapdata.connector.tencent.db.core.TDSqlWriter;
import io.tapdata.connector.tencent.db.table.CreateTable;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.kit.DbKit;
import io.tapdata.partition.DatabaseReadPartitionSplitter;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.partition.splitter.StringCaseInsensitiveSplitter;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jackin
 * @Description
 * @create 2022-12-15 11:42
 **/
@TapConnectorClass("tencent-td-sql-spec.json")
public class TencentDBMySQLConnector extends MysqlConnector {
    private TapConnectionContext tapConnectionContext;
    private static final String TAG = TencentDBMySQLConnector.class.getSimpleName();
    private static final int MAX_FILTER_RESULT_SIZE = 100;

    private MysqlJdbcContext tdSqlJdbcContext;
    private MysqlReader tdSqlReader;
    private MysqlJdbcOneByOneWriter tdSqlJdbcOneByOneWriter;
    private String version;
    private String connectionTimezone;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlMaker ddlSqlMaker;
    Map<String, List<String>> tableTypeMap = new ConcurrentHashMap<>();
    Map<String, MysqlReader> readers = new HashMap<>();
    private AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicReference<String> tableType = new AtomicReference<>("NORMAL");

    void initStreamRead(TapConnectionContext tapConnectionContext) throws Throwable {
        for (Map.Entry<String, MysqlJdbcContext> entry : getPartitionSetMap().entrySet()) {
            readers.put(entry.getKey(), new TDSqlReader(entry.getValue()));
        }
    }

    private Map<String, MysqlJdbcContext> getPartitionSetMap() {
        Map<String, MysqlJdbcContext> contextMap = new HashMap<>();
        try {
            tdSqlJdbcContext.query("/*proxy*/ show status", rs -> {
                while (rs.next()) {
                    String statusName = rs.getString("status_name");
                    if ("set".equals(statusName)) {
                        String items = rs.getString("value");
                        if (Objects.nonNull(items)) {
                            String[] split = items.split(",");
                            for (String backend : split) {
                                if (tapConnectionContext instanceof TapConnectorContext) {
                                    MysqlJdbcContext context = new MysqlJdbcContext(tapConnectionContext).partitionSetId(backend);
                                    contextMap.put(backend.trim(), context);
                                }
                            }
                        }
                    }
                }
            });
        } catch (Throwable e) {
            TapLogger.warn(TAG, e.getMessage());
        }
        return contextMap;
    }

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        this.tapConnectionContext = tapConnectionContext;
        tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
        super.onStart(tapConnectionContext);
        this.tdSqlJdbcContext = super.initMysqlJdbcContext(tapConnectionContext);
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.tdSqlWriter = new TDSqlWriter(tdSqlJdbcContext, map -> {
                try {
                    return new TDSqlJdbcOneByOneWriter(tdSqlJdbcContext, map).type(tableType);
                } catch (Throwable throwable) {
                    return null;
                }
            });
            this.tdSqlReader = new MysqlReader(tdSqlJdbcContext);
            this.version = tdSqlJdbcContext.getMysqlVersion();
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.connectionTimezone = tdSqlJdbcContext.timezone().substring(3);
            }
        }
        ddlSqlMaker = new MysqlDDLSqlMaker(version);
        initStreamRead(tapConnectionContext);
        synchronized (this) {
            started.set(true);
        }
        //fieldDDLHandlers = new BiClassHandlers<>();
        //fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        //fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        //fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        //fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        synchronized (this) {
            started.set(false);
        }
        try {
            super.onStop(connectionContext);
            try {
                Optional.ofNullable(this.tdSqlReader).ifPresent(MysqlReader::close);
            } catch (Exception ignored) {
            }
            try {
                Optional.ofNullable(this.tdSqlWriter).ifPresent(MysqlWriter::onDestroy);
            } catch (Exception ignored) {
            }
            if (null != tdSqlJdbcContext) {
                try {
                    this.tdSqlJdbcContext.close();
                } catch (Exception e) {
                    TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
                }
            }
            if (null != readers && !readers.isEmpty()) {
                try {
                    for (Map.Entry<String, MysqlReader> entry : readers.entrySet()) {
                        String key = entry.getKey();
                        MysqlReader reader = entry.getValue();
                        MysqlJdbcContext context = reader.mysqlJdbcContext();
                        try {
                            Optional.ofNullable(reader).ifPresent(MysqlReader::close);
                        } catch (Exception e) {
                            TapLogger.warn(TAG, "can not close jdbc reader of partition set {}", key);
                        }
                        if (null != context) {
                            try {
                                context.close();
                            } catch (Exception e) {
                                TapLogger.warn(TAG, "Release jdbc context of partition set {} failed, error: {} \n {}.", key, e.getMessage(), getStackString(e));
                            }
                        }
                    }
                } finally {
                    readers = null;
                }
            }
        } finally {
            if (null != sourceConsumer && !sourceConsumer.isShutdown()) {
                sourceConsumer.shutdown();
                sourceConsumer = null;
            }
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
        databaseContext.getConnectionConfig().put("protocolType", "mysql");
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        CommonDbConfig commonDbConfig = new CommonDbConfig();
        commonDbConfig.set__connectionType(databaseContext.getConnectionConfig().getString("__connectionType"));
        try (
                TencentDBMySQLConnectorTest tencentDBMySQLConnectorTest = new TencentDBMySQLConnectorTest(
                        new MysqlJdbcContext(databaseContext),
                        databaseContext,
                        consumer,
                        commonDbConfig,
                        connectionOptions)) {
            tencentDBMySQLConnectorTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone(ZoneId.of(this.connectionTimezone)));
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(TimeZone.getTimeZone(ZoneId.of(this.connectionTimezone)));
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> {
            DateTime value = tapYearValue.getValue();
            if (value != null && value.getTimeZone() == null) {
                value.setTimeZone(TimeZone.getTimeZone(ZoneId.of(this.connectionTimezone)));
            }
            return formatTapDateTime(value, "yyyy");
        });

        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
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
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> tdSqlJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportQueryFieldMinMaxValueFunction(this::minMaxValue);
        connectorFunctions.supportGetReadPartitionsFunction(this::getReadPartitions);
    }

    private ExecutorService sourceConsumer;
    private final Object binlogLock = new Object();
    private final AtomicBoolean binlogFlag = new AtomicBoolean(true);
    private Throwable streamReadFailed = null;

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        if (!(offset instanceof MySqlPartitionBinlogPosition)){
            offset = timestampToStreamOffset(tapConnectorContext, null);
        }
        //mysqlJdbcContext.execute("/*proxy*/ set binlog_dump_sticky_backend=set_1681181636_1");
//        DataMap nodeConfig = tapConnectorContext.getConnectionConfig();
//        Object caseSensitive = nodeConfig.get("caseSensitive");
//        if (null == caseSensitive || "false".equals(caseSensitive)){
//            List<String> table = new ArrayList<>();
//            for (String t : tables) {
//                table.add(t.toLowerCase(Locale.ROOT));
//            }
//            tables.clear();
//            tables.addAll(table);
//        }

//        List<String> type = tableTypeMap.get(tapTable.getId());
//        Object database = tapConnectorContext.getConnectionConfig().get("database");
//        Object caseSensitive = tapConnectorContext.getConnectionConfig().get("caseSensitive");
//        if (null == loader){
//            synchronized (this){
//                if ( null == loader) {
//                    loader = new TDSqlDiscoverSchema(tdSqlJdbcContext).caseSensitive(caseSensitive);
//                }
//            }
//        }
//        if (type == null) {
//            type = tableTypeMap.computeIfAbsent(tapTable.getId(), id -> {
//                List<String> list = loader.getAllPartitionKey((String) database, id);
//                return list;
//            });
//        }

        try {
            consumer.streamReadStarted();
            if (sourceConsumer == null) {
                synchronized (this) {
                    if (sourceConsumer == null) {
                        int size = readers.size();
                        this.sourceConsumer = new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
                    }
                }
            }
            for (Map.Entry<String, MysqlReader> entry : readers.entrySet()) {
                String key = entry.getKey();
                MysqlReader value = entry.getValue();
                Object finalOffset = offset;
                this.sourceConsumer.execute(() -> {
                    Thread.currentThread().setUncaughtExceptionHandler((thread, throwable) -> {
                        synchronized (binlogFlag) {
                            binlogFlag.set(false);
                        }
                        streamReadFailed = throwable;
                        TapLogger.error(TAG, "Binary Log partition {} has Stoped. Stop reason: {}.", key, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
                    });
                    try {
                        Object position = null;
                        if (finalOffset instanceof MySqlPartitionBinlogPosition) {
                            position = ((MySqlPartitionBinlogPosition) finalOffset).getPosition(key);
                        }
                        value.readBinlog(
                                tapConnectorContext,
                                tables,
                                Optional.ofNullable(position).orElse(finalOffset),
                                batchSize,
                                DDLParserType.MYSQL_CCJ_SQL_PARSER,
                                consumer,
                                map(
                                        entry("tdsql.partition", key),
                                        entry("database.history.store.only.monitored.tables.ddl", true)
                                ));
                    } catch (Throwable throwable) {
                        TapLogger.error(TAG, throwable.getMessage());
                        throw new CoreException(throwable.getMessage());
                    }
                });
            }
            synchronized (binlogLock) {
                while (isAlive()) {
                    try {
                        binlogLock.wait(1500);
                    } catch (Exception ignored) {
                    }
                    synchronized (binlogFlag) {
                        if (!binlogFlag.get()) {
                            if (null != sourceConsumer && !sourceConsumer.isShutdown()) {
                                sourceConsumer.shutdown();
                                sourceConsumer = null;
                            }
                            break;
                        }
                    }
                }
            }
        } finally {
            consumer.streamReadEnded();
            synchronized (binlogLock){
                binlogLock.notifyAll();
            }
            if (null != sourceConsumer && !sourceConsumer.isShutdown()) {
                sourceConsumer.shutdown();
            }
            sourceConsumer = null;
        }
        if (streamReadFailed != null)
            throw new CoreException(TDSQLErrors.STREAM_READ_FAILED, streamReadFailed, "stream read occurred error {}", streamReadFailed.getMessage());
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        int count;
        try {
            count = tdSqlJdbcContext.count(tapTable.getName());
        } catch (Exception e) {
            throw new CoreException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
        }
        return count;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
        MysqlSnapshotOffset mysqlSnapshotOffset = offset instanceof MysqlSnapshotOffset ?
                (MysqlSnapshotOffset) offset
                : new MysqlSnapshotOffset();
        List<TapEvent>[] tempList = new ArrayList[]{new ArrayList<TapEvent>()};
        this.tdSqlReader.readWithOffset(tapConnectorContext, tapTable, mysqlSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
            TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, null, data, tapTable, "i");
            tempList[0].add(tapRecordEvent);
            if (tempList[0].size() == batchSize) {
                consumer.accept(tempList[0], mysqlSnapshotOffset);
                tempList[0].clear();
            }
        });
        if (CollectionUtils.isNotEmpty(tempList[0])) {
            consumer.accept(tempList[0], mysqlSnapshotOffset);
            tempList[0] = null;
        }
    }

    private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> before, Map<String, Object> after, TapTable tapTable, String op) {
        TapRecordEvent tapRecordEvent;
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
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
        nameFieldMap.entrySet().stream().filter(ent -> {
            TapField value = ent.getValue();
            return null != value.getDataType() && "YEAR".equals(value.getDataType().toUpperCase(Locale.ROOT));
        }).forEach(entry -> {
            warpInteger(after,entry.getKey());
            warpInteger(before, entry.getKey());
        });
        tapRecordEvent.setConnector(tapConnectorContext.getSpecification().getId());
        tapRecordEvent.setConnectorVersion(version);
        return tapRecordEvent;
    }

    private void warpInteger(Map<String, Object> warpMap, String key){
        if (null != warpMap && !warpMap.isEmpty()) {
            Object o = warpMap.get(key);
            if (o instanceof Integer) {
                warpMap.put(key, "" + o);
            }
        }
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
        String tableId = tapClearTableEvent.getTableId();
        if (tdSqlJdbcContext.tableExists(tableId)) {
            tdSqlJdbcContext.clearTable(tableId);
        } else {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", database, tableId);
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        tdSqlJdbcContext.dropTable(tapDropTableEvent.getTableId());
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            if (tdSqlJdbcContext.tableExists(tapCreateTableEvent.getTableId())) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String mysqlVersion = tdSqlJdbcContext.getMysqlVersion();
                DataMap nodeConfig = tapConnectorContext.getNodeConfig();
                SqlMaker sqlMaker = CreateTable.sqlMaker((String) nodeConfig.get("tableCreateType"),  nodeConfig.get("partitionKey"));
                if (null == tapCreateTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return createTableOptions;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
                for (String createTableSql : createTableSqls) {
                    try {
                        tdSqlJdbcContext.execute(createTableSql);
                    } catch (Throwable e) {
                        TapLogger.warn(TAG,"Create table failed, message: {}", e.getMessage());
                        throw new CoreException("Execute create table failed, sql: " + createTableSql + ", message: " + e.getMessage());
                    }
                }
                createTableOptions.setTableExists(false);
            }
            return createTableOptions;
        } catch (Throwable t) {
            TapLogger.warn(TAG,"Create table failed, message: {}", t.getMessage());
            throw new CoreException("Create table failed, message: " + t.getMessage());
        }
    }

    TDSqlDiscoverSchema loader;
    TDSqlWriter tdSqlWriter;
    private synchronized void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        List<String> type = tableTypeMap.get(tapTable.getId());
        Object database = tapConnectorContext.getConnectionConfig().get("database");
        Object caseSensitive = tapConnectorContext.getConnectionConfig().get("caseSensitive");
        if (null == loader){
            synchronized (this){
                if ( null == loader) {
                    loader = new TDSqlDiscoverSchema(tdSqlJdbcContext).caseSensitive(caseSensitive);
                }
            }
        }
        if (type == null) {
            type = tableTypeMap.computeIfAbsent(tapTable.getId(), id -> {
                List<String> list = loader.getAllPartitionKey((String) database, id);
                return list;
            });
        }


        WriteListResult<TapRecordEvent> writeListResult;
        if (type.isEmpty()) {
            synchronized (tableType){
                tableType.set(TDSqlWriter.NORMAL_TABLE);
            }
            //普通表
            writeListResult = tdSqlWriter.type(TDSqlWriter.NORMAL_TABLE).write(tapConnectorContext, tapTable, tapRecordEvents);
        } else {
            //分区表
            synchronized (tableType){
                tableType.set(TDSqlWriter.PARTITION_TABLE);
            }
            //setPartitionInfo(type, tapTable);
            if (!type.isEmpty()){
                for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
                    if (type.contains(loader.caseSensitive() ? entry.getKey() : entry.getKey().toLowerCase(Locale.ROOT))){
                        TapField value = entry.getValue();
                        value.setComment(TDSqlDiscoverSchema.PARTITION_KEY_SINGLE);
                    }
                }
            }
            writeListResult = tdSqlWriter
                    .type(TDSqlWriter.PARTITION_TABLE)
                    .write(tapConnectorContext, tapTable, tapRecordEvents);
        }
        consumer.accept(writeListResult);
    }

//    private TapTable setPartitionInfo(List<String> type, TapTable tapTable) {
//        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
//        if (!(null == nameFieldMap || nameFieldMap.isEmpty())) {
//            nameFieldMap.forEach((name, field) -> {
//                field.setComment(null != name && type.contains(name.trim()) ?
//                        TDSqlDiscoverSchema.PARTITION_KEY_SINGLE :
//                        TDSqlDiscoverSchema.PARTITION_KEY_SINGLE_NOT);
//            });
//        }
//        return tapTable;
//    }

    private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            int batchSize = MAX_FILTER_RESULT_SIZE;
            if (tapAdvanceFilter.getBatchSize() != null && tapAdvanceFilter.getBatchSize() > 0) {
                batchSize = tapAdvanceFilter.getBatchSize();
            }
            int finalBatchSize = batchSize;
            this.tdSqlReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
                filterResults.add(data);
                if (filterResults.getResults().size() == finalBatchSize) {
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

    private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent tapCreateIndexEvent) {
        List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
        SqlMaker sqlMaker = new MysqlMaker();
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        for (TapIndex tapIndex : indexList.stream().filter(v -> queryExistIndexes(database, tapTable.getId()).stream()
                .noneMatch(i -> DbKit.ignoreCreateIndex(i, v))).collect(Collectors.toList())) {
            String createIndexSql;
            try {
                createIndexSql = sqlMaker.createIndex(tapConnectorContext, tapTable, tapIndex);
            } catch (Throwable e) {
                throw new RuntimeException("Get create index sql failed, message: " + e.getMessage(), e);
            }
            try {
                this.tdSqlJdbcContext.execute(createIndexSql);
            } catch (Throwable e) {
                // mysql index  less than  3072 bytes。
                if (e.getMessage() != null && e.getMessage().contains("42000 1071")) {
                    TapLogger.warn(TAG, "Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
                } else {
                    throw new RuntimeException("Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
                }
            }
        }
    }

    private List<TapIndex> queryExistIndexes(String database, String tableName) {
        MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(tdSqlJdbcContext);
        try {
            return mysqlSchemaLoader.discoverIndexes(database, tableName);
        } catch (Throwable throwable) {
            return Collections.emptyList();
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        DataMap nodeConfig = connectionContext.getConnectionConfig();
        Object caseSensitive = nodeConfig.get("caseSensitive");
        MysqlSchemaLoader mysqlSchemaLoader = new TDSqlDiscoverSchema(tdSqlJdbcContext).caseSensitive(caseSensitive);
        mysqlSchemaLoader.discoverSchema(tables, consumer, tableSize);
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        this.tdSqlJdbcContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }

    private FieldMinMaxValue minMaxValue(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapPartitionFilter, String fieldName) {
        SqlMaker sqlMaker = new MysqlMaker();
        FieldMinMaxValue fieldMinMaxValue = FieldMinMaxValue.create().fieldName(fieldName);
        String selectSql;
        try {
            selectSql = sqlMaker.selectSql(tapConnectorContext, tapTable, TapPartitionFilter.create().fromAdvanceFilter(tapPartitionFilter));
        } catch (Throwable e) {
            throw new RuntimeException("Build sql with partition filter failed", e);
        }
        // min value
        String minSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MIN(`%s`) AS MIN_VALUE FROM", fieldName));
        AtomicReference<Object> minObj = new AtomicReference<>();
        try {
            tdSqlJdbcContext.query(minSql, rs -> {
                if (rs.next()) {
                    minObj.set(rs.getObject("MIN_VALUE"));
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("Query min value failed, sql: " + minSql, e);
        }
        Optional.ofNullable(minObj.get()).ifPresent(min -> fieldMinMaxValue.min(min).detectType(min));
        // max value
        String maxSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MAX(`%s`) AS MAX_VALUE FROM", fieldName));
        AtomicReference<Object> maxObj = new AtomicReference<>();
        try {
            tdSqlJdbcContext.query(maxSql, rs -> {
                if (rs.next()) {
                    maxObj.set(rs.getObject("MAX_VALUE"));
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("Query max value failed, sql: " + maxSql, e);
        }
        Optional.ofNullable(maxObj.get()).ifPresent(max -> fieldMinMaxValue.max(max).detectType(max));
        return fieldMinMaxValue;
    }

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions handle = super.errorHandle(tapConnectionContext, pdkMethod, throwable);
        RetryOptions retryOptions = RetryOptions.create();
        retryOptions.setNeedRetry(null == handle || handle.isNeedRetry());
        retryOptions.beforeRetryMethod(() -> {
            try {
                synchronized (this) {
                    //mysqlJdbcContext是否有效
                    if (tdSqlJdbcContext == null || !checkValid() || !started.get()) {
                        //如果无效执行onStop,有效就return
                        this.onStop(tapConnectionContext);
                        if (isAlive()) {
                            this.onStart(tapConnectionContext);
                        }
                    } else {
                        if (null != tdSqlWriter){
                            tdSqlWriter.selfCheck();
                        }
                    }
                }
            } catch (Throwable ignore) {
            }
        });
        return retryOptions;
    }

    private boolean checkValid() {
        try {
            tdSqlJdbcContext.getMysqlVersion();
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

//    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
//        RetryOptions retryOptions = RetryOptions.create();
//        if (throwable instanceof CoreException) {
//            switch (((CoreException) throwable).getCode()) {
//                case TDSQLErrors.STREAM_READ_FAILED:
//                    retryOptions.needRetry(false);
//                    break;
//                default:
//                    retryOptions.setNeedRetry(true);
//                    retryOptions.beforeRetryMethod(() -> {
//                        try {
//                            this.onStart(tapConnectionContext);
//                        } catch (Throwable ignore) {
//                        }
//                    });
//                    break;
//            }
//        }
//        return retryOptions;
//    }

    private void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) {
        DatabaseReadPartitionSplitter.calculateDatabaseReadPartitions(connectorContext, table, options)
                .queryFieldMinMaxValue(this::minMaxValue)
                .typeSplitterMap(options.getTypeSplitterMap().registerSplitter(TypeSplitterMap.TYPE_STRING, StringCaseInsensitiveSplitter.INSTANCE))
                .startSplitting();
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(tdSqlJdbcContext);
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
                tdSqlJdbcContext.execute(sql);
            } catch (Throwable e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            Map<String, MysqlBinlogPosition> positionMap = new HashMap<>();
            for (Map.Entry<String, MysqlJdbcContext> entry : getPartitionSetMap().entrySet()) {
                positionMap.put(entry.getKey(), entry.getValue().readBinlogPosition());
            }
            return new MySqlPartitionBinlogPosition(positionMap);
        }
        return startTime;
    }
}
