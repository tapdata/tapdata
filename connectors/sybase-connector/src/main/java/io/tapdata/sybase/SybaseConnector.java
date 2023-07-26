package io.tapdata.sybase;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.MysqlMaker;
import io.tapdata.connector.mysql.MysqlReader;
import io.tapdata.connector.mysql.SqlMaker;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.cdc.service.CdcHandle;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.extend.SybaseColumn;
import io.tapdata.sybase.extend.SybaseConfig;
import io.tapdata.sybase.extend.SybaseConnectionTest;
import io.tapdata.sybase.extend.SybaseContext;
import io.tapdata.sybase.extend.SybaseReader;
import io.tapdata.sybase.extend.SybaseSqlBatchWriter;
import io.tapdata.sybase.util.Code;
import io.tapdata.sybase.util.Utils;
import org.apache.commons.io.FilenameUtils;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec.json")
public class SybaseConnector extends CommonDbConnector {
    private static final String TAG = SybaseConnector.class.getSimpleName();

    private SybaseContext sybaseContext;
    protected SybaseConfig sybaseConfig;
    private MysqlReader mysqlReader;
    private MysqlWriter mysqlWriter;
    private String version;
    private TimeZone timezone;
    private CdcHandle cdcHandle;
    private StopLock lock;
    private CdcRoot root;
    private OverwriteType overwriteType;
    private Boolean isCdc = new Boolean(false);
    private ConnectionConfig connectionConfig;
    private NodeConfig nodeConfig;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private Log log;


    String encode;
    String decode;
    String outCode;
    boolean needEncode = false;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        sybaseConfig = new SybaseConfig().load(tapConnectionContext.getConnectionConfig());
        sybaseContext = new SybaseContext(sybaseConfig);
        commonDbConfig = sybaseConfig;
        jdbcContext = sybaseContext;
        commonSqlMaker = new CommonSqlMaker('`');
        exceptionCollector = new MysqlExceptionCollector();
        log = tapConnectionContext.getLog();
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new SybaseSqlBatchWriter(sybaseContext);
            this.mysqlReader = new SybaseReader(sybaseContext);
            this.version = sybaseContext.queryVersion();
            //this.timezone = sybaseContext.queryTimeZone();
            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
            root = new CdcRoot(unused -> isAlive());
            lock = new StopLock(true);
            KVMap<Object> stateMap = ((TapConnectorContext) tapConnectionContext).getStateMap();
            Object taskId = stateMap.get("taskId");
            if (null == taskId) {
                String id = ((TapConnectorContext) tapConnectionContext).getId();
                if (null == id) {
                    id = UUID.randomUUID().toString().replaceAll("-", "_");
                }
                stateMap.put("taskId", id.substring(0, 15));
            }
            connectionConfig = new ConnectionConfig(tapConnectionContext);
            nodeConfig = new NodeConfig((TapConnectorContext) tapConnectionContext);

            overwriteType = Optional.ofNullable(OverwriteType.type(String.valueOf(stateMap.get("tableOverType")))).orElse(OverwriteType.OVERWRITE);
            stateMap.put("tableOverType", overwriteType.getType());

            needEncode = nodeConfig.isAutoEncode();
            encode = needEncode ? Optional.ofNullable(nodeConfig.getEncode()).orElse("cp850") : null;
            decode = needEncode ? Optional.ofNullable(nodeConfig.getDecode()).orElse("big5") : null;
            outCode = needEncode ? Optional.ofNullable(nodeConfig.getOutDecode()).orElse("utf-8") : null;
        }
//		fieldDDLHandlers = new BiClassHandlers<>();
//		fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
//		fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
//		fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
//		fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        started.set(true);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        started.set(false);
        try {
            Optional.ofNullable(this.mysqlReader).ifPresent(MysqlReader::close);
        } catch (Exception ignored) {
        }
        try {
            Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
        } catch (Exception ignored) {
        }
        if (null != sybaseContext) {
            try {
                this.sybaseContext.close();
                this.sybaseContext = null;
            } catch (Exception e) {
                TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }

        Optional.ofNullable(lock).ifPresent(StopLock::stop);
        if (root != null) {
            root.notifyAll();
        }
    }

    private void release(TapConnectorContext context) {
        if (null == cdcHandle)
            cdcHandle = new CdcHandle(new CdcRoot(unused -> isAlive()), context, new StopLock(isAlive()));
        CdcRoot root = cdcHandle.getRoot();
        if (null == root) cdcHandle.setRoot(new CdcRoot(unused -> isAlive()));
        root = cdcHandle.getRoot();
        TapConnectorContext rootContext = root.getContext();
        if (null == rootContext) root.setContext(context);
        Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::releaseCdc);
        KVMap<Object> stateMap = context.getStateMap();
        stateMap.put("tableOverType", OverwriteType.OVERWRITE.getType());
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> {
            if (tapYearValue.getValue() != null && tapYearValue.getValue().getTimeZone() == null) {
                tapYearValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapYearValue.getValue(), "yyyy");
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);
        connectorFunctions
                //.supportCreateTableV2(this::createTableV2)
                .supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset)
                .supportBatchCount(this::batchCount)
                .supportBatchRead(this::batchReadV2)
                .supportGetTableNamesFunction(this::getTableNames)
                .supportStreamRead(this::streamRead)
                .supportTimestampToStreamOffset(this::timestampToStreamOffset)
                .supportReleaseExternalFunction(this::release);
    }

//    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
//        RetryOptions retryOptions = RetryOptions.create();
//        retryOptions.setNeedRetry(true);
//        retryOptions.beforeRetryMethod(() -> {
//            try {
//                synchronized (this) {
//                    //mysqlJdbcContext是否有效
//                    if (sybaseContext == null || !checkValid() || !started.get()) {
//                        //如果无效执行onStop,有效就return
//                        this.onStop(tapConnectionContext);
//                        if (isAlive()) {
//                            this.onStart(tapConnectionContext);
//                        }
//                    } else {
//                        mysqlWriter.selfCheck();
//                    }
//                }
//            } catch (Throwable ignore) {
//            }
//        });
//        return retryOptions;
//    }

    private boolean checkValid() {
        try {
            sybaseContext.queryVersion();
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    protected CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            if (sybaseContext.queryAllTables(Collections.singletonList(tapCreateTableEvent.getTableId())).size() > 0) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String mysqlVersion = sybaseContext.queryVersion();
                SqlMaker sqlMaker = new MysqlMaker();
                if (null == tapCreateTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return createTableOptions;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
                sybaseContext.batchExecute(Arrays.asList(createTableSqls));
                createTableOptions.setTableExists(false);
            }
            return createTableOptions;
        } catch (Throwable t) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), t);
            throw new RuntimeException("Create table failed, message: " + t.getMessage(), t);
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    private Map<String, Object> filterTimeForMysql(ResultSet resultSet, ResultSetMetaData metaData, Set<String> dateTypeSet, String encode, String decode, String outCode) throws SQLException {
        Map<String, Object> data = new HashMap<>();

        for (int index = 0, keyIndex = 1; index < metaData.getColumnCount(); keyIndex++, index++) {
            String columnName = metaData.getColumnName(keyIndex);
            try {
                Object value;
                String columnTypeName = metaData.getColumnTypeName(keyIndex);
                final String upperColumnType = null == columnTypeName ? "" : columnTypeName.toUpperCase(Locale.ROOT);
                if ("TIME".equals(upperColumnType) || "DATE".equals(upperColumnType)) {
                    value = resultSet.getString(keyIndex);
                } else if (upperColumnType.contains("CHAR")
                        || upperColumnType.contains("TEXT")
                        || upperColumnType.contains("SYSNAME")) {
                    String string = resultSet.getString(keyIndex);
                    value = null == string ? null : Utils.convertString(string, encode, decode);
                } else {
                    value = resultSet.getObject(keyIndex);
                    if (null == value && dateTypeSet.contains(columnName)) {
                        value = resultSet.getString(keyIndex);
                    }
                }
                data.put(columnName, value);
            } catch (Exception e) {
                throw new RuntimeException("Read column value failed, column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
            }
        }
        return data;
    }

    @Override
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        ConnectionConfig config = new ConnectionConfig(connectorContext);
        String sql = commonSqlMaker.buildSelectClause(table, filter) + getSchemaAndTable(config.getDatabase() + "." + config.getSchema() + "." + table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        int batchSize = null != filter.getBatchSize() && filter.getBatchSize().compareTo(0) > 0 ? filter.getBatchSize() : BATCH_ADVANCE_READ_LIMIT;
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            //get all column names
            Set<String> dateTypeSet = dateFields(table);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                filterResults.add(filterTimeForMysql(resultSet, metaData, dateTypeSet, encode, decode, outCode));
                if (filterResults.getResults().size() == batchSize) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    @Override
    protected void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        if (null == nodeConfig) {
            nodeConfig = new NodeConfig(connectorContext);
        }
        final boolean needEncode = nodeConfig.isAutoEncode();
        final String encode = needEncode ? Optional.ofNullable(nodeConfig.getEncode()).orElse("cp850") : null;
        final String decode = needEncode ? Optional.ofNullable(nodeConfig.getDecode()).orElse("big5") : null;
        final String outCode = needEncode ? Optional.ofNullable(nodeConfig.getOutDecode()).orElse("utf-8") : null;
        ConnectionConfig config = new ConnectionConfig(connectorContext);
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "select * from " + getSchemaAndTable(config.getDatabase() + "." + config.getSchema() + "." + tapTable.getId()) + " where " + commonSqlMaker.buildKeyAndValue(filter.getMatch(), "and", "=");
            FilterResult filterResult = new FilterResult();
            try {
                jdbcContext.query(sql, resultSet -> {
                    Set<String> dateTypeSet = dateFields(tapTable);
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    if (resultSet.next()) {
                        filterResult.setResult(filterTimeForMysql(resultSet, metaData, dateTypeSet, encode, decode, outCode));
                    }
                });
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    private Set<String> dateFields(TapTable tapTable) {
        Set<String> dateTypeSet = new HashSet<>();
        tapTable.getNameFieldMap().forEach((n, v) -> {
            switch (v.getTapType().getType()) {
                case TapType.TYPE_DATE:
                case TapType.TYPE_DATETIME:
                    dateTypeSet.add(n);
                    break;
                default:
                    break;
            }
        });
        return dateTypeSet;
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + config.getDatabase() + "." + config.getSchema() + "." + tapTable.getId();
            try {
                jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            } catch (Exception e) {
                sql = "select count(1) from " + tapTable.getId();
                jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            }
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    private Map<String, Object> filterTimeForMysql0(ResultSet resultSet, Map<String, String> typeAndName, Set<String> dateTypeSet) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        for (Map.Entry<String, String> entry : typeAndName.entrySet()) {
            String metaType = entry.getValue();
            String metaName = entry.getKey();
            try {
                switch (metaType) {
                    case "TIME":
                    case "DATE":
                        data.put(metaName, resultSet.getString(metaName));
                        break;
                    default:
                        if (metaType.contains("CHAR")
                                || metaType.contains("TEXT")
                                || metaType.contains("SYSNAME")) {
                            String string = resultSet.getString(metaName);
                            data.put(metaName, null == string ? null : (needEncode ? Utils.convertString(string, encode, decode) : string));
                        } else {
                            Object value = resultSet.getObject(metaName);
                            if (null == value && dateTypeSet.contains(metaName)) {
                                value = resultSet.getString(metaName);
                            }
                            data.put(metaName, value);
                        }
                }
            } catch (Exception e) {
                throw new CoreException("Read column value failed, column name: {}, type: {}, data: {}, error: {}", metaName, metaType, data, e.getMessage());
            }
        }
        return data;
    }


    private void batchReadV2(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        if (null == tapTable) {
            throw new CoreException("Start batch read with an empty tap table, batch read is failed");
        }
        final String tableId = tapTable.getId();
        if (null == tableId || "".equals(tableId.trim())) {
            throw new CoreException("Start batch read with tap table which table id is empty, batch read is failed");
        }

        ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
        String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> " " + c + " ").collect(Collectors.joining(","));
        String sql = String.format("SELECT %s FROM " + config.getDatabase() + "." + config.getSchema() + "." + tapTable.getId(), columns);
        final Set<String> dateTypeSet = dateFields(tapTable);
        final List<TapEvent>[] tapEvents = new List[]{list()};
        try {
            sybaseContext.query(sql, resultSet -> {
                ResultSetMetaData metaData = resultSet.getMetaData();
                Map<String, String> typeAndNameFromMetaData = new HashMap<>();
                int columnCount = metaData.getColumnCount();
                for (int index = 1; index < columnCount + 1; index++) {
                    String type = metaData.getColumnTypeName(index);
                    if (null == type) continue;
                    typeAndNameFromMetaData.put(metaData.getColumnName(index), type.toUpperCase(Locale.ROOT));
                }
                while (isAlive() && resultSet.next()) {
                    tapEvents[0].add(insertRecordEvent(filterTimeForMysql0(resultSet, typeAndNameFromMetaData, dateTypeSet), tableId).referenceTime(System.currentTimeMillis()));
                    if (tapEvents[0].size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents[0], offsetState);
                        tapEvents[0] = list();
                    }
                }
            });
        } catch (Exception e) {
            tapConnectorContext.getLog().error("Batch read failed, table name: {}, error msg: {}", tableId, e.getMessage());
        } finally {
            if (EmptyKit.isNotEmpty(tapEvents[0])) {
                eventsOffsetConsumer.accept(tapEvents[0], offsetState);
            }
        }

    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        cdcStart(tapConnectorContext);
        return new CdcPosition();
    }

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        if (null == root.getCdcTables() || root.getCdcTables().isEmpty()) {
            root.setCdcTables(tables);
        }
        try {
            if (null == cdcHandle || null == root.getProcess()) {
                throw new CoreException(" Repeated startup of cdc processes is not allowed, the CDC execution information has expired");
//                try {
//                    cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
//                    cdcHandle.safeStopShell();
//                } catch (CoreException e) {
//                    if (e.getCode() == Code.STREAM_READ_WARN) {
//                        tapConnectorContext.getLog().info(e.getMessage());
//                    }
//                    throw e;
//                }
//                cdcHandle.startCdc(overwriteType);
            }
            Process process = root.getProcess();

            if (!process.isAlive()) {
                String msg = Utils.readFromInputStream(process.getErrorStream(), StandardCharsets.UTF_8);
                throw new CoreException("Cdc tool can not running, fail to get stream data. {}", msg);
            }
            try {
                process.exitValue();
                throw new CoreException("Cdc monitor thread is close, can not monitor cdc events, mag: ", Utils.readFromInputStream(process.getErrorStream(), StandardCharsets.UTF_8));
            } catch (Exception ignore) {
            }

            ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
            root.setContext(tapConnectorContext);
            cdcHandle.startListen(
                    FilenameUtils.concat(cdcHandle.getRoot().getSybasePocPath(), "config/sybase2csv/csv/" + config.getDatabase() + "/" + config.getSchema()),
                    "object_metadata.yaml",
                    tables,
                    offset instanceof CdcPosition ? (CdcPosition) offset : new CdcPosition(),
                    batchSize,
                    consumer
            );
            while (isAlive()) {
                sleep(1000);
            }
        } catch (Exception e) {
            tapConnectorContext.getLog().error("Sybase cdc is stopped now, error: {}", e.getMessage());
        } finally {
            Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::stopCdc);
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        AtomicInteger count = new AtomicInteger(0);
        sybaseContext.queryAllTables(null, 999999, (tables) -> {
            if (null != tables && !tables.isEmpty()) count.getAndAdd(tables.size());
        });
        return count.get();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        sybaseConfig = new SybaseConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(sybaseConfig.getConnectionString());
        try (
                SybaseConnectionTest mysqlConnectionTest = new SybaseConnectionTest(sybaseConfig, consumer)
        ) {
            mysqlConnectionTest.testOneByOne();
        }
        List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        if (null == log) log = connectionContext.getLog();
        List<DataMap> tableList = sybaseContext.queryAllTables(tables);
        multiThreadDiscoverSchema(tableList, tableSize, consumer);
    }

    private synchronized List<String> getOutTableList(CopyOnWriteArraySet<List<String>> tableLists) {
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<String> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    protected void multiThreadDiscoverSchema(List<DataMap> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        Map<String, List<DataMap>> tableName = tables.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(t -> t.getString("tableName")));
        CopyOnWriteArraySet<List<String>> tableLists = new CopyOnWriteArraySet<>(DbKit.splitToPieces(new ArrayList<>(tableName.keySet()), tableSize));
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(5);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        try {
            for (int i = 0; i < 5; i++) {
                executorService.submit(() -> {
                    try {
                        List<String> subTableNemas;
                        while ((subTableNemas = getOutTableList(tableLists)) != null) {
                            List<DataMap> tableInfos = new ArrayList<>();
                            subTableNemas.stream().filter(Objects::nonNull).forEach(name -> {
                                tableInfos.addAll(tableName.get(name));
                            });
                            singleThreadDiscoverSchema(tableInfos, consumer);
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                throw new RuntimeException(throwable.get());
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = TapSimplify.list();
        List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
        Map<String, List<DataMap>> tables = subList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(map -> map.getString("tableName")));

        //List<DataMap> columnList = sybaseContext.queryAllColumns(subTableNames);
        List<DataMap> indexList = sybaseContext.queryAllIndexes(new ArrayList<>(tables.keySet()));
        SybaseColumn sybaseColumn = new SybaseColumn();
        tables.forEach((table, columns) -> {
            TapTable tapTable = table(table);
            //tapTable.setComment(subTable.getString("tableComment"));
            //3、primary key and table index
            Set<String> primaryKeySet = new HashSet<>();
            List<TapIndex> tapIndexList = TapSimplify.list();
            makePrimaryKeyAndIndex(indexList, table, primaryKeySet, tapIndexList);
            List<String> primaryKeys = TapSimplify.list();
            primaryKeySet.stream().filter(Objects::nonNull).forEach(name -> primaryKeys.add(name.trim()));
            //4、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            columns.stream()
                    .filter(col -> null != col
                            && null != col.getString("dataType")
                            && !col.getString("dataType").toUpperCase(Locale.ROOT).contains("TIMESTAMP"))
                    .forEach(col -> {
                        TapField tapField = sybaseColumn.initTapField(col);
                        tapField.setPos(keyPos.incrementAndGet());
                        tapField.setPrimaryKey(primaryKeys.contains(tapField.getName().trim()));
                        tapField.setPrimaryKeyPos(primaryKeys.indexOf(tapField.getName()) + 1);
                        tapTable.add(tapField);
                    });
            tapTable.setIndexList(tapIndexList);
            if (null == tapTable.getNameFieldMap() || tapTable.getNameFieldMap().isEmpty()) {
                log.warn("Table {} can not fund any primary key", tapTable.getId());
            }
            tapTableList.add(tapTable);
        });
        syncSchemaSubmit(tapTableList, consumer);
    }

    protected void makePrimaryKeyAndIndex(List<DataMap> indexList, String table, Set<String> primaryKey, List<TapIndex> tapIndexList) {
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("tableName")))
                .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> {
            if (value.stream().anyMatch(v -> ("clustered, unique".equals(v.getString("index_description"))))) {
                primaryKey.addAll(value.stream().filter(v -> Objects.nonNull(v) && ("clustered, unique".equals(v.getString("index_description")))).filter(v -> null != v.get("index_keys")).map(v -> v.getString("index_keys")).collect(Collectors.toList()));
            }
            tapIndexList.add(makeTapIndex(key, value));
        });
    }

    protected TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = new TapIndex();
        index.setName(key);
        value.forEach(v -> {
            String indexKeys = v.getString("index_keys");
            String[] keyNames = indexKeys.split(",");
            String indexDescription = v.getString("index_description");
            List<TapIndexField> fieldList = TapSimplify.list();
            for (String keyName : keyNames) {
                if (null == keyName || "".equals(keyName.trim())) continue;
                TapIndexField field = new TapIndexField();
                //field.setFieldAsc("1".equals(v.getString("isAsc")));
                field.setName(keyName.trim());
                fieldList.add(field);
            }
            index.setUnique(indexDescription.contains("unique"));
            index.setPrimary(indexDescription.contains("clustered, unique"));
            index.setIndexFields(fieldList);

        });
        return index;
    }

    protected String getSchemaAndTable(String tableId) {
        return tableId;
    }

    private void cdcStart(TapConnectorContext tapConnectorContext) {
        Iterator<Entry<TapTable>> tableIterator = tapConnectorContext.getTableMap().iterator();
        Set<String> tableIds = new HashSet<>();
        while (tableIterator.hasNext()) {
            Entry<TapTable> next = tableIterator.next();
            if (null != next) {
                String key = next.getKey();
                if (null != key && !"".equals(key.trim())) {
                    tableIds.add(key);
                }
            }
        }
        tapConnectorContext.getLog().info("Task table will be monitor in cdc: {}", tableIds);

        if (root == null) root = new CdcRoot(unused -> isAlive());
        List<String> cdcTables = root.getCdcTables();

        if (!equalsTable(cdcTables, tableIds)) {
            root.setCdcTables(new ArrayList<>(tableIds));
            if (null == cdcHandle) {
                try {
                    cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
                } catch (CoreException e) {
                    if (e.getCode() == Code.STREAM_READ_WARN) {
                        tapConnectorContext.getLog().info(e.getMessage());
                    }
                    throw e;
                }
                cdcHandle.initCdc(overwriteType);
            } else {
//                cdcHandle.compileFilterTableYamlConfig(new ConnectionConfig(tapConnectorContext));
//                cdcHandle.refreshCdc(overwriteType);
            }
        }
    }

    private boolean equalsTable(List<String> cdcTables, Set<String> tables) {
        if (null == cdcTables || cdcTables.isEmpty()) return false;
        for (String table : tables) {
            if (!cdcTables.contains(table)) return false;
        }
        return true;
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = sybaseContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }
}
