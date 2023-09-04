package io.tapdata.sybase;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
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
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
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
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.analyse.SybaseDataTypeConvert;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.cdc.service.CdcHandle;
import io.tapdata.sybase.extend.*;
import io.tapdata.sybase.util.*;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
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
    public static final Object filterConfigLock = new Object();

    public static final int CDC_PROCESS_FAIL_EXCEPTION_CODE = 555005;

    private SybaseContext sybaseContext;
    private SybaseReader sybaseReader;
    private MysqlWriter mysqlWriter;
    private String version;
    private CdcHandle cdcHandle;
    private StopLock lock;
    private CdcRoot root;
    private OverwriteType overwriteType;
    private ConnectionConfig connectionConfig;
    private NodeConfig nodeConfig;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Log log;
    private String encode;
    private String decode;
    private String outCode;
    private boolean needEncode = false;
    private String taskId;
    protected SybaseConfig sybaseConfig;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        sybaseConfig = new SybaseConfig().load(tapConnectionContext.getConnectionConfig());
        sybaseContext = new SybaseContext(sybaseConfig);
        commonDbConfig = sybaseConfig;
        jdbcContext = sybaseContext;
        commonSqlMaker = new SybaseSqlMaker('"');
        exceptionCollector = new MysqlExceptionCollector();
        log = tapConnectionContext.getLog();
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new SybaseSqlBatchWriter(sybaseContext);
            this.sybaseReader = new SybaseReader(sybaseContext);
            this.version = sybaseContext.queryVersion();

            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
            root = new CdcRoot(unused -> isAlive());
            lock = new StopLock(true);

            //@todo
            DataMap nodeConfig = tapConnectionContext.getNodeConfig();
            if(null == nodeConfig) {
                nodeConfig = new DataMap();
                tapConnectionContext.setNodeConfig(nodeConfig);
            }
            nodeConfig.put("logCdcQuery", tapConnectionContext.getConnectionConfig().get("logCdcQuery"));

            connectionConfig = new ConnectionConfig(tapConnectionContext);
            this.nodeConfig = new NodeConfig((TapConnectorContext) tapConnectionContext);

            //维护taskId
            taskId = ConnectorUtil.maintenanceTaskId((TapConnectorContext) tapConnectionContext);

            //维护cdc工具状态，决定下次启动使用 --resume 还是 --overtime
            overwriteType = ConnectorUtil.maintenanceTableOverType((TapConnectorContext) tapConnectionContext);

            needEncode = this.nodeConfig.isAutoEncode();
            encode = needEncode ? Optional.ofNullable(this.nodeConfig.getEncode()).orElse("cp850") : null;
            decode = needEncode ? Optional.ofNullable(this.nodeConfig.getDecode()).orElse("big5") : null;
            outCode = needEncode ? Optional.ofNullable(this.nodeConfig.getOutDecode()).orElse("utf-8") : null;
        }
        started.set(true);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        ErrorKit.ignoreAnyError(() -> Optional.ofNullable(root).ifPresent(CdcRoot::notifyAll));
        started.set(false);
        ErrorKit.ignoreAnyError(() -> Optional.ofNullable(this.sybaseReader).ifPresent(MysqlReader::close));
        ErrorKit.ignoreAnyError(() -> Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy));
        Optional.ofNullable(sybaseContext).ifPresent(c -> {
            try {
                this.sybaseContext.close();
                this.sybaseContext = null;
            } catch (Exception e) {
                TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        });
        if (connectionContext instanceof TapConnectorContext) {
            Object isMutilStreamTask = ((TapConnectorContext) connectionContext).getStateMap().get("is_multi_stream_task");
            if (isMutilStreamTask instanceof Boolean && (Boolean) isMutilStreamTask) {
                if (null == cdcHandle) {
                    ConnectorUtil.safeStopShell((TapConnectorContext) connectionContext);
                } else {
                    cdcHandle.stopCdc();
                }
                //if (null != cdcHandle) cdcHandle.releaseTaskResources();
                //Set<String> taskIdInGlobalStateMap = ConnectorUtil.removeTaskIdInGlobalStateMap(taskId, (TapConnectorContext) connectionContext);
                //connectionContext.getLog().info("TaskIdInGlobalStateMap: {}", taskIdInGlobalStateMap);
                //synchronized (SybaseConnector.filterConfigLock) {
                //    if (null == taskIdInGlobalStateMap || taskIdInGlobalStateMap.isEmpty()) {
                //        ConnectorUtil.safeStopShell(connectionContext.getLog(), (TapConnectorContext) connectionContext);
                //        ConnectorUtil.removeCdcMonitorTableMap((TapConnectorContext) connectionContext);
                //    }
                //}
            }
        }
        Optional.ofNullable(lock).ifPresent(StopLock::stop);
        Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::releaseTaskResources);
    }

    private void release(TapConnectorContext context) {
        KVMap<Object> stateMap = context.getStateMap();
        Object isMutilStreamTask = stateMap.get("is_multi_stream_task");
        final boolean shareTask = null != isMutilStreamTask && isMutilStreamTask instanceof Boolean && (Boolean) isMutilStreamTask;
        if (shareTask) {
            if (null == cdcHandle)
                cdcHandle = new CdcHandle(new CdcRoot(unused -> isAlive()), context, new StopLock(isAlive()));
            if (null == cdcHandle.getRoot()) cdcHandle.setRoot(new CdcRoot(unused -> isAlive()));
            CdcRoot root = cdcHandle.getRoot();
            if (null == root.getContext()) root.setContext(context);
            Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::releaseCdc);
            stateMap.put("tableOverType", OverwriteType.OVERWRITE.getType());
            //final String closeCdcPositionSql = "dbcc settrunc('ltm','ignore')";
            //if (null == sybaseConfig) {
            //    sybaseConfig = new SybaseConfig().load(context.getConnectionConfig());
            //}
            //sybaseContext = new SybaseContext(sybaseConfig);
            //try {
            //    sybaseContext.execute(closeCdcPositionSql);
            //} catch (Exception e) {
            //    context.getLog().error("Fail to close cdc log, please execute sql in client: {}", closeCdcPositionSql);
            //}
        }
        try {
            String configPath = String.valueOf(stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_BASE_DIR));
            if (!"/*".equals(configPath) && !"".equals(configPath) && !"null".equals(configPath)) {
                if (shareTask) {
                    File taskFile = new File(String.valueOf(configPath));
                    if (taskFile.exists() && taskFile.isDirectory()) {
                        ConnectorUtil.deleteFile(configPath, context.getLog());
                    }
                } else {
                    Object taskId = stateMap.get("taskId");
                    String path = String.format(ConfigPaths.RE_INIT_TABLE_CONFIG_PATH, configPath, taskId);
                    File taskFile = new File(path);
                    if (taskFile.exists() && taskFile.isDirectory() && taskFile.delete()) {
                        context.getLog().info("Task directory has be cleaned, path: {}", configPath);
                    }
                }
            }
            if (shareTask) {
                String[] killShellCmd = ConnectorUtil.getKillShellCmd(context);
                String hostPortFromConfig = ConnectorUtil.getCurrentInstanceHostPortFromConfig(context);
                List<Integer> port = ConnectorUtil.port(
                        killShellCmd,
                        ConnectorUtil.ignoreShells,
                        context.getLog(),
                        hostPortFromConfig
                );
                if (!port.isEmpty()) {
                    ConnectorUtil.safeStopShell(context, port);
                }
            }
        } catch (Exception ignore) {}
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);
        connectorFunctions
                .supportErrorHandleFunction(this::errorHandle)
                //.supportCreateTableV2(this::createTableV2)
                .supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset)
                .supportBatchCount(this::batchCount)
                .supportBatchRead(this::batchReadV2)
                .supportGetTableNamesFunction(this::getTableNames)
                .supportStreamRead(this::streamRead)
                .supportTimestampToStreamOffset(this::timestampToStreamOffset)
                .supportReleaseExternalFunction(this::release)
                .supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> sybaseContext.getConnection(), this::isAlive, c))
                .supportRunRawCommandFunction(this::runRawCommand)
                .supportStreamReadMultiConnectionFunction(this::multiStreamStart);
                //.supportCountRawCommandFunction(this::countRawCommand);
    }

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create();
        retryOptions.setNeedRetry(throwable instanceof CoreException && ((CoreException)throwable).getCode() == CDC_PROCESS_FAIL_EXCEPTION_CODE);
        retryOptions.beforeRetryMethod(() -> {
            try {
                synchronized (this) {
                    if (throwable instanceof CoreException && ((CoreException)throwable).getCode() == CDC_PROCESS_FAIL_EXCEPTION_CODE) {
                        this.onStop(tapConnectionContext);
                        this.onStart(tapConnectionContext);
                    }
                }
            } catch (Throwable ignore) {
            }
        });
        return retryOptions;
    }

    protected void runRawCommand(TapConnectorContext connectorContext, String command, TapTable tapTable, int eventBatchSize, Consumer<List<TapEvent>> eventsOffsetConsumer) throws Throwable {
        final List<TapEvent>[] tapEvents = new List[]{list()};
        try {
            sybaseContext.query(command, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (isAlive() && resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                    tapEvents[0].add(insertRecordEvent(dataMap, tapTable.getId()));
                    if (tapEvents[0].size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents[0]);
                        tapEvents[0] = list();
                    }
                }
            });
        } finally {
            if (!tapEvents[0].isEmpty()) eventsOffsetConsumer.accept(tapEvents[0]);
        }
    }

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
            if (!sybaseContext.queryAllTables(Collections.singletonList(tapCreateTableEvent.getTableId())).isEmpty()) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String sybaseVersion = sybaseContext.queryVersion();
                SqlMaker sqlMaker = new SybaseSqlMarker();
                if (null == tapCreateTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return createTableOptions;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, sybaseVersion);
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

    /**
     * @deprecated
     */
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
        if (null == connectionConfig) {
            connectionConfig = new ConnectionConfig(connectorContext);
        }
        String sql = commonSqlMaker.buildSelectClause(table, filter) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        int batchSize = null != filter.getBatchSize() && filter.getBatchSize().compareTo(0) > 0 ? filter.getBatchSize() : BATCH_ADVANCE_READ_LIMIT;
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            //get all column names
            Set<String> dateTypeSet = ConnectorUtil.dateFields(table);
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
        if (null == connectionConfig) {
            connectionConfig = new ConnectionConfig(connectorContext);
        }
        final boolean needEncode = nodeConfig.isAutoEncode();
        final String encode = needEncode ? Optional.ofNullable(nodeConfig.getEncode()).orElse("cp850") : null;
        final String decode = needEncode ? Optional.ofNullable(nodeConfig.getDecode()).orElse("big5") : null;
        final String outCode = needEncode ? Optional.ofNullable(nodeConfig.getOutDecode()).orElse("utf-8") : null;
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = String.format("select * from %s where %s",
                    getSchemaAndTable(tapTable.getId()),
                    commonSqlMaker.buildKeyAndValue(filter.getMatch(), "and", "="));
            FilterResult filterResult = new FilterResult();
            try {
                jdbcContext.query(sql, resultSet -> {
                    Set<String> dateTypeSet = ConnectorUtil.dateFields(tapTable);
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

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            if (null == connectionConfig) {
                connectionConfig = new ConnectionConfig(tapConnectorContext);
            }
            AtomicLong count = new AtomicLong(0);
            String sql = String.format("select count(*) from %s", getSchemaAndTable(tapTable.getId()));
            try {
                jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            } catch (Exception e) {
                sql = "select count(*) from " + tapTable.getId();
                jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            }
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    private void batchReadV2(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        KVMap<Object> stateMap = tapConnectorContext.getStateMap();
        Object hasSleep = stateMap.get("has_sleep_after_process_start");
        if (!(hasSleep instanceof Boolean && (Boolean) hasSleep)) {
            log.info("Batch read execution requires waiting for the cdc process to fully start, with an estimated waiting time of [30] seconds. Please wait later");
            try {
                Thread.sleep(30000);
            } catch (Exception ignore) {
            } finally {
                stateMap.put("has_sleep_after_process_start", true);
            }
        }

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
        final Set<String> dateTypeSet = ConnectorUtil.dateFields(tapTable);
        final List<TapEvent>[] tapEvents = new List[]{new ArrayList<TapEvent>()};
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
                while (root.checkStep() && resultSet.next()) {
                    tapEvents[0].add(insertRecordEvent(
                            filterTimeForMysql0(resultSet, typeAndNameFromMetaData, dateTypeSet, needEncode, encode, decode),
                            tableId).referenceTime(System.currentTimeMillis()));
                    if (tapEvents[0].size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents[0], offsetState);
                        tapEvents[0] = new ArrayList<>();
                    }
                }
            });
        } catch (Exception e) {
            tapConnectorContext.getLog().error("Batch read failed, table name: {}, error msg: {}", tableId, e.getMessage());
        } finally {
            if (!tapEvents[0].isEmpty()) {
                eventsOffsetConsumer.accept(tapEvents[0], offsetState);
            }
        }

    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        //synchronized (filterConfigLock) {
        //    List<Integer> port = ConnectorUtil.port(
        //            ConnectorUtil.getKillShellCmd(tapConnectorContext),
        //            ConnectorUtil.ignoreShells,
        //            tapConnectorContext.getLog(),
        //            ConnectorUtil.getCurrentInstanceHostPortFromConfig(tapConnectorContext)
        //    );
        //    if (port.isEmpty()) {
        //        if (null != root) {
        //            root.setCdcTables(getCurrentTable(tapConnectorContext));
        //        }
        //        cdcStart(tapConnectorContext, null);
        //        tapConnectorContext.getStateMap().put("timestampToStreamOffsetTarget", true);
        //    }
        //}
        return new HashMap<String, Integer>();
    }

    private Map<String, Map<String, List<String>>> getCurrentTable(TapConnectorContext context) {
        KVReadOnlyMap<TapTable> tableMap = context.getTableMap();
        List<String> currentTables = new ArrayList<>();
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        ConnectionConfig config = new ConnectionConfig(context);
        final String databaseSuf = config.getDatabase() + ".";
        final String schemaSuf = config.getSchema() + ".";
        while (iterator.hasNext()) {
            Entry<TapTable> next = iterator.next();
            String key = next.getKey();
            if (key.startsWith(databaseSuf)) {
                key = key.replace(databaseSuf, "");
                if (key.startsWith(schemaSuf)) {
                    key = key.replace(schemaSuf, "");
                }
            }
            currentTables.add(key);
        }

        Map<String, Map<String, List<String>>> tables = new HashMap<>();
        Map<String, List<String>> schemaTables = new HashMap<>();
        schemaTables.put(config.getSchema(), currentTables);
        tables.put(config.getDatabase(), schemaTables);
        return tables;
    }

    /**
     * @deprecated
     * */
    private void streamRead(TapConnectorContext tapConnectorContext, List<String> currentTables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        //@todo
        DataMap node = tapConnectorContext.getNodeConfig();
        if(null == node) {
            node = new DataMap();
            tapConnectorContext.setNodeConfig(node);
        }
        node.put("logCdcQuery", tapConnectorContext.getConnectionConfig().get("logCdcQuery"));


        root.csvFileModifyIndexCache(offset instanceof Map ? (Map<String, Map<String, Integer>>) offset : new HashMap<>());
        tapConnectorContext.getStateMap().put("is_multi_stream_task", true);
        //throw new CoreException("Not support stream read by node, please open Shared-Mining ");
        Object position = tapConnectorContext.getStateMap().get("cdc_position");
        CdcPosition cdcPosition = (position instanceof CdcPosition ? (CdcPosition) position : new CdcPosition()).notMultiTask();

        tapConnectorContext.getLog().info("Running with normal task on cdc, offset: {}", cdcPosition);
        if (null == currentTables || currentTables.isEmpty()) return;

        if (null == root.getTaskCdcId()) {
            root.setTaskCdcId(taskId = null == taskId ? ConnectorUtil.maintenanceTaskId(tapConnectorContext) : taskId);
        }
        //if (null == root.getCdcTables() || root.getCdcTables().isEmpty()) {
        //    root.setCdcTables(tables);
        //}
        //KVReadOnlyMap<TapTable> tableMap = tapConnectorContext.getTableMap();
        //Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        Map<String, Map<String, List<String>>> tables = new HashMap<>();
        Map<String, List<String>> database = new HashMap<>();
        database.put(connectionConfig.getSchema(), currentTables);
        tables.put(connectionConfig.getDatabase(), database);

        List<ConnectionConfigWithTables> connectionConfigWithTables = new ArrayList<>();
        ConnectionConfigWithTables connectionConfigWithTablesItem = new ConnectionConfigWithTables();
        connectionConfigWithTablesItem.connectionConfig(tapConnectorContext.getConnectionConfig());
        connectionConfigWithTablesItem.setTables(currentTables);
        connectionConfigWithTables.add(connectionConfigWithTablesItem);
        try {
            //Object target = tapConnectorContext.getStateMap().get("timestampToStreamOffsetTarget");
            //if (!(null != target && target instanceof Boolean && (Boolean)target)) {
            cdcStart(tapConnectorContext, connectionConfigWithTables);
            //} else {
            //   tapConnectorContext.getStateMap().put("timestampToStreamOffsetTarget", true);
            //}

            root.setContext(tapConnectorContext);

            if (null == cdcHandle) {
                try {
                    root.setSybasePocPath(getSybasePocPath(tapConnectorContext));
                    cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
                } catch (CoreException e) {
                    if (e.getCode() == Code.STREAM_READ_WARN) {
                        tapConnectorContext.getLog().info(e.getMessage());
                    }
                    throw e;
                }
            }
            String sybasePocPath = cdcHandle.getRoot().getSybasePocPath();
            if (null == sybasePocPath || "".equals(sybasePocPath.trim())) {
                Object path = tapConnectorContext.getStateMap().get(ConfigPaths.SYBASE_USE_TASK_CONFIG_BASE_DIR);
                if (null == path) {
                    path = new File("sybase-poc-temp/"+ ConnectorUtil.getCurrentInstanceHostPortFromConfig(root.getContext()) + "/").getAbsolutePath();
                    tapConnectorContext.getStateMap().put(ConfigPaths.SYBASE_USE_TASK_CONFIG_BASE_DIR, path);
                    //throw new CoreException("Can not get sybase poc cdc base path from global state map");
                }
                sybasePocPath = String.valueOf(path);
            }
            if(!(hasMonitor && cdcHandle.reflshCdcTable(tables))) {
                hasMonitor = true;
                if (ReadFilter.LOG_CDC_QUERY_READ_SOURCE == nodeConfig.getLogCdcQuery()) {
                    root.setConnectionConfigWithTables(connectionConfigWithTables);
                }
                cdcHandle.startListen(
                        sybasePocPath + ConfigPaths.SYBASE_USE_CSV_DIR,
                        ConfigPaths.YAML_METADATA_NAME,
                        tables,
                        cdcPosition,
                        batchSize,
                        nodeConfig.getCloseDelayMill(),
                        consumer
                );
            }

            while (isAlive()) {
                try {
                    sleep(5000);
                } catch (Exception ignore){}
                List<Integer> port = ConnectorUtil.port(
                        ConnectorUtil.getKillShellCmd(tapConnectorContext),
                        ConnectorUtil.ignoreShells,
                        tapConnectorContext.getLog(),
                        ConnectorUtil.getCurrentInstanceHostPortFromConfig(tapConnectorContext)
                );
                if (port.size() < 2 && isAlive()) {
                    //tapConnectorContext.getLog().info("Cdc task is normal, but not any cdc process is active, this task is normal task can not retry to start cdc process.");
                    //CDC_PROCESS_FAIL_EXCEPTION_CODE
                    throw new CoreException(SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE, "Cdc task is normal, but not any cdc process is active, will retry to start cdc process");
                }
            }
        } catch (Throwable e) {
//            if ( e instanceof CoreException && ((CoreException)e).getCode() == SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE) {
//                throw
//            }
            throw e;
            //tapConnectorContext.getLog().error("Sybase cdc is stopped now, error: {}", e.getMessage());
        }
    }

    /**
     * support multi task
     * */
    boolean hasMonitor = false;
    private void multiStreamStart(TapConnectorContext tapConnectorContext, List<ConnectionConfigWithTables> connectionConfigWithTables, Object offset, int batchSize, StreamReadConsumer consumer) {
        //@todo
        DataMap node = tapConnectorContext.getNodeConfig();
        if(null == node) {
            node = new DataMap();
            tapConnectorContext.setNodeConfig(node);
        }
        node.put("logCdcQuery", tapConnectorContext.getConnectionConfig().get("logCdcQuery"));

        tapConnectorContext.getStateMap().put("is_multi_stream_task", true);
        Object position = tapConnectorContext.getStateMap().get("cdc_position");
        root.csvFileModifyIndexCache(offset instanceof Map ? (Map<String, Map<String, Integer>>) offset : new HashMap<>());
        root.setContext(tapConnectorContext);
        if(batchSize < 200 || batchSize > 500) batchSize = 200;
        if (null == connectionConfigWithTables || connectionConfigWithTables.isEmpty()) return;

        if (null == root.getTaskCdcId()) {
            root.setTaskCdcId(taskId = null == taskId ? ConnectorUtil.maintenanceTaskId(tapConnectorContext) : taskId);
        }
        Map<String, Map<String, List<String>>> tables = ConnectorUtil.groupTableFromConnectionConfigWithTables(connectionConfigWithTables);
        if (null == root.getCdcTables() || root.getCdcTables().isEmpty()) {
            root.setCdcTables(tables);
        }
        tapConnectorContext.getLog().info("Multi stream start with tables: {}", toJson(tables));
        try {
            //Object target = tapConnectorContext.getStateMap().get("timestampToStreamOffsetTarget");
            //if (!(null != target && target instanceof Boolean && (Boolean)target)) {
            cdcStart(tapConnectorContext, connectionConfigWithTables);
            //} else {
            //   tapConnectorContext.getStateMap().put("timestampToStreamOffsetTarget", true);
            //}

            root.setContext(tapConnectorContext);

            if (null == cdcHandle) {
                try {
                    root.setSybasePocPath(getSybasePocPath(tapConnectorContext));
                    cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
                } catch (CoreException e) {
                    if (e.getCode() == Code.STREAM_READ_WARN) {
                        tapConnectorContext.getLog().info(e.getMessage());
                    }
                    throw e;
                }
            }
            String sybasePocPath = cdcHandle.getRoot().getSybasePocPath();
            if (null == sybasePocPath || "".equals(sybasePocPath.trim())) {
                Object path = tapConnectorContext.getStateMap().get(ConfigPaths.SYBASE_USE_TASK_CONFIG_BASE_DIR);
                if (null == path) {
                    throw new CoreException("Can not get sybase poc cdc base path from global state map");
                }
                sybasePocPath = String.valueOf(path);
            }
            //log.info("cdc monitor path: {}", sybasePocPath + ConfigPaths.SYBASE_USE_CSV_DIR);
            if(!(hasMonitor && cdcHandle.reflshCdcTable(tables))) {
                hasMonitor = true;
                if (ReadFilter.LOG_CDC_QUERY_READ_SOURCE == nodeConfig.getLogCdcQuery()) {
                    root.setConnectionConfigWithTables(connectionConfigWithTables);
                }
                cdcHandle.startListen(
                        sybasePocPath + ConfigPaths.SYBASE_USE_CSV_DIR,
                        ConfigPaths.YAML_METADATA_NAME,
                        tables,
                        position instanceof CdcPosition ? (CdcPosition) position : new CdcPosition(),
                        batchSize,
                        nodeConfig.getCloseDelayMill(),
                        consumer
                );
            }

            while (isAlive()) {
                try {
                    sleep(3000);
                } catch (Exception ignore){}
                List<Integer> port = ConnectorUtil.port(
                        ConnectorUtil.getKillShellCmd(tapConnectorContext),
                        ConnectorUtil.ignoreShells,
                        tapConnectorContext.getLog(),
                        ConnectorUtil.getCurrentInstanceHostPortFromConfig(tapConnectorContext)
                );
                if (port.size() < 2 && isAlive()) {
                    //CDC_PROCESS_FAIL_EXCEPTION_CODE
                    throw new CoreException(SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE, "Cdc task is normal, but not any cdc process is active, will retry to start cdc process");
                }
            }
        } catch (Throwable e) {
            if (!isAlive()) {
                throw e;
            }
            //tapConnectorContext.getLog().error("Sybase cdc is stopped now, error: {}", e.getMessage());
        }
    }

    public String getSybasePocPath(TapConnectorContext context) {
        String targetPath = "sybase-poc-temp/"+ ConnectorUtil.getCurrentInstanceHostPortFromConfig(root.getContext()) + "/sybase-poc";
        return new File(targetPath).getAbsolutePath();
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
        try (SybaseConnectionTest mysqlConnectionTest = new SybaseConnectionTest(sybaseConfig, consumer)) {
            mysqlConnectionTest.testOneByOne();
        }
        List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
        ddlCapabilities.forEach(connectionOptions::capability);


        if (sybaseConfig.getPort() <= 0) return connectionOptions;
        if (StringUtils.isAnyBlank(sybaseConfig.getHost(), sybaseConfig.getDatabase(), sybaseConfig.getSchema())) return connectionOptions;

        connectionOptions.setInstanceUniqueId(StringKit.md5(String.join("|"
                , sybaseConfig.getHost()
                , String.valueOf(sybaseConfig.getPort())
                , sybaseConfig.getDatabase()
        )));
        List<String> options = new ArrayList<>();
        options.add(sybaseConfig.getDatabase());
        options.add(sybaseConfig.getSchema());
        connectionOptions.setNamespaces(options);
        return connectionOptions;
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        if (null == log) log = connectionContext.getLog();
        List<DataMap> tableList = sybaseContext.queryAllTables(tables);
        multiThreadDiscoverSchema(tableList, tableSize, consumer);
    }

    @Override
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
                        List<String> subTableNames;
                        while ((subTableNames = ConnectorUtil.getOutTableList(tableLists)) != null) {
                            List<DataMap> tableInfos = new ArrayList<>();
                            subTableNames.stream().filter(Objects::nonNull).forEach(name -> {
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
        //List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
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
            ConnectorUtil.makePrimaryKeyAndIndex(indexList, table, primaryKeySet, tapIndexList);
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

    @Override
    protected String getSchemaAndTable(String tableId) {
        return String.format("%s.%s.%s", connectionConfig.getDatabase(), connectionConfig.getSchema(), tableId);
    }

    private void cdcStart(TapConnectorContext tapConnectorContext, List<ConnectionConfigWithTables> connectionConfigWithTables) {
        if (!isAlive()) return;
        KVMap<Object> stateMap = tapConnectorContext.getStateMap();
        String hostPortFromConfig = ConnectorUtil.getCurrentInstanceHostPortFromConfig(tapConnectorContext);
        if (root == null) {
            root = new CdcRoot(unused -> isAlive());
            root.setCdcTables(getCurrentTable(tapConnectorContext));
        }
        Map<String, Map<String, List<String>>> cdcTables = root.getCdcTables();

        ConnectionConfig config = new ConnectionConfig(tapConnectorContext);

        Map<String, Map<String, List<String>>> groupTableFromConnectionConfigWithTables = ConnectorUtil.groupTableFromConnectionConfigWithTables(connectionConfigWithTables);
        //get all tables which contains field typed timestamp
        final Map<String, Map<String, List<String>>> containsTimestampFieldTables = new HashMap<>();
        if(null == connectionConfigWithTables) {
            if (null == cdcTables) {
                throw new CoreException("Cdc table is empty, can not start cdc process");
            }
            Map<String, List<String>> stringListMap = cdcTables.get(config.getDatabase());
            if (null == stringListMap) {
                throw new CoreException("Cdc table is empty, can not start cdc process, database: {}, not find schema: {}", config.getDatabase(), config.getSchema());
            }
            List<String> tables = stringListMap.get(config.getSchema());
            if (null == tables) {
                throw new CoreException("Cdc table is empty, can not start cdc process, database: {}, not find any tables in schema: {}", config.getDatabase(), config.getSchema());
            }
            ConnectorUtil.containsTimestampFieldTables(containsTimestampFieldTables, tapConnectorContext, tables, sybaseContext);
            root.setCdcTables(cdcTables);
        } else {
            ConnectorUtil.containsTimestampFieldTablesV2(containsTimestampFieldTables, tapConnectorContext, connectionConfigWithTables);
            root.setCdcTables(groupTableFromConnectionConfigWithTables);
        }
        root.setContainsTimestampFieldTables(containsTimestampFieldTables);

        if (null == cdcHandle) {
            try {
                cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
            } catch (CoreException e) {
                if (e.getCode() == Code.STREAM_READ_WARN) {
                    tapConnectorContext.getLog().info(e.getMessage());
                }
                throw e;
            }
        }
        String[] killShellCmd = ConnectorUtil.getKillShellCmd(tapConnectorContext);
        synchronized (filterConfigLock) {
            File filterConfigFile = new File(String.format(CdcRoot.POC_TEMP_TRACE_LOG_PATH, hostPortFromConfig, ConnectorUtil.maintenanceGlobalCdcProcessId(tapConnectorContext)));
            String path = String.valueOf(stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_BASE_DIR));
            if (filterConfigFile.exists() && filterConfigFile.isFile() && !"null".equals(path)) {
                List<Integer> port = ConnectorUtil.port(
                        killShellCmd,
                        ConnectorUtil.ignoreShells,
                        root.getContext().getLog(),
                        hostPortFromConfig
                );
                this.root.setSybasePocPath(path);
                stateMap.put(ConfigPaths.SYBASE_USE_TASK_CONFIG_KEY, (path.endsWith("/") ? path : path + "/") + ConfigPaths.SYBASE_USE_TASK_CONFIG_DIR + String.valueOf(stateMap.get("taskId")));

                int portSize = port.size();
                Map<String, Map<String, List<String>>> monitorTables = (Map<String, Map<String, List<String>>>) stateMap.get("monitorTables");
                log.debug("Table from state map: {}", monitorTables);
                root.setCdcTables(monitorTables);
                //filter the tables which is new table

                Map<String, Map<String, List<String>>> currentTable = Optional.ofNullable(ConnectorUtil.tableFromFilterYaml(path, tapConnectorContext)).orElse(monitorTables);
                Map<String, Map<String, List<String>>> appendTables = ConnectorUtil.filterAppendTable(currentTable, groupTableFromConnectionConfigWithTables);
                //当前任务出先新表需要加入到CDC的监听列表时
                if (null != appendTables && !appendTables.isEmpty()) {
                    if (!port.isEmpty()) {
                        ConnectorUtil.safeStopShell( tapConnectorContext);
                    }
                    log.info("Cdc process is alive and add new tables, will reinit cdc process now, new tables are: {}", appendTables);
                    cdcHandle.addTableAndRestartProcess(config, groupTableFromConnectionConfigWithTables, appendTables, tapConnectorContext.getLog());
                    //ConnectorUtil.maintenanceCdcMonitorTableMap(sybaseFilters, tapConnectorContext);
                } else {
                    if (portSize < 2) {
                        tapConnectorContext.getLog().info("Cdc process not alive, will start cdc process now");
                        List<Map<String, Object>> mapList = cdcHandle.compileFilterTableYamlConfig(monitorTables);
                        CdcStartVariables variables = root.getVariables();
                        if (null == variables) {
                            variables = new CdcStartVariables();
                            root.setVariables(variables);
                        }
                        variables.setFilterConfig(ConnectorUtil.fromYaml(mapList));
                        if (portSize > 0) {
                            ConnectorUtil.safeStopShell( tapConnectorContext);
                        }

                        log.info("Cdc process has init, but status not normal, will be resume start, {}", hostPortFromConfig);
                        //start cdc process in stream at first
                        cdcHandle.initCdc(OverwriteType.RESUME);
                    }
                    log.info("Not fund any table change in monitor table config, reInit cdc process will be canceled");
                }
            } else {
                //start cdc process in stream at first
                synchronized (filterConfigLock) {
                    List<Integer> port = ConnectorUtil.port(
                            killShellCmd,
                            ConnectorUtil.ignoreShells,
                            root.getContext().getLog(),
                            hostPortFromConfig
                    );
                    if (port.size() > 2 && !"null".equals(path)) {
                        this.root.setSybasePocPath(path);
                    } else {
                        if (!port.isEmpty()) {
                            ConnectorUtil.safeStopShell(tapConnectorContext);
                        }
                        log.info("Cdc process not init, will init now, {}", hostPortFromConfig);
                        cdcHandle.initCdc(overwriteType);
                    }
                }
            }
            root.setCdcTables(groupTableFromConnectionConfigWithTables);
            stateMap.put("monitorTables", groupTableFromConnectionConfigWithTables);
        }
    }


    public static Map<String, Object> filterTimeForMysql0(
            ResultSet resultSet,
            Map<String, String> typeAndName,
            Set<String> dateTypeSet,
            boolean needEncode,
            String encode,
            String decode
    ) throws SQLException {
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
                    case "DATETIME":
                        data.put(metaName, SybaseDataTypeConvert.objToTimestamp(resultSet.getString(metaName),"DATETIME"));
                        break;
                    default:
                        if (needEncode && (metaType.contains("CHAR")
                                || metaType.contains("TEXT")
                                || metaType.contains("SYSNAME"))) {
                            String string = resultSet.getString(metaName);
                            data.put(metaName, Utils.convertString(string, encode, decode));
                        } else if(metaType.contains("DATETIME")) {
                            data.put(metaName, SybaseDataTypeConvert.objToTimestamp(resultSet.getString(metaName),""));
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
}
