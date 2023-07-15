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
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
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
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.cdc.service.CdcHandle;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.SybaseColumn;
import io.tapdata.sybase.extend.SybaseConfig;
import io.tapdata.sybase.extend.SybaseConnectionTest;
import io.tapdata.sybase.extend.SybaseContext;
import io.tapdata.sybase.extend.SybaseReader;
import io.tapdata.sybase.extend.SybaseSqlBatchWriter;
import io.tapdata.sybase.util.Code;
import org.apache.commons.io.FilenameUtils;

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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
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

    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        sybaseConfig = new SybaseConfig().load(tapConnectionContext.getConnectionConfig());
        sybaseContext = new SybaseContext(sybaseConfig);
        commonDbConfig = sybaseConfig;
        jdbcContext = sybaseContext;
        commonSqlMaker = new CommonSqlMaker('`');
        exceptionCollector = new MysqlExceptionCollector();
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new SybaseSqlBatchWriter(sybaseContext);
            this.mysqlReader = new SybaseReader(sybaseContext);
            this.version = sybaseContext.queryVersion();
            //this.timezone = sybaseContext.queryTimeZone();
            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
            root = new CdcRoot();
            lock = new StopLock(true);
        }
//		fieldDDLHandlers = new BiClassHandlers<>();
//		fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
//		fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
//		fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
//		fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        started.set(true);
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

        connectorFunctions.supportCreateTableV2(this::createTableV2)
                .supportDropTable(this::dropTable)
                .supportClearTable(this::clearTable)
                .supportBatchCount(this::batchCount)
                .supportBatchRead(this::batchReadV2)
                .supportGetTableNamesFunction(this::getTableNames)

                .supportStreamRead(this::streamRead)
                .supportTimestampToStreamOffset(this::timestampToStreamOffset)
                .supportReleaseExternalFunction(this::release);
        //connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        //connectorFunctions.supportWriteRecord(this::writeRecord);

        //connectorFunctions.supportCreateIndex(this::createIndex);
        //connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        //connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        //connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        //connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        //connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> mysqlJdbcContext.getConnection(), this::isAlive, c));
        //connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        //connectorFunctions.supportQueryFieldMinMaxValueFunction(this::minMaxValue);
        //connectorFunctions.supportGetReadPartitionsFunction(this::getReadPartitions);
        //connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        //connectorFunctions.supportTransactionBeginFunction(this::begin);
        //connectorFunctions.supportTransactionCommitFunction(this::commit);
        //connectorFunctions.supportTransactionRollbackFunction(this::rollback);
    }

//    private void rollback(TapConnectorContext tapConnectorContext) {
//    }
//
//    private void commit(TapConnectorContext tapConnectorContext) {
//    }
//
//    private void begin(TapConnectorContext tapConnectorContext) {
//    }

//    private void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) {
//        DatabaseReadPartitionSplitter.calculateDatabaseReadPartitions(connectorContext, table, options)
//                //.queryFieldMinMaxValue(this::minMaxValue)
//                .typeSplitterMap(options.getTypeSplitterMap().registerSplitter(TypeSplitterMap.TYPE_STRING, StringCaseInsensitiveSplitter.INSTANCE))
//                .startSplitting();
//    }

//    private void partitionRead(TapConnectorContext connectorContext, TapTable table, ReadPartition readPartition, int eventBatchSize, Consumer<List<TapEvent>> consumer) {
//
//    }

//	private FieldMinMaxValue minMaxValue(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapPartitionFilter, String fieldName) {
//		SqlMaker sqlMaker = new MysqlMaker();
//		FieldMinMaxValue fieldMinMaxValue = FieldMinMaxValue.create().fieldName(fieldName);
//		String selectSql, aaa;
//		try {
//			selectSql = sqlMaker.selectSql(tapConnectorContext, tapTable, TapPartitionFilter.create().fromAdvanceFilter(tapPartitionFilter));
//		} catch (Throwable e) {
//			throw new RuntimeException("Build sql with partition filter failed", e);
//		}
//		// min value
//		String minSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MIN(`%s`) AS MIN_VALUE FROM", fieldName));
//		AtomicReference<Object> minObj = new AtomicReference<>();
//		try {
//			sybaseContext.query(minSql, rs -> {
//				if (rs.next()) {
//					minObj.set(rs.getObject("MIN_VALUE"));
//				}
//			});
//		} catch (Throwable e) {
//			throw new RuntimeException("Query min value failed, sql: " + minSql, e);
//		}
//		Optional.ofNullable(minObj.get()).ifPresent(min -> fieldMinMaxValue.min(min).detectType(min));
//		// max value
//		String maxSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MAX(`%s`) AS MAX_VALUE FROM", fieldName));
//		AtomicReference<Object> maxObj = new AtomicReference<>();
//		try {
//			sybaseContext.query(maxSql, rs -> {
//				if (rs.next()) {
//					maxObj.set(rs.getObject("MAX_VALUE"));
//				}
//			});
//		} catch (Throwable e) {
//			throw new RuntimeException("Query max value failed, sql: " + maxSql, e);
//		}
//		Optional.ofNullable(maxObj.get()).ifPresent(max -> fieldMinMaxValue.max(max).detectType(max));
//		return fieldMinMaxValue;
//	}

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create();
        retryOptions.setNeedRetry(true);
        retryOptions.beforeRetryMethod(() -> {
            try {
                synchronized (this) {
                    //mysqlJdbcContext是否有效
                    if (sybaseContext == null || !checkValid() || !started.get()) {
                        //如果无效执行onStop,有效就return
                        this.onStop(tapConnectionContext);
                        if (isAlive()) {
                            this.onStart(tapConnectionContext);
                        }
                    } else {
                        mysqlWriter.selfCheck();
                    }
                }
            } catch (Throwable ignore) {
            }
        });
        return retryOptions;
    }

    private boolean checkValid() {
        try {
            sybaseContext.queryVersion();
            return true;
        } catch (Throwable ignored) {
            return false;
        }
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
        Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::stopCdc);
        Optional.ofNullable(lock).ifPresent(StopLock::stop);
    }

    private void release(TapConnectorContext context) {
        Optional.ofNullable(cdcHandle).ifPresent(CdcHandle::releaseCdc);
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

    private Map<String, Object> filterTimeForMysql(ResultSet resultSet, ResultSetMetaData metaData, Set<String> dateTypeSet) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            try {
                Object value;
                if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                    value = resultSet.getString(i + 1);
                } else if ("DATE".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                    value = resultSet.getString(i + 1);
                } else {
                    value = resultSet.getObject(i + 1);
                    if (null == value && dateTypeSet.contains(columnName)) {
                        value = resultSet.getString(i + 1);
                    }
                }
                data.put(columnName, value);
            } catch (Exception e) {
                throw new RuntimeException("Read column value failed, column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
            }
        }
        return data;
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + config.getDatabase() + "." + config.getUsername() + "." + tapTable.getId();
            jdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    private void batchReadV2(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
        String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> " " + c + " ").collect(Collectors.joining(","));
        String sql = String.format("SELECT %s FROM " + config.getDatabase() + "." + config.getUsername() + "." + tapTable.getId(), columns);

        sybaseContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            Set<String> dateTypeSet = dateFields(tapTable);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(filterTimeForMysql(resultSet, metaData, dateTypeSet), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        });

    }

    @Override
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        int batchSize = null != filter.getBatchSize() && filter.getBatchSize().compareTo(0) > 0 ? filter.getBatchSize() : BATCH_ADVANCE_READ_LIMIT;
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            //get all column names
            Set<String> dateTypeSet = dateFields(table);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                filterResults.add(filterTimeForMysql(resultSet, metaData, dateTypeSet));
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
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "select * from " + getSchemaAndTable(tapTable.getId()) + " where " + commonSqlMaker.buildKeyAndValue(filter.getMatch(), "and", "=");
            FilterResult filterResult = new FilterResult();
            try {
                jdbcContext.query(sql, resultSet -> {
                    Set<String> dateTypeSet = dateFields(tapTable);
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    if (resultSet.next()) {
                        filterResult.setResult(filterTimeForMysql(resultSet, metaData, dateTypeSet));
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

    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        root.setCdcTables(tables);
        root.setContext(tapConnectorContext);
        try {
            try {
                cdcHandle = new CdcHandle(root, tapConnectorContext, lock);
            } catch (CoreException e) {
                if (e.getCode() == Code.STREAM_READ_WARN) {
                    tapConnectorContext.getLog().info(e.getMessage());
                }
                throw e;
            }
            cdcHandle.startCdc();
            ConnectionConfig config = new ConnectionConfig(tapConnectorContext);
            root.setContext(tapConnectorContext);
            cdcHandle.startListen(
                    FilenameUtils.concat(cdcHandle.getRoot().getSybasePocPath(), "config/sybase2csv/csv/" + config.getDatabase() + "/" + config.getUsername()),
                    "object_metadata.yaml",
                    tables,
                    offset instanceof CdcPosition ? (CdcPosition) offset : new CdcPosition(),
                    batchSize,
                    consumer
            );
            while (isAlive()){
                sleep(1000);
            }
        } catch (Exception e){
            tapConnectorContext.getLog().error("Sybase cdc is stopped now, error: {}", e.getMessage());
        }
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

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        return new CdcPosition();
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = sybaseContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
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
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
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
        List<DataMap> indexList = sybaseContext.queryAllIndexes(subTableNames);
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
            columns.stream().filter(Objects::nonNull).forEach(col -> {
                TapField tapField = sybaseColumn.initTapField(col);
                tapField.setPos(keyPos.incrementAndGet());
                tapField.setPrimaryKey(primaryKeys.contains(tapField.getName()));
                tapField.setPrimaryKeyPos(primaryKeys.indexOf(tapField.getName()) + 1);
                tapTable.add(tapField);
            });
            tapTable.setIndexList(tapIndexList);
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
}
