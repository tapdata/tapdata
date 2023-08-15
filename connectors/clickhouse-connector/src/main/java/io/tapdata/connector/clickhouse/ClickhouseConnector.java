package io.tapdata.connector.clickhouse;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.clickhouse.bean.ClickhouseColumn;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.connector.clickhouse.ddl.sqlmaker.ClickhouseDDLSqlGenerator;
import io.tapdata.connector.clickhouse.ddl.sqlmaker.ClickhouseSqlMaker;
import io.tapdata.connector.clickhouse.dml.ClickhouseBatchWriter;
import io.tapdata.connector.clickhouse.dml.ClickhouseRecordWriter;
import io.tapdata.connector.clickhouse.dml.TapTableWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
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
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.codec.binary.Base64;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_clickhouse.json")
public class ClickhouseConnector extends CommonDbConnector {

    public static final String TAG = ClickhouseConnector.class.getSimpleName();

    private ClickhouseConfig clickhouseConfig;
    private ClickhouseJdbcContext clickhouseJdbcContext;
    private String clickhouseVersion;

    private final ClickhouseBatchWriter clickhouseWriter = new ClickhouseBatchWriter(TAG);
    private ExecutorService executorService;
    private Long lastMergeTime;
    private final Map<String, TapTable> tapTableMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectionContext) throws SQLException {
        initConnection(connectionContext);
        ddlSqlGenerator = new ClickhouseDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        if (connectionContext instanceof TapConnectorContext) {
            TapConnectorContext tapConnectorContext = (TapConnectorContext) connectionContext;
            Optional.ofNullable(tapConnectorContext.getConnectorCapabilities()).ifPresent(connectorCapabilities -> {
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)).ifPresent(clickhouseWriter::setInsertPolicy);
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)).ifPresent(clickhouseWriter::setUpdatePolicy);
            });
        }

    }

    private void initConnection(TapConnectionContext connectionContext) throws SQLException {
        clickhouseConfig = new ClickhouseConfig().load(connectionContext.getConnectionConfig());
        isConnectorStarted(connectionContext, connectorContext -> clickhouseConfig.load(connectorContext.getNodeConfig()));
        clickhouseJdbcContext = new ClickhouseJdbcContext(clickhouseConfig);
        commonDbConfig = clickhouseConfig;
        jdbcContext = clickhouseJdbcContext;
        clickhouseVersion = clickhouseJdbcContext.queryVersion();
        commonSqlMaker = new ClickhouseSqlMaker().withVersion(clickhouseVersion);
        tapLogger = connectionContext.getLog();
        exceptionCollector = new ClickhouseExceptionCollector();
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = TapSimplify.list();
        List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
        List<DataMap> columnList = jdbcContext.queryAllColumns(subTableNames);
        subList.forEach(subTable -> {
            //1、table name/comment
            String table = subTable.getString("tableName");
            TapTable tapTable = table(table);
            tapTable.setComment(subTable.getString("tableComment"));
            //2、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            AtomicInteger primaryKeyPos = new AtomicInteger(0);
//            AtomicInteger partitionKeyPos = new AtomicInteger(0);
            List<TapIndexField> sortingIndexFields = new ArrayList<>();
            List<TapIndexField> primaryIndexFields = new ArrayList<>();
            columnList.stream().filter(col -> table.equals(col.getString("tableName")))
                    .forEach(col -> {
                        ClickhouseColumn column = new ClickhouseColumn(col);
                        TapField tapField = column.getTapField();
                        tapField.setPos(keyPos.incrementAndGet());
                        if (column.isPrimary()) {
                            tapField.setPrimaryKey(true);
                            tapField.setPrimaryKeyPos(primaryKeyPos.incrementAndGet());
                            primaryIndexFields.add(new TapIndexField().name(tapField.getName()).fieldAsc(true));
                        }
                        if (column.isSorting()) {
                            sortingIndexFields.add(new TapIndexField().name(tapField.getName()).fieldAsc(true));
                        }
//                        if (column.isPartition()) {
//                            tapField.setPartitionKey(column.isPartition());
//                            tapField.setPartitionKeyPos(partitionKeyPos.incrementAndGet());
//                        }
                        tapTable.add(tapField);
                    });
            if (EmptyKit.isNotEmpty(primaryIndexFields)) {
                TapIndex tapIndex = new TapIndex().primary(true).unique(true);
                tapIndex.setIndexFields(primaryIndexFields);
                tapTable.add(tapIndex);
            }
            if (EmptyKit.isNotEmpty(sortingIndexFields) && (primaryIndexFields.size() != sortingIndexFields.size())) {
                TapIndex tapIndex = new TapIndex().primary(false).unique(false);
                tapIndex.setIndexFields(sortingIndexFields);
                tapTable.add(tapIndex);
            }
            tapTableList.add(tapTable);
        });
        syncSchemaSubmit(tapTableList, consumer);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        isConnectorStarted(connectionContext, this::mergeWriteRecordTables);
        EmptyKit.closeQuietly(clickhouseJdbcContext);
        EmptyKit.closeQuietly(clickhouseWriter);
        if (EmptyKit.isNotNull(executorService)) {
            executorService.shutdown();
            executorService = null;
        }
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
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "UInt8", tapValue -> {
            if (tapValue.getValue()) return 1;
            else return 0;
        });

        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "String", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return new String(Base64.encodeBase64(tapValue.getValue()));
            return null;
        });

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
//        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            DateTime datetime = tapDateTimeValue.getValue();
//            datetime.setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return datetime.toTimestamp();
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            DateTime datetime = tapDateValue.getValue();
//            datetime.setTimeZone(TimeZone.getTimeZone(this.connectionTimezone));
            return datetime.toSqlDate();
        });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
//        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportWriteRecord(this::writeRecord);


        //source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        //query
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);

        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> clickhouseJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportCountRawCommandFunction(this::countRawCommand);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);

    }

    protected CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws SQLException {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (clickhouseJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(TapTableWriter.sqlQuota(".", clickhouseConfig.getDatabase(), tapTable.getId()));
        sql.append("(").append(commonSqlMaker.buildColumnDefinition(tapTable, true));
        sql.setLength(sql.length() - 1);

        // primary key
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sql.append(") ENGINE = ReplacingMergeTree");
            sql.append(" PRIMARY KEY (").append(TapTableWriter.sqlQuota(",", primaryKeys)).append(")");
        } else {
            sql.append(") ENGINE = MergeTree");
        }

        // sorting key
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sql.append(" ORDER BY (").append(TapTableWriter.sqlQuota(",", primaryKeys)).append(")");
        } else {
            sql.append(" ORDER BY tuple()");
        }

        try {
            List<String> sqlList = TapSimplify.list();
            sqlList.add(sql.toString());
            TapLogger.info("table :", "table -> {}", tapTable.getId());
            clickhouseJdbcContext.batchExecute(sqlList);
        } catch (Throwable e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage(), e);
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        synchronized (this) {
            if (EmptyKit.isNull(lastMergeTime)) {
                lastMergeTime = (Long) tapConnectorContext.getStateMap().get("lastMergeTime");
            }
            if (EmptyKit.isNull(lastMergeTime)) {
                lastMergeTime = System.currentTimeMillis();
                tapConnectorContext.getStateMap().put("lastMergeTime", lastMergeTime);
            }
            startMergeThreadIfNeeded(tapConnectorContext, tapTable);
        }
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new ClickhouseRecordWriter(clickhouseJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .setTapLogger(tapLogger)
                .write(tapRecordEvents, consumer, this::isAlive);
    }

    private void startMergeThreadIfNeeded(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        if (!tapTableMap.containsKey(tapTable.getId())) {
            tapTableMap.put(tapTable.getId(), tapTable);
        }
        if (EmptyKit.isNull(executorService)) {
            executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            executorService.submit(() -> {
                while (isAlive()) {
                    TapSimplify.sleep(1000);
                    if (System.currentTimeMillis() - lastMergeTime > 1000L * 60 * clickhouseConfig.getMergeMinutes()) {
                        mergeWriteRecordTables(tapConnectorContext);
                    }
                }
            });
        }
    }

    private void mergeWriteRecordTables(TapConnectorContext connectorContext) {
        List<String> sqlList = tapTableMap.values().stream().map(v -> "OPTIMIZE TABLE `" + clickhouseConfig.getDatabase() + "`.`" + v.getId() + "` FINAL").collect(Collectors.toList());
        try {
            tapLogger.info("Clickhouse Optimize Table start, tables: {}", toJson(tapTableMap.keySet()));
            clickhouseJdbcContext.batchExecute(sqlList);
            tapLogger.info("Clickhouse Optimize Table end");
        } catch (Throwable e) {
            tapLogger.warn("Clickhouse Optimize Table failed", e);
        }
        lastMergeTime = System.currentTimeMillis();
        connectorContext.getStateMap().put("lastMergeTime", lastMergeTime);
    }

    //需要改写成ck的创建索引方式
    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) {
        try {
            List<String> sqls = TapSimplify.list();
            if (EmptyKit.isNotEmpty(createIndexEvent.getIndexList())) {
                createIndexEvent.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqls.add("CREATE " + (i.isUnique() ? "UNIQUE " : " ") + "INDEX " +
                                (EmptyKit.isNotNull(i.getName()) ? "IF NOT EXISTS " + TapTableWriter.sqlQuota(i.getName()) : "") + " ON " + TapTableWriter.sqlQuota(".", clickhouseConfig.getDatabase(), tapTable.getId()) + "(" +
                                i.getIndexFields().stream().map(f -> TapTableWriter.sqlQuota(f.getName()) + " " + (f.getFieldAsc() ? "ASC" : "DESC"))
                                        .collect(Collectors.joining(",")) + ')'));
            }
            clickhouseJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Indexes for " + tapTable.getId() + " Failed! " + e.getMessage());
        }

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        clickhouseConfig = new ClickhouseConfig().load(connectionContext.getConnectionConfig());
        try (
                ClickhouseTest clickhouseTest = new ClickhouseTest(clickhouseConfig, consumer)
        ) {
            clickhouseTest.testOneByOne();
            return connectionOptions;
        }
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = clickhouseJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("NUM_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("AVG_ROW_LEN")));
        return tableInfo;
    }

}
