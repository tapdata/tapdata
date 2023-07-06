package io.tapdata.connector.selectdb;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.connector.selectdb.bean.SelectDbColumn;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.connector.selectdb.exception.SelectDbErrorCodes;
import io.tapdata.connector.selectdb.util.CopyIntoUtils;
import io.tapdata.connector.selectdb.util.HttpUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.write.WriteValve;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:16
 **/

@TapConnectorClass("spec_selectdb.json")
public class SelectDbConnector extends CommonDbConnector {
    public static final String TAG = SelectDbConnector.class.getSimpleName();

    private SelectDbConfig selectDbConfig;
    private SelectDbContext selectDbContext;
    private SelectDbJdbcContext selectDbJdbcContext;
    private String selectDbVersion;
    private SelectDbTest selectDbTest;
    private CopyIntoUtils copyIntoUtils;
    private CopyIntoUtils copyIntoKey;
    private DDLSqlMaker ddlSqlMaker;
    private SelectDbStreamLoader selectDbStreamLoader;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    public static final int size = 50000;
    private static final SelectDbDDLInstance DDLInstance = SelectDbDDLInstance.getInstance();
    private WriteValve valve;

    @Override
    public void onStart(TapConnectionContext connectorContext) throws SQLException {
        this.copyIntoUtils = new CopyIntoUtils(connectorContext);
        this.selectDbContext = new SelectDbContext(connectorContext);
        this.selectDbConfig = new SelectDbConfig().load(connectorContext.getConnectionConfig());
        this.commonDbConfig = this.selectDbConfig;
        this.selectDbTest = new SelectDbTest(selectDbConfig, testItem -> {
        }).initContext();
        selectDbJdbcContext = new SelectDbJdbcContext(selectDbConfig);
        this.jdbcContext = this.selectDbJdbcContext;
        this.selectDbVersion = selectDbJdbcContext.queryVersion();
        this.selectDbStreamLoader = new SelectDbStreamLoader(new HttpUtil().getHttpClient(), selectDbConfig)
                .selectDbJdbcContext(selectDbJdbcContext);
        commonSqlMaker = new SelectDbSqlMaker('`').closeNotNull(selectDbConfig.getCloseNotNull());
        ddlSqlMaker = new SelectDbDDLSqlMaker();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        TapLogger.info(TAG, "SelectDB connector started");
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        EmptyKit.closeQuietly(selectDbJdbcContext);
        ErrorKit.ignoreAnyError(() -> {
            if (EmptyKit.isNotNull(selectDbStreamLoader)) {
                selectDbStreamLoader.shutdown();
            }
        });
        if (this.valve != null) {
            this.valve.close();
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportErrorHandleFunction(this::handleErrors);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null)
                return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "boolean", tapValue -> {
            if (tapValue != null) {
                Boolean value = tapValue.getValue();
                if (value != null && value) {
                    return 1;
                }
            }
            return 0;
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue();
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return formatTapDateTime(tapValue.getValue(), "YYYY-MM-DD HH:MM:SS.ssssss");
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "varchar(10)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().toTimeStr();
            }
            return "null";
        });

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
//        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create().beforeRetryMethod(() -> {
        });
        Throwable match;
        if (null != (match = matchThrowable(throwable, CoreException.class)) && ((CoreException) match).getCode() == SelectDbErrorCodes.ERROR_SDB_COPY_INTO_CANCELLED) {
            retryOptions.needRetry(false);
            return retryOptions;
        }
        return retryOptions.needRetry(true);
    }

    protected void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        List<String> sqls = fieldDDLHandlers.handle(tapFieldBaseEvent, tapConnectorContext);
        if (null == sqls) {
            return;
        }
        for (String sql : sqls) {
            try {
                TapLogger.info(TAG, "Execute ddl sql: " + sql);
                selectDbJdbcContext.execute(sql);
            } catch (Throwable e) {
                throw new RuntimeException("Execute ddl sql failed: " + sql + ", error: " + e.getMessage(), e);
            }
        }
    }

    protected List<String> newField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (Objects.isNull(tapFieldBaseEvent) && Objects.isNull(tapFieldBaseEvent.getTableId())) {
            throw new CoreException("TapFieldBaseEvent and tapConnectorContext can not be empty.");
        }
        if (!(tapFieldBaseEvent instanceof TapNewFieldEvent)) {
            return null;
        }
        TapNewFieldEvent tapNewFieldEvent = (TapNewFieldEvent) tapFieldBaseEvent;
        if (this.valve != null) {
            try {
                this.valve.commit(tapFieldBaseEvent.getTableId());
            } catch (Throwable throwable) {
                throw new CoreException(throwable.getMessage());
            }
        }
        return ddlSqlMaker.addColumn(tapConnectorContext, tapNewFieldEvent);
    }

    protected List<String> alterFieldAttr(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (Objects.isNull(tapFieldBaseEvent) && Objects.isNull(tapFieldBaseEvent.getTableId())) {
            throw new CoreException("TapFieldBaseEvent and tapConnectorContext can not be empty.");
        }
        if (!(tapFieldBaseEvent instanceof TapAlterFieldAttributesEvent)) {
            return null;
        }
        TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = (TapAlterFieldAttributesEvent) tapFieldBaseEvent;
        if (this.valve != null) {
            try {
                this.valve.commit(tapFieldBaseEvent.getTableId());
            } catch (Throwable throwable) {
                throw new CoreException(throwable.getMessage());
            }
        }
        return ddlSqlMaker.alterColumnAttr(tapConnectorContext, tapAlterFieldAttributesEvent);
    }

    protected List<String> alterFieldName(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (Objects.isNull(tapFieldBaseEvent) && Objects.isNull(tapFieldBaseEvent.getTableId())) {
            throw new CoreException("TapFieldBaseEvent and tapConnectorContext can not be empty.");
        }
        if (!(tapFieldBaseEvent instanceof TapAlterFieldNameEvent)) {
            return null;
        }
        TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapFieldBaseEvent;
        if (this.valve != null) {
            try {
                this.valve.commit(tapFieldBaseEvent.getTableId());
            } catch (Throwable throwable) {
                throw new CoreException(throwable.getMessage());
            }
        }
        return ddlSqlMaker.alterColumnName(tapConnectorContext, tapAlterFieldNameEvent);
    }

    protected List<String> dropField(TapFieldBaseEvent tapFieldBaseEvent, TapConnectorContext tapConnectorContext) {
        if (Objects.isNull(tapFieldBaseEvent) && Objects.isNull(tapFieldBaseEvent.getTableId())) {
            throw new CoreException("TapFieldBaseEvent and tapConnectorContext can not be empty.");
        }
        if (!(tapFieldBaseEvent instanceof TapDropFieldEvent)) {
            return null;
        }
        TapDropFieldEvent tapDropFieldEvent = (TapDropFieldEvent) tapFieldBaseEvent;
        if (this.valve != null) {
            try {
                this.valve.commit(tapFieldBaseEvent.getTableId());
            } catch (Throwable throwable) {
                throw new CoreException(throwable.getMessage());
            }
        }
        return ddlSqlMaker.dropColumn(tapConnectorContext, tapDropFieldEvent);
    }

    private RetryOptions handleErrors(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        return RetryOptions.create().needRetry(true);
    }

    protected void queryByFilter(TapConnectorContext tapConnectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM `" + selectDbConfig.getDatabase() + "`.`" + tapTable.getId() + "` WHERE " + buildKeyAndValue(filter.getMatch(), "AND", "=");
            FilterResult filterResult = new FilterResult();
            try {
                selectDbJdbcContext.queryWithNext(sql, resultSet -> filterResult.setResult(DbKit.getRowFromResultSet(resultSet, columnNames)));
            } catch (Throwable e) {
                filterResult.setError(e);
            } finally {
                filterResults.add(filterResult);
            }
        }
        listConsumer.accept(filterResults);
    }

    public static String buildKeyAndValue(Map<String, Object> record, String splitSymbol, String operator) {
        StringBuilder builder = new StringBuilder();
        if (EmptyKit.isNotEmpty(record)) {
            record.forEach((fieldName, value) -> {
                builder.append('`').append(fieldName).append('`').append(operator);
                if (!(value instanceof Number)) {
                    builder.append('\'').append(value).append('\'');
                } else {
                    builder.append(value);
                }
                builder.append(' ').append(splitSymbol).append(' ');
            });
            builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
        }
        return builder.toString();
    }

    @Override
    protected String getSchemaAndTable(String tableId) {
        StringBuilder sb = new StringBuilder();
        if (EmptyKit.isNotBlank(commonDbConfig.getDatabase())) {
            sb.append("`").append(commonDbConfig.getDatabase()).append("`").append('.');
        }
        sb.append("`").append(tableId).append("`");
        return sb.toString();
    }

    //    private final Object lock = new int[0];
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
//        if (Objects.isNull(this.valve)) {
//            synchronized (lock) {
//                if (Objects.isNull(this.valve)) {
//                    this.valve = WriteValve.open(
//                            50000,
//                            10,
//                            this::uploadEvents,
//                            writeListResultConsumer
//                    ).start();
//                }
//            }
//        }
//        this.valve.write(tapRecordEvents, tapTable);
        uploadEvents(connectorContext, writeListResultConsumer, tapRecordEvents, tapTable);
    }

    /**
     * This is a thread safe method. Please upload synchronously
     *
     * @param writeListResultConsumer
     * @param events
     */
    private void uploadEvents(TapConnectorContext connectorContext,Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table) {

        int catchCount = 0;
        while (catchCount <= selectDbConfig.getRetryCount()) {
            try {
                WriteListResult<TapRecordEvent> writeListResult = selectDbStreamLoader.writeRecord(connectorContext, events, table);
                writeListResultConsumer.accept(writeListResult);
                break;
            } catch (Throwable e) {
                catchCount++;
                if (catchCount <= selectDbConfig.getRetryCount()) {
                    connectorContext.getLog().warn("Data source upload retry: {}", catchCount);
                } else {
                    TapLogger.error(TAG, "Data write failure" + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //get table info
        List<DataMap> tableList = selectDbJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
            Map<String, List<DataMap>> columnList = selectDbJdbcContext.queryAllColumnsGroupByTableName(subTableNames);
            List<DataMap> indexList = selectDbJdbcContext.queryAllIndexes(subTableNames);
            //make up tapTable
            subList.forEach(subTable -> {
                //1、table name/comment
                String table = subTable.getString("TABLE_NAME");
                TapTable tapTable = table(table);
                tapTable.setComment(subTable.getString("COMMENT"));
                //2、primary key and table index (find primary key from index info)
                List<String> primaryKey = TapSimplify.list();
                List<TapIndex> tapIndexList = TapSimplify.list();
                Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("TABLE_NAME")))
                        .collect(Collectors.groupingBy(idx -> idx.getString("TABLE_NAME"), LinkedHashMap::new, Collectors.toList()));
                indexMap.forEach((key, value) -> {
                    if (value.stream().anyMatch(v -> (boolean) v.get("IS_PRIMARY"))) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("COLUMN_NAME")).collect(Collectors.toList()));
                    }
                    TapIndex index = index(key);
                    value.forEach(v -> index.indexField(indexField(v.getString("COLUMN_NAME")).fieldAsc("A".equals(v.getString("ASC_OR_DESC")))));
                    index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("IS_UNIQUE")));
                    index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("IS_PRIMARY")));
                    tapIndexList.add(index);
                });
                //3、table columns info
                LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
                List<DataMap> columnFields = columnList.get(table);
                columnFields.stream().filter(Objects::nonNull).forEach(col -> {
                    TapField tapField = SelectDbColumn.create(col).getTapField();
                    fieldMap.put(tapField.getName(), tapField);
                });
                tapTable.setNameFieldMap(fieldMap);
                tapTable.setIndexList(tapIndexList);
                tapTableList.add(tapTable);
            });
            consumer.accept(tapTableList);
        });
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
        DataMap connectionConfig = databaseContext.getConnectionConfig();
        if (Objects.isNull(connectionConfig)) {
            throw new CoreException("connectionConfig cannot be null");
        }
        selectDbConfig = new SelectDbConfig().load(connectionConfig);
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(selectDbConfig.getConnectionString());
        CopyIntoUtils.setConfig(databaseContext);

        try (SelectDbTest selectDbTest = new SelectDbTest(selectDbConfig, consumer).initContext()) {
            selectDbTest.testOneByOne();
            return connectionOptions;
        }

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        this.selectDbJdbcContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
            if (rs.next()) {
                count.set(Integer.parseInt(rs.getString("count")));
            }
        });
        return count.get();
    }

    protected CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        String database = selectDbContext.getSelectDbConfig().getDatabase();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<DataMap> tableNames = selectDbJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId()));
        if (tableNames.contains(tapTable.getId())) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        this.copyIntoKey = new CopyIntoUtils(EmptyKit.isEmpty(primaryKeys));
        String firstColumn = tapTable.getNameFieldMap().values().stream().findFirst().orElseGet(TapField::new).getName();
        String sql = "CREATE TABLE IF NOT EXISTS `" + database + "`.`" + tapTable.getId() + "`(" +
                DDLInstance.buildColumnDefinition(tapTable) + ")";
        try {
            List<String> sqls = TapSimplify.list();
            if (EmptyKit.isEmpty(primaryKeys)) {
                sql += "DUPLICATE KEY (`" + firstColumn + "` ) " +
                        "DISTRIBUTED BY HASH(`" + firstColumn + "` ) BUCKETS 10 ";
            } else if (EmptyKit.isNotNull(tapTable.getComment())) {
                sql += "UNIQUE KEY (" + DDLInstance.buildDistributedKey(primaryKeys) + " ) " +
                        "COMMENT \"" + tapTable.getComment() + "\"" +
                        "DISTRIBUTED BY HASH(" + DDLInstance.buildDistributedKey(primaryKeys) + " ) BUCKETS 10 ";
            } else {
                sql += "UNIQUE KEY (" + DDLInstance.buildDistributedKey(primaryKeys) + " ) " +
                        "DISTRIBUTED BY HASH(" + DDLInstance.buildDistributedKey(primaryKeys) + " ) BUCKETS 10 ";
            }
            sqls.add(sql);
            selectDbJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (selectDbJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                selectDbJdbcContext.execute("TRUNCATE TABLE `" + selectDbConfig.getDatabase() + "`.`" + tapClearTableEvent.getTableId() + "`");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            selectDbJdbcContext.execute("DROP TABLE IF EXISTS `" + selectDbConfig.getDatabase() + "`.`" + tapDropTableEvent.getTableId() + "`");
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    protected void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        selectDbJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }
}