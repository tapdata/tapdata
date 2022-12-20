package io.tapdata.connector.selectdb;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.connector.selectdb.bean.SelectDbColumn;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.connector.selectdb.streamload.CopyIntoUtils;
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
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.io.IOException;
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
public class SelectDbConnector extends ConnectorBase {
    public static final String TAG = SelectDbConnector.class.getSimpleName();

    private SelectDbConfig selectDbConfig;
    private SelectDbContext selectDbContext;
    private SelectDbJdbcContext selectDbJdbcContext;
    private String selectDbVersion;
    private SelectDbTest selectDbTest;
    private DDLSqlGenerator ddlSqlGenerator;
    private CopyIntoUtils copyIntoUtils;
    private SelectDbStreamLoader selectDbStreamLoader;
    private static final SelectDbDDLInstance DDLInstance = SelectDbDDLInstance.getInstance();


    @Override
    public void onStart(TapConnectionContext connectorContext) {
        this.copyIntoUtils = new CopyIntoUtils(connectorContext);
        this.selectDbContext = new SelectDbContext(connectorContext);
        this.selectDbConfig = new SelectDbConfig().load(connectorContext.getConnectionConfig());
        this.selectDbTest = new SelectDbTest(selectDbConfig, testItem -> {
        }).initContext();
        if (EmptyKit.isNull(selectDbJdbcContext) || selectDbJdbcContext.isFinish()) {
            selectDbJdbcContext = (SelectDbJdbcContext) DataSourcePool.getJdbcContext(selectDbConfig, SelectDbJdbcContext.class, connectorContext.getId());
        }
        this.selectDbVersion = selectDbJdbcContext.queryVersion();
        this.selectDbStreamLoader = new SelectDbStreamLoader(selectDbContext, new HttpUtil().getHttpClient());
        this.selectDbStreamLoader = new SelectDbStreamLoader(new HttpUtil().getHttpClient(), selectDbConfig);

        TapLogger.info(TAG, "SelectDB connector started");
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        try {
            this.selectDbStreamLoader.shutdown();
        } catch (Exception e) {

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
        codecRegistry.registerFromTapValue(TapTimeValue.class, "datetime", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    private RetryOptions handleErrors(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        return RetryOptions.create().needRetry(true);
    }

    private void queryByFilter(TapConnectorContext tapConnectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        for (TapFilter filter : filters) {
            String sql = "SELECT * FROM \"" + selectDbConfig.getSchema() + "\".\"" + tapTable.getId() + "\" WHERE " + CommonSqlMaker.buildKeyAndValue(filter.getMatch(), "AND", "=");
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

    private TapEventCollector tapEventCollector;

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws IOException {
        if (tapEventCollector == null) {
            synchronized (this) {
                if (tapEventCollector == null) {
                    tapEventCollector = TapEventCollector.create()
                            .maxRecords(500)
                            .idleSeconds(1)
                            .table(tapTable)
                            .writeListResultConsumer(writeListResultConsumer)
                            .eventCollected(this::uploadEvents);
                }
            }
        }
        tapEventCollector.addTapEvents(tapRecordEvents);
    }

    /**
     * This is a thread safe method. Please upload synchronously
     *
     * @param writeListResultConsumer
     * @param events
     */
    private void uploadEvents(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table) {
        //TODO upload tapEvents to selectDB.

        try {
            selectDbStreamLoader.writeRecord(events, table, writeListResultConsumer);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        WriteListResult<TapRecordEvent> listResult = writeListResult();
        writeListResultConsumer.accept(new WriteListResult<TapRecordEvent>().insertedCount(events.size()));
    }

    private void writeRecord1(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws IOException {
        selectDbStreamLoader.writeRecord(tapRecordEvents, tapTable, writeListResultConsumer);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        //get table info
        List<DataMap> tableList = selectDbJdbcContext.queryAllTables(tables);
        //paginate by tableSize
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
            List<DataMap> columnList = selectDbJdbcContext.queryAllColumns(subTableNames);
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
                    if (value.stream().anyMatch(v -> (boolean) v.get("is_primary"))) {
                        primaryKey.addAll(value.stream().map(v -> v.getString("COLUMN_NAME")).collect(Collectors.toList()));
                    }
                    TapIndex index = index(key);
                    value.forEach(v -> index.indexField(indexField(v.getString("COLUMN_NAME")).fieldAsc("A".equals(v.getString("asc_or_desc")))));
                    index.setUnique(value.stream().anyMatch(v -> (boolean) v.get("is_unique")));
                    index.setPrimary(value.stream().anyMatch(v -> (boolean) v.get("is_primary")));
                    tapIndexList.add(index);
                });
                //3、table columns info
                AtomicInteger keyPos = new AtomicInteger(0);
                columnList.stream().filter(col -> table.equals(col.getString("TABLE_NAME")))
                        .forEach(col -> {
                            TapField tapField = new SelectDbColumn(col).getTapField(); //make up fields
                            tapField.setPos(keyPos.incrementAndGet());
                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            tapTable.add(tapField);
                        });
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

        try (SelectDbTest selectDbTest = new SelectDbTest(selectDbConfig, consumer).initContext()) {
            selectDbTest.testOneByOne();
            return connectionOptions;
        }

    }


    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return selectDbJdbcContext.queryAllTables(null).size();
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        String database = selectDbContext.getSelectDbConfig().getDatabase();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<DataMap> tableNames = selectDbJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId()));
        if (tableNames.contains(tapTable.getId())) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys();
        String firstColumn = tapTable.getNameFieldMap().values().stream().findFirst().orElseGet(TapField::new).getName();
        String sql = "CREATE TABLE IF NOT EXISTS " + database + "." + tapTable.getId() + "(" +
                DDLInstance.buildColumnDefinition(tapTable) + ")";
        if (EmptyKit.isEmpty(primaryKeys)) {
            sql += "DUPLICATE KEY (" + firstColumn + " ) " +
                    "DISTRIBUTED BY HASH(" + firstColumn + " ) BUCKETS 10 ";
        } else {
            sql += "UNIQUE KEY (" + DDLInstance.buildDistributedKey(primaryKeys) + " ) " +
                    "DISTRIBUTED BY HASH(" + DDLInstance.buildDistributedKey(primaryKeys) + " ) BUCKETS 10 ";
        }
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqls.add("COMMENT ON TABLE \"" + selectDbConfig.getSchema() + "\".\"" + tapTable.getId() + "\" IS '" + tapTable.getComment() + "'");
            }
            Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
            for (String fieldName : fieldMap.keySet()) {
                String fieldComment = fieldMap.get(fieldName).getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqls.add("COMMENT ON COLUMN \"" + selectDbConfig.getSchema() + "\".\"" + tapTable.getId() + "\".\"" + fieldName + "\" IS '" + fieldComment + "'");
                }
            }
            selectDbJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (selectDbJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() > 1) {
                selectDbJdbcContext.execute("TRUNCATE TABLE \"" + selectDbConfig.getSchema() + "\".\"" + tapClearTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (selectDbJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                selectDbJdbcContext.execute("DROP TABLE IF EXISTS \"" + selectDbConfig.getSchema() + "\".\"" + tapDropTableEvent.getTableId() + "\"");
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        selectDbJdbcContext.queryAllTables(TapSimplify.list(), batchSize, listConsumer);
    }
}

