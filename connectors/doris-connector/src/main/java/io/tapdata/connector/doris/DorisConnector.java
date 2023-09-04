package io.tapdata.connector.doris;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.connector.doris.streamload.DorisStreamLoader;
import io.tapdata.connector.doris.streamload.HttpUtil;
import io.tapdata.connector.doris.streamload.exception.DorisRetryableException;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jarad
 * @date 7/14/22
 */
@TapConnectorClass("spec_doris.json")
public class DorisConnector extends CommonDbConnector {

    public static final String TAG = DorisConnector.class.getSimpleName();
    private DorisJdbcContext dorisJdbcContext;
    private DorisConfig dorisConfig;
    private final Map<String, DorisStreamLoader> dorisStreamLoaderMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) {
        this.dorisConfig = new DorisConfig().load(tapConnectionContext.getConnectionConfig());
        isConnectorStarted(tapConnectionContext, connectorContext -> dorisConfig.load(connectorContext.getNodeConfig()));
        dorisJdbcContext = new DorisJdbcContext(dorisConfig);
        commonDbConfig = dorisConfig;
        jdbcContext = dorisJdbcContext;
        commonSqlMaker = new DorisSqlMaker();
        exceptionCollector = new DorisExceptionCollector();
    }


    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        dorisConfig = new DorisConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(dorisConfig.getConnectionString());
        try (
                DorisTest dorisTest = new DorisTest(dorisConfig, consumer)
        ) {
            dorisTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> dorisJdbcContext.getConnection(), this::isAlive, c));

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

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, "varchar(10)", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().toTimeStr();
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);

    }

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create();
        if (null != matchThrowable(throwable, DorisRetryableException.class)
                || null != matchThrowable(throwable, IOException.class)) {
            retryOptions.needRetry(true);
            return retryOptions;
        }
        return retryOptions;
    }

    private DorisStreamLoader getDorisStreamLoader() {
        String threadName = Thread.currentThread().getName();
        if (!dorisStreamLoaderMap.containsKey(threadName)) {
            DorisJdbcContext context = new DorisJdbcContext(dorisConfig);
            DorisStreamLoader dorisStreamLoader = new DorisStreamLoader(context, new HttpUtil().getHttpClient());
            dorisStreamLoaderMap.put(threadName, dorisStreamLoader);
        }
        return dorisStreamLoaderMap.get(threadName);
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        try {
            if (checkStreamLoad()) {
                getDorisStreamLoader().writeRecord(tapRecordEvents, tapTable, writeListResultConsumer);
            } else {
                // TODO: 2023/4/28 jdbc writeRecord
            }
        }catch (Throwable t){
            exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), t);
            throw t;
        }
    }

    @Override
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (jdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        String sql;
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (CollectionUtils.isEmpty(primaryKeys)) {
            //append mode
            if (EmptyKit.isEmpty(dorisConfig.getDuplicateKey())) {
                Collection<String> allColumns = tapTable.getNameFieldMap().keySet();
                sql = "CREATE TABLE IF NOT EXISTS " + getSchemaAndTable(tapTable.getId()) +
                        "(" + commonSqlMaker.buildColumnDefinition(tapTable, true) + ") " +
                        "UNIQUE KEY (`" + String.join("`,`", allColumns) + "`) " +
                        "DISTRIBUTED BY HASH(`" + String.join("`,`", allColumns) + "`) BUCKETS 16 " +
                        "PROPERTIES(\"replication_num\" = \"" +
                        dorisConfig.getReplicationNum().toString() +
                        "\")";
            } else {
                sql = "CREATE TABLE IF NOT EXISTS " + getSchemaAndTable(tapTable.getId()) +
                        "(" + ((DorisSqlMaker) commonSqlMaker).buildColumnDefinitionByOrder(tapTable, dorisConfig.getDuplicateKey()) + ") " +
                        "DUPLICATE KEY (`" + String.join("`,`", dorisConfig.getDuplicateKey()) + "`) " +
                        "DISTRIBUTED BY HASH(`" + String.join("`,`", dorisConfig.getDistributedKey()) + "`) BUCKETS 16 " +
                        "PROPERTIES(\"replication_num\" = \"" +
                        dorisConfig.getReplicationNum().toString() +
                        "\")";
            }
        } else {
            sql = "CREATE TABLE IF NOT EXISTS " + getSchemaAndTable(tapTable.getId()) +
                    "(" + ((DorisSqlMaker) commonSqlMaker).buildColumnDefinitionByOrder(tapTable, primaryKeys) + ") " +
                    "UNIQUE KEY (`" + String.join("`,`", primaryKeys) + "`) " +
                    "DISTRIBUTED BY HASH(`" + String.join("`,`", primaryKeys) + "`) BUCKETS 16 " +
                    "PROPERTIES(\"replication_num\" = \"" +
                    dorisConfig.getReplicationNum().toString() +
                    "\")";
        }
        createTableOptions.setTableExists(false);
        try {
            dorisJdbcContext.execute(sql);
            return createTableOptions;
        } catch (Exception e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed | Error: " + e.getMessage() + " | Sql: " + sql, e);
        }
    }

    //the second method to load schema instead of mysql
    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = dorisJdbcContext.queryTablesDesc(subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList()));
        syncSchemaSubmit(tapTableList, consumer);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new MysqlColumn(dataMap).getTapField();
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            ErrorKit.ignoreAnyError(() -> {
                for (DorisStreamLoader dorisStreamLoader : dorisStreamLoaderMap.values()) {
                    if (EmptyKit.isNotNull(dorisStreamLoader)) {
                        dorisStreamLoader.shutdown();
                    }
                }
            });
            if (EmptyKit.isNotNull(dorisJdbcContext)) {
                dorisJdbcContext.close();
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
        }
    }

    private boolean checkStreamLoad() {
        // TODO: 2023/4/28 check stream load
        return true;
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = dorisJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }
}
