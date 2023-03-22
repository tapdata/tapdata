package io.tapdata.connector.doris;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.connector.doris.bean.DorisSnapshotOffset;
import io.tapdata.connector.doris.streamload.DorisStreamLoader;
import io.tapdata.connector.doris.streamload.HttpUtil;
import io.tapdata.connector.doris.streamload.exception.DorisRetryableException;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @Author dayun
 * @Date 7/14/22
 */
@TapConnectorClass("spec.json")
public class DorisConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = DorisConnector.class.getSimpleName();
    private DorisContext dorisContext;
    private DorisReader dorisReader;
    private DorisSchemaLoader dorisSchemaLoader;
    private TapConnectionContext connectionContext;
    private Map<String, DorisStreamLoader> dorisStreamLoaderMap = new ConcurrentHashMap<>();
    private DorisConnectionTest dorisConnectionTest;
    private  DorisConfig dorisConfig;
    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Flow engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        dorisSchemaLoader.discoverSchema(connectionContext, dorisContext.getDorisConfig(), tables, consumer, tableSize);
    }

//    @Override
//    public ConnectionOptions connectionTest(TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer) throws Throwable {
//        ConnectionOptions connectionOptions = ConnectionOptions.create();
//        dorisConfig = (DorisConfig) new DorisConfig().load(connectionContext.getConnectionConfig());
//        try (DorisConnectorTest dorisConnectorTest = new DorisConnectorTest(dorisConfig, consumer, connectionOptions);
//        ) {
//            dorisConnectorTest.testOneByOne();
//
//        }
//        return connectionOptions;
//    }
    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
//    @Override
//    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
//        //Assume below tests are successfully, below tests are recommended, but not required.
//        //Connection test
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
//        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
//        //Read test
//        //TODO 通过权限检查有没有读权限, 暂时无法通过jdbc方式获取权限信息
//        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
//        //Write test
//        //TODO 通过权限检查有没有写权限, 暂时无法通过jdbc方式获取权限信息
//        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
//        //When test failed
//        // consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
//        //When test successfully, but some warn is reported.
//        // consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
//        return null;
//    }
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        CommonDbConfig commonDbConfig = new CommonDbConfig();
        commonDbConfig.set__connectionType(databaseContext.getConnectionConfig().getString("__connectionType"));
        try (
                DorisConnectionTest dorisConnectionTest = new DorisConnectionTest(new DorisContext(databaseContext),
                        databaseContext, consumer, commonDbConfig, connectionOptions)
        ) {
            dorisConnectionTest.testOneByOne();
            return connectionOptions;
        }
    }
    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        final String database = this.dorisContext.getDorisConfig().getDatabase();
        final List<String> tables = this.dorisSchemaLoader.queryAllTables(database, null);
        return tables.size();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //source
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        //target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportAlterTable(this::alterTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> dorisContext.getConnection(), c));

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
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
    }


    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
        DorisSnapshotOffset dorisSnapshotOffset;
        if (offset instanceof DorisSnapshotOffset) {
            dorisSnapshotOffset = (DorisSnapshotOffset) offset;
        } else {
            dorisSnapshotOffset = new DorisSnapshotOffset();
        }


        List<TapEvent> tempList = new ArrayList<>();
        this.dorisReader.readWithOffset(tapConnectorContext, tapTable, dorisSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
            TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, null, data, tapTable, "i");
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







    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        return dorisContext.count(tapTable.getName());
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
            DorisContext context = new DorisContext(connectionContext);
            DorisStreamLoader dorisStreamLoader = new DorisStreamLoader(context, new HttpUtil().getHttpClient());
            dorisStreamLoaderMap.put(threadName, dorisStreamLoader);
        }
        return dorisStreamLoaderMap.get(threadName);
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        if (!useStreamLoad()) {
            throw new UnsupportedOperationException("Doris httpUrl is required for write operation");
        }
        getDorisStreamLoader().writeRecord(tapRecordEvents, tapTable, writeListResultConsumer);
    }

    private void queryByFilter(TapConnectionContext connectionContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        List<FilterResult> filterResults = dorisReader.queryByFilter(filters, tapTable);
        listConsumer.accept(filterResults);
    }

    private void createTable(TapConnectionContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws SQLException {
        dorisSchemaLoader.createTable(tapCreateTableEvent.getTable());
    }

//FIXME DOIRS异步执行alter命令，无回调接口，没次对同一个table同时执行一个alter命令；不能保证某个时刻是否存在alter命令正在执行

//    private void alterTable(TapConnectorContext tapConnectorContext, TapAlterTableEvent tapAlterTableEvent)
//        // TODO 需要实现修改表的功能， 不过测试只能先从源端模拟一个修改表事件
//        initConnection(tapConnectorContext.getConnectionConfig());
//        TapTable tapTable = tapConnectorContext.getTable();
//        Set<String> fieldNames = tapTable.getNameFieldMap().keySet();
//        try {
//            for (TapField insertField : tapAlterTableEvent.getInsertFields()) {
//                if (insertField.getOriginType() == null || insertField.getDefaultValue() == null) continue;
//                String sql = "ALTER TABLE " + tapTable.getName() +
//                        " ADD COLUMN " + insertField.getName() + ' ' + insertField.getOriginType() +
//                        " DEFAULT '" + insertField.getDefaultValue() + "'";
//                stmt.execute(sql);
//            }
//            for (String deletedFieldName : tapAlterTableEvent.getDeletedFields()) {
//                if (!fieldNames.contains(deletedFieldName)) continue;
//                String sql = "ALTER TABLE " + tapTable.getName() +
//                        " DROP COLUMN " + deletedFieldName;
//                stmt.execute(sql);
//            }
//            // TODO Doris在文档中没有看到修改列名的相关操作
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw new RuntimeException("ALTER Table " + tapTable.getName() + " Failed! \n ");
//        }
//
//        PDKLogger.info(TAG, "alterTable");
//    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        final String tableName = tapClearTableEvent.getTableId();
        final String databaseName = dorisContext.getDorisConfig().getDatabase();
        dorisSchemaLoader.clearTable(databaseName, tableName);
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        final String tableName = tapDropTableEvent.getTableId();
        final String databaseName = dorisContext.getDorisConfig().getDatabase();
        dorisSchemaLoader.dropTable(databaseName, tableName);
    }

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
    @Override
    public void onStart(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        this.dorisContext = new DorisContext(connectionContext);
        this.dorisReader = new DorisReader(dorisContext);
        this.dorisSchemaLoader = new DorisSchemaLoader(dorisContext);
        TapLogger.info(TAG, "Doris connector started");
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            for (DorisStreamLoader dorisStreamLoader : dorisStreamLoaderMap.values()) {
                if (null != dorisStreamLoader) {
                    dorisStreamLoader.shutdown();
                }
            }
            this.dorisContext.close();
        } catch (Exception e) {
            TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
        }
    }

    private boolean useStreamLoad() {
        return StringUtils.isNotBlank(dorisContext.getDorisConfig().getDorisHttp());
    }
}
