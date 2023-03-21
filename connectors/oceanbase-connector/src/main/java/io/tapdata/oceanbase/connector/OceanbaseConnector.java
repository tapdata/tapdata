package io.tapdata.oceanbase.connector;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.oceanbase.*;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/23 15:56
 */
@TapConnectorClass("oceanbase-spec.json")
public class OceanbaseConnector extends CommonDbConnector {
    public static final String TAG = OceanbaseConnector.class.getSimpleName();

    private OceanbaseJdbcContext oceanbaseJdbcContext;
    private String connectionTimezone;
    private OceanbaseConfig oceanbaseConfig;

    private final OceanbaseWriter oceanbaseWriter = new OceanbaseWriter();

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> destroy -> ended
     * <p>
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Incremental engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        OceanbaseSchemaLoader oceanbaseSchemaLoader = new OceanbaseSchemaLoader(oceanbaseJdbcContext);
        oceanbaseSchemaLoader.discoverSchema(tables, consumer, tableSize);
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> destroy -> ended
     * <p>
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        oceanbaseConfig = new OceanbaseConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try (
                OceanbaseTest oceanbaseTest = new OceanbaseTest(oceanbaseConfig, consumer)
        ) {
            oceanbaseTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        final OceanbaseSchemaLoader oceanbaseSchemaLoader = new OceanbaseSchemaLoader(oceanbaseJdbcContext);
        return oceanbaseSchemaLoader.queryAllTables(oceanbaseJdbcContext.getDatabase(), null).size();
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a target, please implement WriteRecordFunction, QueryByFilterFunction and DropTableFunction.
     * WriteRecordFunction is to write insert/update/delete events into database.
     * QueryByFilterFunction will be used to verify written record is the same with the record query from database base on the same primary keys.
     * DropTableFunction here will be used to drop the table created by tests.
     * <p>
     * If the database need create table before record insertion, then please implement CreateTableFunction,
     * Incremental engine will generate the data types for each field base on incoming records for CreateTableFunction to create the table.
     * </p>
     *
     * <p>
     * To be as a source, please implement BatchReadFunction, BatchCountFunction, BatchOffsetFunction, StreamReadFunction and StreamOffsetFunction, QueryByAdvanceFilterFunction.
     * If the data is schema free which can not fill TapField for TapTable in discoverSchema method, Incremental Engine will sample some records to build TapField by QueryByAdvanceFilterFunction.
     * QueryByFilterFunction is not necessary, once implemented QueryByAdvanceFilterFunction.
     * BatchReadFunction is to read initial records from beginner or offset.
     * BatchCountFunction is to count initial records from beginner or offset.
     * BatchOffsetFunction is to return runtime offset during reading initial records, if batchRead not started yet, return null.
     * StreamReadFunction is to start CDC to read incremental record events, insert/update/delete.
     * StreamOffsetFunction is to return stream offset for specified timestamp or runtime stream offset.
     * </p>
     * <p>
     * If defined data types in spec.json is not covered all the TapValue,
     * like TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue,
     * then please provide the custom codec for missing TapValue by using codeRegistry.
     * This is only needed when database need create table before insert records.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        //If database need insert record before table created, then please implement the below two methods.
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);

        //If database need insert record before table created, please implement the custom codec for the TapValue that data types in spec.json didn't cover.
        //TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue
        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) {
                return toJson(tapRawValue.getValue());
            }
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
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> oceanbaseJdbcContext.getConnection(), c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
    }

    /**
     * @param tapConnectorContext
     * @param tapRecordEvents
     * @param tapTable
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();
        TapTableWriter instance = oceanbaseWriter.partition(oceanbaseJdbcContext, tapTable, this::isAlive);
        for (TapRecordEvent event : tapRecordEvents) {
            if (!isAlive()) {
                throw new InterruptedException("node not alive");
            }
            instance.addBath(event, writeListResult);
        }
        instance.summit(writeListResult);
        writeListResultConsumer.accept(writeListResult);
    }

    /**
     * The method will mainly be used by TDD tests. To verify the record has writen correctly or not.
     *
     * @param connectorContext
     * @param filters          Multple fitlers, need return multiple filter results
     * @param listConsumer     tell incremental engine the filter results according to filters
     */
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) throws Throwable {
        //Filter is exactly match.
        //If query by the filter, no value is in database, please still create a FilterResult with null value in it. So that incremental engine can understand the filter has no value.
        this.oceanbaseJdbcContext.queryByFilter(connectorContext, filters, tapTable, listConsumer);
    }

    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        oceanbaseJdbcContext.dropTable(dropTableEvent.getTableId());
    }

    private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws Throwable {
        try {
            if (oceanbaseJdbcContext.tableExists(createTableEvent.getTableId())) {
                DataMap connectionConfig = connectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = createTableEvent.getTableId();
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                if (null == createTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + createTableEvent);
                    return;
                }
                String[] createTableSqls = OceanbaseMaker.createTable(connectorContext, createTableEvent);
                for (String createTableSql : createTableSqls) {
                    try {
                        oceanbaseJdbcContext.execute(createTableSql);
                    } catch (Throwable e) {
                        throw new Exception("Execute create table failed, sql: " + createTableSql + ", message: " + e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable t) {
            throw new Exception("Create table failed, message: " + t.getMessage(), t);
        }
    }

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        oceanbaseConfig = new OceanbaseConfig().load(tapConnectionContext.getConnectionConfig());
        if (EmptyKit.isNull(oceanbaseJdbcContext) || oceanbaseJdbcContext.isFinish()) {
            oceanbaseJdbcContext = (OceanbaseJdbcContext) DataSourcePool.getJdbcContext(oceanbaseConfig, OceanbaseJdbcContext.class, tapConnectionContext.getId());
            oceanbaseJdbcContext.setTapConnectionContext(tapConnectionContext);
        }
        commonDbConfig = oceanbaseConfig;
        jdbcContext = oceanbaseJdbcContext;
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.connectionTimezone = oceanbaseJdbcContext.timezone();
            }
        }

        if (tapConnectionContext instanceof TapConnectorContext) {
            TapConnectorContext tapConnectorContext = (TapConnectorContext) tapConnectionContext;
            Optional.ofNullable(tapConnectorContext.getConnectorCapabilities()).ifPresent(connectorCapabilities -> {
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)).ifPresent(oceanbaseWriter::setInsertPolicy);
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)).ifPresent(oceanbaseWriter::setUpdatePolicy);
            });
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            oceanbaseWriter.close();
        } catch (Exception e) {
            TapLogger.warn(TAG, "close writer failed: {}", e.getMessage());
        }
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) throws Throwable {
        DataMap dataMap = oceanbaseJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }
}
