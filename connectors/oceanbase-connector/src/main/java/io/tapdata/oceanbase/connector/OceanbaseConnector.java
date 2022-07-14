package io.tapdata.oceanbase.connector;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.oceanbase.OceanbaseMaker;
import io.tapdata.oceanbase.OceanbaseSchemaLoader;
import io.tapdata.oceanbase.OceanbaseWriter;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/23 15:56
 */

@TapConnectorClass("oceanbase-spec.json")
public class OceanbaseConnector extends ConnectorBase {
    public static final String TAG = OceanbaseConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    private OceanbaseJdbcContext oceanbaseJdbcContext;
    private OceanbaseWriter oceanbaseWriter;
    private String version;
    private String connectionTimezone;

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
        //TODO execute connection test here
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO execute read test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
//        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
        return null;
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
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);

        //If database need insert record before table created, then please implement the below two methods.
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);

        //If database need insert record before table created, please implement the custom codec for the TapValue that data types in spec.json didn't cover.
        //TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue
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
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
    }

    /**
     *
     * @param tapConnectorContext
     * @param tapRecordEvents
     * @param tapTable
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.oceanbaseWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
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
                String mysqlVersion = oceanbaseJdbcContext.getOceanbaseVersion();
                if (null == createTableEvent.getTable()) {
                    TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + createTableEvent);
                    return;
                }
                String[] createTableSqls = OceanbaseMaker.createTable(connectorContext, createTableEvent, mysqlVersion);
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
        this.oceanbaseJdbcContext = new OceanbaseJdbcContext(tapConnectionContext);
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.oceanbaseWriter = new OceanbaseWriter(oceanbaseJdbcContext);
            this.version = oceanbaseJdbcContext.getOceanbaseVersion();
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.connectionTimezone = oceanbaseJdbcContext.timezone();
            }
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            this.oceanbaseJdbcContext.close();
        } catch (Exception e) {
            TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
        }
        Optional.ofNullable(this.oceanbaseWriter).ifPresent(OceanbaseWriter::onDestroy);
    }
}
