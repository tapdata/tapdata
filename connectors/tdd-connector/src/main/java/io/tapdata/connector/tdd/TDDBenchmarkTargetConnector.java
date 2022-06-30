package io.tapdata.connector.tdd;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("targetDorisSpec.json")
public class TDDBenchmarkTargetConnector extends ConnectorBase {
    public static final String TAG = TDDBenchmarkTargetConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
//    private static Map<String, Map<String, Object>> primaryKeyRecordMap;
//    static {
//        if(primaryKeyRecordMap == null) {
//            primaryKeyRecordMap = new ConcurrentHashMap<>();
//        }
//    }
    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Flow engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        //TODO Load schema from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables with specified fields.
        consumer.accept(list(
                //Define first table
                table("empty-table1")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "string").isPrimaryKey(true).partitionKeyPos(1))
                        .add(field("description", "string").isPrimaryKey(true).partitionKeyPos(2))
                        .add(field("name", "string"))
                        .add(field("age", "int"))
        ));
    }

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
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        //TODO execute connection test here
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO execute read test here
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test here
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        //Read log test to check CDC capability
        //TODO execute read log test here
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));

        //When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
 //        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 1;
    }

    /**
     * Register connector capabilities here.
     *
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchOffset(this::batchOffset);
//        connectorFunctions.supportStreamOffset(this::streamOffset);

        connectorFunctions.supportWriteRecord(this::writeRecord);

        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return "raw";//toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null)
                return "map";//toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return "array";//toJson(tapValue.getValue());
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
                return "binary";//toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "datetime", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return "time"; //toJson(tapValue.getValue());
            return "null";
        });
    }

//    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) {
////        primaryKeyRecordMap.clear();
//    }
//
//    private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) {
////        throw new NullPointerException("aaaa");
//
//    }

//    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, Consumer<List<FilterResult>> listConsumer) {
//        if(filters != null) {
//            List<FilterResult> filterResults = new ArrayList<>();
//            for(TapFilter filter : filters) {
//                Map<String, Object> value = primaryKeyRecordMap.get(primaryKey(connectorContext, filter.getMatch()));
//                FilterResult filterResult = new FilterResult().result(value).filter(filter);
//                filterResults.add(filterResult);
//            }
//            listConsumer.accept(filterResults);
//        }
//    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(needCreateTable)
     *      createTable
     *  if(needClearTable)
     *      clearTable
     *  writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        //TODO write records into database

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for(TapRecordEvent recordEvent : tapRecordEvents) {
            if(recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> value = insertRecordEvent.getAfter();
                if(value != null) {
//                    primaryKeyRecordMap.put(primaryKey(connectorContext, value), value);
                    inserted.incrementAndGet();
                }

//                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;

                Map<String, Object> value = updateRecordEvent.getAfter();
                Map<String, Object> before = updateRecordEvent.getBefore();
                if(value != null && before != null) {
//                    primaryKeyRecordMap.put(primaryKey(connectorContext, before), value);
                    updated.incrementAndGet();
                }
//                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if(recordEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();
                if(before != null) {
//                    primaryKeyRecordMap.remove(primaryKey(connectorContext, before));
                }
                deleted.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }
        //Need to tell flow engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    private String primaryKey(Map<String, Object> value) {
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, Object> entry : value.entrySet()) {
            builder.append(entry.getValue());
        }
        return builder.toString();
    }


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

    }

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
//    @Override
//    public void onDestroy(TapConnectionContext connectionContext) {
//        //TODO release resources
//        isShutDown.set(true);
//    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        isShutDown.set(true);
    }
}
