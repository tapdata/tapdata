package io.tapdata.connector.tdd;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("sourceBenchmarkNoTableSpec.json")
public class TDDBenchmarkNoTableSourceConnector extends ConnectorBase {
    public static final String TAG = TDDBenchmarkNoTableSourceConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

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
        //TODO originType最好使用标准列类型来表达， 避免混淆
        consumer.accept(list(
                //Define first table
                table("tdd-table")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "tapString").isPrimaryKey(true).primaryKeyPos(1))
                        .add(field("0", "tapString"))
                        .add(field("1", "tapString"))
                        .add(field("2", "tapString"))
                        .add(field("3", "tapString"))
                        .add(field("4", "tapString"))
                        .add(field("5", "tapString"))
                        .add(field("6", "tapString"))
                        .add(field("7", "tapString"))
                        .add(field("8", "tapString"))
                        .add(field("9", "tapString"))
                        .add(field("10", "tapString"))
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
        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportWriteRecord(this::writeRecord);

        codecRegistry.registerToTapValue(TDDUser.class, (value, tapType) -> new TapStringValue(toJson(value)));

        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportBatchOffset(this::batchOffset);
//        connectorFunctions.supportStreamOffset(this::streamOffset);
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportControlFunction(this::control);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @return
     */
    private long batchCount(TapConnectorContext connectorContext, TapTable table) {
        //TODO Count the batch size.
        return 1000000L;
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param table
     * @param offsetState
     * @param eventsOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        //TODO batch read all records from database, use consumer#accept to send to flow engine.
        //Below is sample code to generate records directly.
        List<TapEvent> tapEvents = list();
        for (int j = 0; j < 1000; j++) {
            for (int i = 0; i < 1000; i++) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", String.valueOf((j * 1000 + i)));
                for(int m = 0; m < 4; m++) {
                    String key = String.valueOf(m);
                    map.put(key, key);
                }
                TapInsertRecordEvent recordEvent = insertRecordEvent(map, table.getId());
                counter.incrementAndGet();
                tapEvents.add(recordEvent);
                if(tapEvents.size() >= eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, null);
                    tapEvents = list();
                }
            }
        }
        if(!tapEvents.isEmpty())
            eventsOffsetConsumer.accept(tapEvents, null);
        counter.set(counter.get() + 1000);
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

    }

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     * <p>
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
