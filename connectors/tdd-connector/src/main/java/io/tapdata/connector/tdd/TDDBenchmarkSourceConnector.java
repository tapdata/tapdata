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

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("sourceBenchmarkSpec.json")
public class TDDBenchmarkSourceConnector extends ConnectorBase {
    public static final String TAG = TDDBenchmarkSourceConnector.class.getSimpleName();
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
                        .add(field("tapString", "tapString").isPrimaryKey(true).primaryKeyPos(2))
                        .add(field("tddUser", "tapString"))
                        .add(field("tapString10", "tapString(10)"))
                        .add(field("tapString10Fixed", "tapString(10) fixed"))
                        .add(field("tapInt", "int"))
                        .add(field("tapBoolean", "tapBoolean"))
                        .add(field("tapDate", "tapDate"))
                        .add(field("tapArrayString", "tapArray"))
                        .add(field("tapArrayDouble", "tapArray"))
                        .add(field("tapArrayTDDUser", "tapArray"))
                        .add(field("tapRawTDDUser", "tapRaw"))
                        .add(field("tapNumber", "tapNumber"))
//                        .add(field("tapNumber8", "tapNumber(8)"))
                        .add(field("tapNumber52", "tapNumber(5, 2)"))
                        .add(field("tapBinary", "tapBinary"))
                        .add(field("tapTime", "tapTime"))
                        .add(field("tapMapStringString", "tapMap"))
                        .add(field("tapMapStringDouble", "tapMap"))
                        .add(field("tapMapStringTDDUser", "tapMap"))
                        .add(field("tapDateTime", "tapDateTime"))
                        .add(field("tapDateTimeTimeZone", "tapDateTime"))
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
        return 1L;
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
     * @param tables
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private Date date = new Date();
    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState, int batchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        //TODO batch read all records from database, use consumer#accept to send to flow engine.

        //Below is sample code to generate records directly.
        for (int j = 0; j < 1000; j++) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < batchSize; i++) {
                TapInsertRecordEvent recordEvent = insertRecordEvent(map(
                        entry("id", "id_" + counter.get()),
                        entry("tapString", "123"),
//                        entry("tddUser", new TDDUser("uid_" + counter.get(), "name_" + counter.get(), "desp_" + counter.get(), (int) counter.get(), TDDUser.GENDER_FEMALE)),
                        entry("tapString10", "1234567890"),
                        entry("tapString10Fixed", "1"),
                        entry("tapInt", 123123),
                        entry("tapBoolean", true),
                        entry("tapDate", date),
                        entry("tapArrayString", list("1", "2", "3")),
                        entry("tapArrayDouble", list(1.1, 2.2, 3.3)),
//                        entry("tapArrayTDDUser", list(new TDDUser("a", "n", "d", 1, TDDUser.GENDER_MALE), new TDDUser("b", "a", "b", 2, TDDUser.GENDER_FEMALE))),
//                        entry("tapRawTDDUser", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)),
                        entry("tapNumber", 123.0),
                        entry("tapNumber(8)", 1111),
                        entry("tapNumber52", 343.22),
                        entry("tapBinary", new byte[]{123, 21, 3, 2}),
                        entry("tapTime", date),
                        entry("tapMapStringString", map(entry("a", "a"), entry("b", "b"))),
                        entry("tapMapStringDouble", map(entry("a", 1.0), entry("b", 2.0))),
//                        entry("tapMapStringTDDUser", map(entry("a", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)))),
                        entry("tapDateTime", date),
                        entry("tapDateTimeTimeZone", date)
                ), table.getId());
                counter.incrementAndGet();
                tapEvents.add(recordEvent);
            }

            eventsOffsetConsumer.accept(tapEvents, null);
        }
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
