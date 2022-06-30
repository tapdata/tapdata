package ${package};

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.error.NotSupportedException;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.entity.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class ${libName}Connector extends ConnectorBase{
	public static final String TAG=${libName}Connector.class.getSimpleName();
	private final AtomicLong counter = new AtomicLong();
	private final AtomicBoolean isShutDown = new AtomicBoolean(false);

	/**
	 * The method invocation life circle is below,
	 * onStart -> discoverSchema -> onStop
	 *
	 * You need to create the connection in onStart method and release the connection in onStop method.
	 * In connectionContext, you can get the connection config which is the user input for your connection form which described in spec.json file.
	 *
	 * Consumer can accept multiple times, especially huge number of table list.
	 * This is sync method, once the method return, Incremental engine will consider schema has been discovered completely.
	 *
	 * @param connectionContext
	 * @param tables only discover the tables in the list.
	 * @param tableSize the max size of a batch when discover tables.
	 * @param consumer use consumer to report the discovered tables.
	 */
	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		//TODO Load schema from database, connection information in connectionContext#getConnectionConfig
		//Sample code shows how to define tables with specified fields.

		consumer.accept(list(
			//Define first table
			table("empty-table1")
				//Define a field named "id", origin field type, whether is primary key and primary key position
				.add(field("id", "VARCHAR").isPrimaryKey(true))
				.add(field("description", "TEXT"))
				.add(field("name", "VARCHAR"))
				.add(field("age", "DOUBLE")),
			//Define second table
			table("empty-table2")
				.add(field("id", "VARCHAR").isPrimaryKey(true))
				.add(field("description", "TEXT"))
				.add(field("name", "VARCHAR"))
				.add(field("age", "DOUBLE"))
		));
	}

	/**
	 * The method invocation life circle is below,
	 * connectionTest
	 * onStart/onStop will not be invoked before/after connectionTest, please create/release connection within connectionTest method.
	 *
	 * You need to create the connection in onStart method and release the connection in onStop method.
	 * In connectionContext, you can get the connection config which is the user input for your connection form which described in spec.json file.
	 *
	 * consumer can call accept method multiple times to test different items
	 *
	 * @param connectionContext
	 * @return ConnectionOptions to specify extra capabilities and ddlEvents this connector support. And timezone this database is using.
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
		return ConnectionOptions.create();
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		return 2;
	}

	/**
	 * The method invocation life circle is below,
	 * registerCapabilities
	 * This method is special method, may be invoked at any time for understanding the connector's capabilities and custom codecs.
	 *
	 * Register connector capabilities here.
	 * <p>
	 * To be as a target, please implement WriteRecordFunction, QueryByFilterFunction/QueryByAdvanceFilterFunction and DropTableFunction.
	 * WriteRecordFunction is to write insert/update/delete events into database.
	 * QueryByFilterFunction/QueryByAdvanceFilterFunction will be used to verify written record is the same with the record query from database base on the same primary keys.
	 * DropTableFunction here will be used to drop the table created by tests.
	 *
	 * If the database need create table before record insertion, then please implement CreateTableFunction,
	 * Incremental engine will generate the data types for each field base on incoming records for CreateTableFunction to create the table.
	 * </p>
	 *
	 * <p>
	 * To be as a source, please implement BatchReadFunction, BatchCountFunction and StreamReadFunction, QueryByAdvanceFilterFunction.
	 * If the data is schema free which can not fill TapField for TapTable in discoverSchema method, Incremental Engine will sample some records to build TapField by QueryByAdvanceFilterFunction.
	 * QueryByFilterFunction is not necessary, once implemented QueryByAdvanceFilterFunction.
	 * BatchReadFunction is to read initial records from beginning or offset.
	 * BatchCountFunction is to count initial records.
	 * StreamReadFunction is to start CDC to read incremental record events, insert/update/delete.
	 * </p>
	 *
	 * If defined data types in spec.json is not covered all the TapValue,
	 * like TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue,
	 * then please provide the custom from codec for missing TapValue by using codeRegistry.
	 * This is only needed when database need create table before insert records.
	 *
	 * If database loaded a field with a special type, you may provide the custom to codec to convert field value into a TapValue.
	 *
	 * @param connectorFunctions
	 * @param codecRegistry
	 */
	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportStreamRead(this::streamRead);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
		connectorFunctions.supportWriteRecord(this::writeRecord);
		connectorFunctions.supportDropTable(this::dropTable);
		connectorFunctions.supportCreateTable(this::createTable);
		connectorFunctions.supportClearTable(this::clearTable);
		connectorFunctions.supportCreateIndex(this::createIndex);
		connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);

		//Below capabilities, developer can decide to implement or not.
		//onStart/onStop mean the start and stop for a task, once the task will be reset, releaseExternalFunction will be called if you support it. Then you can release the external resources for the task.
//		connectorFunctions.supportReleaseExternalFunction(this::releaseExternal);
		//Provide a way to output runtime memory for this connector.
//		connectorFunctions.supportMemoryFetcher(this::memoryFetcher);
		//Query indexes
//		connectorFunctions.supportQueryIndexes(this::queryIndexes);
		//Support alter field name ddl event
//		connectorFunctions.supportAlterFieldNameFunction(this::alterFieldName);
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> timestampToStreamOffset -> onStop
	 *
	 * Use timestamp to get corresponding stream offset; if timestamp is null, means current stream offset.
	 * Support it if your connector are capable to do it.
	 *
	 * @param connectorContext
	 * @param timestamp the timestamp to return corresponding stream offset.
	 * @return
	 */
	private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long timestamp) {
		return null;
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> createIndex -> onStop
	 *
	 * Create index by TapCreateIndexEvent
	 *
	 * @param connectorContext
	 * @param table
	 * @param tapCreateIndexEvent
	 */
	private void createIndex(TapConnectorContext connectorContext, TapTable table, TapCreateIndexEvent tapCreateIndexEvent) {
//		tapCreateIndexEvent.getIndexList();  indexList to create indexes.
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> clearTable -> onStop
	 *
	 * Clear table by TapClearTableEvent.
	 * TapClearTableEvent#tableId specified which table to clear
	 *
	 * @param connectorContext
	 * @param tapClearTableEvent
	 */
	private void clearTable(TapConnectorContext connectorContext, TapClearTableEvent tapClearTableEvent) {
//		tapClearTableEvent.getTableId(); tableId is the table name that to clear.
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> createTable -> onStop
	 *
	 * Create table by TapCreateTableEvent
	 *
	 * @param connectorContext
	 * @param tapCreateTableEvent
	 */
	private void createTable(TapConnectorContext connectorContext, TapCreateTableEvent tapCreateTableEvent) {
//		tapCreateTableEvent.getTable(); table is to create.
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> dropTable -> onStop
	 *
	 * Drop table by TapDropTableEvent
	 * This method will be invoked when user selected drop table before insert records. Or TDD will use this method to drop the table created for test.
	 *
	 * @param connectorContext
	 * @param dropTableEvent
	 */
	private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
//		tapClearTableEvent.getTableId(); tableId is the table name that to drop.
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> queryByAdvanceFilter -> onStop
	 *
	 * This method will be invoked when Incremental Engine need sample some records for generating TapFields or preview records, etc.
	 *
	 * Need to implement Matching, GT, GTE, LT, LTE operators, sorts, limit, projection and skip.
	 *
	 * @param connectorContext
	 * @param tapAdvanceFilter
	 * @param table
	 * @param filterResultsConsumer
	 */
	private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable table, Consumer<FilterResults> filterResultsConsumer) {
//		tapAdvanceFilter.getMatch(); //get match condition, which is a DataMap.
//		tapAdvanceFilter.getOperators(); //operator list, gt, gte, lt, lte, etc.
//		tapAdvanceFilter.getSortOnList(); //sort on list.
//		tapAdvanceFilter.getSkip(); //skip.
//		tapAdvanceFilter.getLimit(); //limit.
//		tapAdvanceFilter.getProjection(); //include or exclude fields.
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> writeRecord -> onStop
	 *
	 * Through this method, Insert/Update/Delete record events will be passed into tapRecordEvents param, apply the event into database.
	 * Use writeListResultConsumer to accept the number of inserted/updated/deleted and errorMap for each failed record event.
	 *
	 * @param connectorContext
	 * @param tapRecordEvents
	 * @param table
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
				inserted.incrementAndGet();
				TapLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
			} else if(recordEvent instanceof TapUpdateRecordEvent) {
				updated.incrementAndGet();
				TapLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
			} else if(recordEvent instanceof TapDeleteRecordEvent) {
				deleted.incrementAndGet();
				TapLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
			}
		}
		//Need to tell incremental engine to write result
		writeListResultConsumer.accept(writeListResult()
			.insertedCount(inserted.get())
			.modifiedCount(updated.get())
			.removedCount(deleted.get()));
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> batchCount -> onStop
	 *
	 * Total count of batch read for specified table
	 *
	 * @param connectorContext
	 * @param table
	 * @return
	 */
	private long batchCount(TapConnectorContext connectorContext, TapTable table) {
		//TODO Count the batch size.
		return 20L;
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> batchRead -> onStop
	 *
	 * In connectorContext,
	 * you can get the connection/node config which is the user input for your connection/node form, described in your spec.json file.
	 *
	 * Param table is the table to do the batch read.
	 *
	 *
	 * @param connectorContext
	 * @param table is the table to do the batch read.
	 * @param offset if null, start from beginning, if not null, start from specified offset.
	 * @param eventBatchSize max batch size for record events.
	 * @param eventsOffsetConsumer consumer accept a list of record events and batch read offset.
	 */
	private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offset, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
		//TODO batch read all records from database, use consumer#accept to send to incremental engine.

		//Below is sample code to generate records directly.
		for (int j = 0; j < 1; j++) {
			List<TapEvent> tapEvents = list();
			for (int i = 0; i < eventBatchSize; i++) {
				TapInsertRecordEvent recordEvent = insertRecordEvent(map(
					entry("id", counter.incrementAndGet()),
					entry("description", "123"),
					entry("name", "123"),
					entry("age", 12)
				), table.getId());
				tapEvents.add(recordEvent);
			}
			eventsOffsetConsumer.accept(tapEvents, null);
		}
		counter.set(counter.get() + 1000);
	}

	/**
	 * The method invocation life circle is below,
	 * onStart -> batchRead -> onStop
	 *
	 * In connectorContext,
	 * you can get the connection/node config which is the user input for your connection/node form, described in your spec.json file.
	 *
	 * @param connectorContext task context.
	 * @param tableList is the tables to do stream read.
	 * @param streamOffset where the stream read will start from.
	 * @param batchSize the max size for each batch.
	 * @param consumer use it to accept a batch of events and mark stream started/stopped.
	 */
	private void streamRead(TapConnectorContext connectorContext, List<String> tableList, Object streamOffset, int batchSize, StreamReadConsumer consumer) {
		//TODO using CDC APi or log to read stream records from database, use consumer#accept to send to incremental engine.

		consumer.streamReadStarted();
		//Below is sample code to generate stream records directly
		while(!isShutDown.get()) {
			String tableId = tableList.get(0);
			List<TapEvent> tapEvents = list();
			for (int i = 0; i < batchSize; i++) {
				TapInsertRecordEvent event = insertRecordEvent(map(
					entry("id", counter.incrementAndGet()),
					entry("description", "123"),
					entry("name", "123"),
					entry("age", 12)
				), tableId);
				tapEvents.add(event);
			}

			sleep(1000L);
			consumer.accept(tapEvents, null);
		}
	}


	/**
	 * Initialize database connection here.
	 *
	 *
	 * @param connectionContext when do one time job, like connectionTest, discoverSchema, etc, TapConnectionContext will be used, otherwise when task actually started, TapConnectorContext will be used.
	 * @throws Throwable
	 */
	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
//		isConnectorStarted(connectionContext, connectorContext -> {
		//Get connectorContext when the task started.
//		});
	}

	/**
	 * Release database connection here.
	 *
	 * @param connectionContext
	 * @throws Throwable
	 */
	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
//		isConnectorStarted(connectionContext, connectorContext -> {
		//Get connectorContext when the task started.
//		});
	}
}
