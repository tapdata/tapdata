package ${package};

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.entity.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class ${libName}Connector extends ConnectorBase {
	public static final String TAG = ${libName}Connector.class.getSimpleName();
	private KVMap<Object> storageMap;

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
	 * @param tables only discover the tables in the list, if tables is null or empty, means discover all tables.
	 * @param tableSize the max size of a batch to accept when discover tables.
	 * @param consumer use consumer to report the discovered tables.
	 */
	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		//TODO Load schema from database, connection information in connectionContext#getConnectionConfig
		TapLogger.info(TAG, "discoverSchema {}", connectionContext.getConnectionConfig());
		//Sample code to give at least one table.
		consumer.accept(list(
				table("Target")
		));
	}

	/**
	 * The method invocation life circle is below,
	 * connectionTest
	 * onStart/onStop will not be invoked before/after connectionTest, please create/release connection within connectionTest method.
	 *
	 * consumer can call accept method multiple times for different test items
	 *
	 * @param connectionContext
	 * @return ConnectionOptions to specify extra capabilities and ddlEvents this connector support. And timezone/charset this database is using. And connectionString for display the connection information.
	 */
	@Override
	public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
		//Assume below tests are successfully, below tests are recommended, but not required.
		TapLogger.info(TAG, "connectionTest {}", connectionContext.getConnectionConfig());
		try {
			connect(connectionContext);
			consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, "Connect successfully"));
			TapLogger.info(TAG, "XDBConnector test successfully");
		} catch (Throwable throwable) {
			TapLogger.error(TAG, "Connect failed, ", getStackTrace(throwable));
			consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connect failed, " + throwable.getMessage()));
		}

		//Read test
		//TODO execute read test here
//		consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
		//Write test
		//TODO execute write test here
//		consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
		//Read log test to check CDC capability
		//TODO execute read log test here
//		consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));

		//When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
		//When test successfully, but some warn is reported.
//        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
		return ConnectionOptions.create();
	}

	/**
	 * Initialize "storageMap", used as KV database.
	 *
	 * "storageMap" is persistent to disk, will be removed after onStart invoked. The "storageMap" is to demonstrate how to write record into a KV database.
	 *
	 * @param connectionContext
	 */
	private void connect(TapConnectionContext connectionContext) {
		if(storageMap == null) {
			// json schema if from json object, "configOptions.connection" in file "resources/spec.json"
			// the keys in json schema is the keys to get the values which input by users
			DataMap connectionConfig = connectionContext.getConnectionConfig();
			String host = connectionConfig.getValue("host", "localhost");
			Integer port = connectionConfig.getValue("port", 3306);
			String database = connectionConfig.getValue("database", "");
			String username = connectionConfig.getValue("username", "root");
			String password = connectionConfig.getValue("password", "");
			// Create connection by above user inputs.
			// The storageMap cache 10 entries in memory, more records will be stored in disk, limited to 1024MB.
			storageMap = InstanceFactory.instance(KVMap.class, "persistent");
			// mapKey stand for a database specified in connectionConfig. Object.class means the map can accept any type of record.
			storageMap.init(host + ":" + port + "@" + database + "#" + username + "$" + password, Object.class);
		}
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		TapLogger.info(TAG, "tableCount");
		//Only one Table return from discoverSchema.
		return 1;
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
		TapLogger.info(TAG, "registerCapabilities");
		connectorFunctions.supportWriteRecord(this::writeRecord);
		connectorFunctions.supportDropTable(this::dropTable);

		//Below capabilities, developer can decide to implement or not.
//		connectorFunctions.supportBatchRead(this::batchRead);
//		connectorFunctions.supportStreamRead(this::streamRead);
//		connectorFunctions.supportBatchCount(this::batchCount);
//		connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
//		connectorFunctions.supportDropTable(this::dropTable);
//		connectorFunctions.supportCreateTableV2(this::createTable);
//		connectorFunctions.supportCreateIndex(this::createIndex);
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
	 * onStart -> dropTable -> onStop
	 *
	 * Drop table by TapDropTableEvent.
	 * TapDropTableEvent#tableId specified which table to drop
	 *
	 * @param connectorContext
	 * @param tapDropTableEvent
	 */
	private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent tapDropTableEvent) {
		//Should just drop the table specified by tapClearTableEvent.getTableId()
		//The reset will drop the whole KV database which is only for demo purpose.
		storageMap.reset();
		TapLogger.info(TAG, "dropTable {}", tapDropTableEvent.getTableId());
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
		if(storageMap == null)
			throw new NullPointerException("storageMap is not initialized");

		//Please record how many records are inserted, updated and deleted
		AtomicLong inserted = new AtomicLong(0); //insert count
		AtomicLong updated = new AtomicLong(0); //update count
		AtomicLong deleted = new AtomicLong(0); //delete count
		Map<String, Object> filter;
		for(TapRecordEvent recordEvent : tapRecordEvents) {
			filter = recordEvent.getFilter(table.primaryKeys()); //Get filter map for generating kv key to insert/update/delete
			String key = getKey(table.getId(), filter); //Combine the filter values into one string as a key.
			switch (recordEvent.getType()) {
				case TapInsertRecordEvent.TYPE:
					TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
					storageMap.put(key, insertRecordEvent.getAfter()); //Write insert record into KV Map
					TapLogger.info(TAG, "Write on key {} record {}", key, toJson(insertRecordEvent.getAfter()));
					inserted.incrementAndGet();
					break;
				case TapUpdateRecordEvent.TYPE:
					TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
					storageMap.put(key, updateRecordEvent.getAfter()); //Write update record into KV Map
					TapLogger.info(TAG, "Update on key {} record {}", key, toJson(updateRecordEvent.getAfter()));
					updated.incrementAndGet();
					break;
				case TapDeleteRecordEvent.TYPE:
					storageMap.remove(key); //Delete record
					TapLogger.info(TAG, "Delete on key {}");
					deleted.incrementAndGet();
					break;
			}
		}
		//Need to tell incremental engine about write result
		writeListResultConsumer.accept(writeListResult()
				.insertedCount(inserted.get())
				.modifiedCount(updated.get())
				.removedCount(deleted.get()));
	}

	/**
	 * Combine filter map into one string as KV key.
	 * Table id as the prefix.
	 *
	 * @param tableId
	 * @param filter
	 * @return
	 */
	private String getKey(String tableId, Map<String, Object> filter) {
		Collection<Object> values = filter.values();
		StringBuilder builder = new StringBuilder(tableId);
		for (Object value : values) {
			builder.append("_").append(value);
		}
		return builder.toString();
	}

	/**
	 * Initialize database connection here.
	 *
	 * @param connectionContext when do one time job, like connectionTest, discoverSchema, etc, TapConnectionContext will be used, otherwise when task actually started, TapConnectorContext will be used.
	 * @throws Throwable
	 */
	@Override
	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		connect(connectionContext);
		TapLogger.info(TAG, "onStart {}", toJson(connectionContext.getConnectionConfig()));
	}

	/**
	 * Release database connection here.
	 *
	 * @param connectionContext
	 * @throws Throwable
	 */
	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		storageMap.reset();
		TapLogger.info(TAG, "onStop {}", toJson(connectionContext.getConnectionConfig()));
	}
}
