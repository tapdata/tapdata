package io.tapdata.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ParagraphFormatter;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbStreamReader;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.mongodb.reader.v3.MongodbV3StreamReader;
import io.tapdata.mongodb.writer.MongodbWriter;
import io.tapdata.partition.DatabaseReadPartitionSplitter;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.exception.NotSupportedException;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.io.Closeable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static java.util.Collections.singletonList;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class MongodbConnector extends ConnectorBase {

	private static final int SAMPLE_SIZE_BATCH_SIZE = 100;
	private static final String COLLECTION_ID_FIELD = "_id";
	public static final String TAG = MongodbConnector.class.getSimpleName();
	private final AtomicLong counter = new AtomicLong();
	private final AtomicBoolean isShutDown = new AtomicBoolean(false);
	protected MongodbConfig mongoConfig;
	protected MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	private final int[] lock = new int[0];
	MongoCollection<Document> mongoCollection;
	private MongoBatchOffset batchOffset = null;

	private MongodbStreamReader mongodbStreamReader;

	private volatile MongodbWriter mongodbWriter;
	private Map<String, Integer> stringTypeValueMap;

	private final MongodbExecuteCommandFunction mongodbExecuteCommandFunction = new MongodbExecuteCommandFunction();

	private Bson queryCondition(String firstPrimaryKey, Object value) {
		return gte(firstPrimaryKey, value);
	}

	private MongoCollection<Document> getMongoCollection(String table) {
		return mongoDatabase.getCollection(table);
	}

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
		final String version = MongodbUtil.getVersionString(mongoClient, mongoConfig.getDatabase());
		MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
		TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
		this.stringTypeValueMap = new HashMap<>();

		ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 30, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(30));

		try (Closeable ignored = executor::shutdown) {
			List<String> collectionNameList = StreamSupport.stream(collectionNames.spliterator(), false).collect(Collectors.toList());

			if (tables != null && !tables.isEmpty()) {
				collectionNameList = ListUtils.retainAll(collectionNameList, tables);
			}
			ListUtils.partition(collectionNameList, tableSize).forEach(nameList -> {
				CountDownLatch countDownLatch = new CountDownLatch(nameList.size());

				if (version.compareTo("3.2") >= 0) {
					Map<String, MongoCollection<Document>> documentMap = Collections.synchronizedMap(new HashMap<>());

					nameList.forEach(name -> executor.execute(() -> {
						documentMap.put(name, mongoDatabase.getCollection(name));
						countDownLatch.countDown();
					}));

					try {
						countDownLatch.await();
					} catch (InterruptedException e) {
						TapLogger.error(TAG, "MongodbConnector discoverSchema countDownLatch await", e);
					}

					//List all the tables under the database.
					List<TapTable> list = list();
					nameList.forEach(name -> {
						TapTable table = table(name).defaultPrimaryKeys("_id");
						try {
							MongodbUtil.sampleDataRow(documentMap.get(name), SAMPLE_SIZE_BATCH_SIZE, (dataRow) -> {
								Set<String> fieldNames = dataRow.keySet();
								for (String fieldName : fieldNames) {
									BsonValue value = dataRow.get(fieldName);
									getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, value, fieldName, table);
								}
							});
						} catch (Exception e) {
							TapLogger.error(TAG, "Use $sample load mongo connection {}'s {} schema failed {}, will use first row as data schema.",
									MongodbUtil.maskUriPassword(mongoConfig.getUri()), name, e.getMessage(), e);
						}

						if (!Objects.isNull(table.getNameFieldMap()) && !table.getNameFieldMap().isEmpty()) {
							list.add(table);
						}
					});

					consumer.accept(list);
				} else {
					Map<String, MongoCollection<BsonDocument>> documentMap = Collections.synchronizedMap(new HashMap<>());

					nameList.forEach(name -> executor.execute(() -> {
						documentMap.put(name, mongoDatabase.getCollection(name, BsonDocument.class));
						countDownLatch.countDown();
					}));

					try {
						countDownLatch.await();
					} catch (InterruptedException e) {
						TapLogger.error(TAG, "MongodbConnector discoverSchema countDownLatch await", e);
					}

					//List all the tables under the database.
					List<TapTable> list = list();
					nameList.forEach(name -> {
						TapTable table = table(name).defaultPrimaryKeys(singletonList(COLLECTION_ID_FIELD));
						try (MongoCursor<BsonDocument> cursor = documentMap.get(name).find().iterator()) {
							while (cursor.hasNext()) {
								final BsonDocument document = cursor.next();
								for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
									final String fieldName = entry.getKey();
									final BsonValue value = entry.getValue();
									getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, value, fieldName, table);
								}
								break;
							}
						}
						if (!Objects.isNull(table.getNameFieldMap()) && !table.getNameFieldMap().isEmpty()) {
							list.add(table);
						}
					});

					consumer.accept(list);
				}
			});
		}
	}

	public void getRelateDatabaseField(TapConnectionContext connectionContext, TableFieldTypesGenerator tableFieldTypesGenerator, BsonValue value, String fieldName, TapTable table) {
		if (value instanceof BsonDocument) {
			BsonDocument bsonDocument = (BsonDocument) value;
			for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
				getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, entry.getValue(), fieldName + "." + entry.getKey(), table);
			}
		} else if (value instanceof BsonArray) {
			BsonArray bsonArray = (BsonArray) value;
			BsonDocument bsonDocument = new BsonDocument();
			for (BsonValue bsonValue : bsonArray) {
				if (bsonValue instanceof BsonDocument) {
					BsonDocument theDoc = (BsonDocument) bsonValue;
					for (Map.Entry<String, BsonValue> entry : theDoc.entrySet()) {
						BsonValue bsonValue1 = bsonDocument.get(entry.getKey());
						if ((bsonValue1 == null || bsonValue1.isNull()) || !entry.getValue().isNull()) {
							bsonDocument.put(entry.getKey(), entry.getValue());
						}
					}
				}
			}
			if (MapUtils.isNotEmpty(bsonDocument)) {
				for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
					getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, entry.getValue(), fieldName + "." + entry.getKey(), table);
				}
			}
		}
		TapField field;
		if (value != null && !value.isNull()) {
			BsonType bsonType = value.getBsonType();
			if (BsonType.STRING.equals(bsonType)) {
				if (!(value instanceof BsonString)) {
					field = TapSimplify.field(fieldName, bsonType.name());
				} else {
					String valueString = ((BsonString) value).getValue();
					int currentLength = valueString.getBytes().length;
					Integer lastLength = stringTypeValueMap.get(fieldName);
					if (currentLength > 0 && (null == lastLength || currentLength > lastLength)) {
						stringTypeValueMap.put(fieldName, currentLength);
					}
					if (null != stringTypeValueMap.get(fieldName)) {
						int length = stringTypeValueMap.get(fieldName);
						length = length * 5;
						if (length < 100)
							length = 100;
						field = TapSimplify.field(fieldName, bsonType.name() + String.format("(%s)", length));
					} else {
						field = TapSimplify.field(fieldName, bsonType.name());
					}
				}
			} else {
				field = TapSimplify.field(fieldName, bsonType.name());
			}
		} else {
			field = TapSimplify.field(fieldName, BsonType.NULL.name());
		}

		if (COLLECTION_ID_FIELD.equals(fieldName)) {
			field.primaryKeyPos(1);
		}
		TapField currentFiled = null;
		if (table.getNameFieldMap() != null)
			currentFiled = table.getNameFieldMap().get(fieldName);
		if (currentFiled != null &&
				currentFiled.getDataType() != null &&
				!currentFiled.getDataType().equals(BsonType.NULL.name()) &&
				field.getDataType() != null && field.getDataType().equals(BsonType.NULL.name())
		) {
			return;
		}
		tableFieldTypesGenerator.autoFill(field, connectionContext.getSpecification().getDataTypesMap());
		table.add(field);
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
		ConnectionOptions connectionOptions = ConnectionOptions.create();
		try {
			onStart(connectionContext);
			try (
					MongodbTest mongodbTest = new MongodbTest(mongoConfig, consumer, mongoClient)
			) {
				mongodbTest.testOneByOne();
			}
		} catch (Throwable throwable) {
			TapLogger.error(TAG, throwable.getMessage());
			consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
		} finally {
			onStop(connectionContext);
		}
		return connectionOptions;
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		int index = 0;
		try {
			MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
			index = 0;
			for (String collectionName : collectionNames) {
				index++;
			}
		} catch (Exception e) {
			throw e;
		}
		return index;
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
		connectorFunctions.supportMemoryFetcher(this::memoryFetcher);
		connectorFunctions.supportWriteRecord(this::writeRecord);
		connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
		connectorFunctions.supportDropTable(this::dropTable);
		connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

		//Handle the special bson types, convert them to TapValue. Otherwise the unrecognized types will be converted to TapRawValue by default.
		//Target side will not easy to handle the TapRawValue.
		codecRegistry.registerToTapValue(ObjectId.class, (value, tapType) -> {
			ObjectId objValue = (ObjectId) value;
			return new TapStringValue(objValue.toHexString());
		});
		codecRegistry.registerToTapValue(Binary.class, (value, tapType) -> {
			Binary binary = (Binary) value;
			return new TapBinaryValue(binary.getData());
		});

		codecRegistry.registerToTapValue(Code.class, (value, tapType) -> {
			Code code = (Code) value;
			return new TapStringValue(code.getCode());
		});
		codecRegistry.registerToTapValue(Decimal128.class, (value, tapType) -> {
			Decimal128 decimal128 = (Decimal128) value;
			return new TapNumberValue(decimal128.doubleValue());
		});

		codecRegistry.registerToTapValue(Document.class, (value, tapType) -> {
			Document document = (Document) value;
			for (Map.Entry<String, Object> entry : document.entrySet()) {
				if (entry.getValue() instanceof Double && entry.getValue().toString().contains("E")) {
					entry.setValue(new BigDecimal(entry.getValue().toString()).toString());
				}
			}
			return new TapMapValue(document);
		});
		codecRegistry.registerToTapValue(Symbol.class, (value, tapType) -> {
			Symbol symbol = (Symbol) value;
			return new TapStringValue(symbol.getSymbol());
		});

		//TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
		codecRegistry.registerFromTapValue(TapTimeValue.class, "DATE_TIME", tapTimeValue -> tapTimeValue.getValue().toDate());
		codecRegistry.registerFromTapValue(TapDateTimeValue.class, "DATE_TIME", tapDateTimeValue -> tapDateTimeValue.getValue().toDate());
		codecRegistry.registerFromTapValue(TapDateValue.class, "DATE_TIME", tapDateValue -> tapDateValue.getValue().toDate());
		codecRegistry.registerFromTapValue(TapYearValue.class, "STRING(4)", tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"));

		//Handle ObjectId when the source is also mongodb, we convert ObjectId to String before enter incremental engine.
		//We need check the TapStringValue, when will write to mongodb, if the originValue is ObjectId, then use originValue instead of the converted String value.
		codecRegistry.registerFromTapValue(TapStringValue.class, tapValue -> {
			Object originValue = tapValue.getOriginValue();
			String value = tapValue.getValue();
			if (originValue instanceof ObjectId) {
				return originValue;
			} else if (originValue instanceof byte[]) {
				byte[] bytes = (byte[]) originValue;
				if (bytes.length == 26 && bytes[0] == 99 && bytes[bytes.length - 1] == 23
						&& null != value && value.length() == 24) {
					return new ObjectId(tapValue.getValue());
				}
			}
			//If not ObjectId, use default TapValue Codec to convert.
			return codecRegistry.getValueFromDefaultTapValueCodec(tapValue);
		});

		//TO be as a source, need to implement below methods.
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportCreateIndex(this::createIndex);
//		connectorFunctions.supportCreateTableV2(this::createTableV2);
		connectorFunctions.supportStreamRead(this::streamRead);
		connectorFunctions.supportTimestampToStreamOffset(this::streamOffset);
		connectorFunctions.supportErrorHandleFunction(this::errorHandle);

		connectorFunctions.supportGetReadPartitionsFunction(this::getReadPartitions);
		connectorFunctions.supportCountByPartitionFilterFunction(this::countByPartitionFilter);
		connectorFunctions.supportQueryFieldMinMaxValueFunction(this::queryFieldMinMaxValue);
//        connectorFunctions.supportStreamOffset((connectorContext, tableList, offsetStartTime, offsetOffsetTimeConsumer) -> streamOffset(connectorContext, tableList, offsetStartTime, offsetOffsetTimeConsumer));
		connectorFunctions.supportExecuteCommandFunction(this::executeCommand);
		connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
	}

	private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
		// TODO: 为 分片集群建表, schema 约束建表预留位置
		CreateTableOptions createTableOptions = new CreateTableOptions();
		createTableOptions.setTableExists(false);
		return createTableOptions;
	}


	private void executeCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> executeResultConsumer) {
		try {
			Map<String, Object> executeObj = tapExecuteCommand.getParams();
			String command = tapExecuteCommand.getCommand();
			if (MapUtils.isNotEmpty(executeObj) && StringUtils.isEmpty((CharSequence) executeObj.get("database"))) {
				executeObj.put("database", mongoConfig.getDatabase());
			}
			if ("execute".equals(command)) {
				executeResultConsumer.accept(new ExecuteResult<Long>().result(mongodbExecuteCommandFunction.execute(executeObj, mongoClient)));
			} else if ("executeQuery".equals(command)) {
				mongodbExecuteCommandFunction.executeQuery(executeObj, mongoClient, list -> executeResultConsumer.accept(new ExecuteResult<List<Map<String, Object>>>().result(list)), this::isAlive);
			} else if ("count".equals(command)) {
				executeResultConsumer.accept(new ExecuteResult<Long>().result(mongodbExecuteCommandFunction.count(executeObj, mongoClient)));
			} else if ("aggregate".equals(command)) {
				mongodbExecuteCommandFunction.aggregate(executeObj, mongoClient, list -> executeResultConsumer.accept(new ExecuteResult<List<Map<String, Object>>>().result(list)), this::isAlive);
			} else {
				throw new NotSupportedException(command);
			}
		} catch (Exception e) {
			executeResultConsumer.accept(new ExecuteResult<>().error(e));
		}
	}

	private FieldMinMaxValue queryFieldMinMaxValue(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter partitionFilter, String fieldName) {
		MongoCollection<Document> collection = getMongoCollection(table.getId());
		TapIndexEx partitionIndex = table.partitionIndex();
		if (partitionIndex == null)
			throw new CoreException(MongoErrors.NO_INDEX_FOR_PARTITION, "No index to do partition");

		Bson query = queryForPartitionFilter(partitionFilter, partitionIndex);

		List<TapIndexField> indexFields = partitionIndex.getIndexFields();
		Document sort = new Document();
		Boolean fieldAsc = null;
		for (TapIndexField indexField : indexFields) {
			Boolean asc = indexField.getFieldAsc();
			if (asc == null)
				asc = true;
			if (indexField.getName().equals(fieldName)) {
				fieldAsc = asc;
			}
			sort.put(indexField.getName(), asc ? 1 : -1);
		}
		if (fieldAsc == null)
			throw new CoreException(MongoErrors.FIELD_NOT_IN_PARTITION_INDEXES, "field {} not found in partition indexes {}", fieldName, partitionIndex.getIndexMap().keySet());
		FieldMinMaxValue fieldMinMaxValue = FieldMinMaxValue.create().fieldName(fieldName);

		Document minSort;
		Document maxSort;
		if (fieldAsc) {
			minSort = sort;
			maxSort = reverseSort(minSort);
		} else {
			maxSort = sort;
			minSort = reverseSort(maxSort);
		}
		//Get min value
		FindIterable<Document> minIterable = collection.find(query).sort(minSort).projection(new Document().append(fieldName, 1)).limit(1);
		Document minDoc = minIterable.first();
		if (minDoc == null) {
//			throw new CoreException(MongoErrors.NO_RECORD_WHILE_GET_MIN, "No record while get min for field {}, query {}, sort {}", fieldName, query, sort);
			TapLogger.info(TAG, "No record while get min for field {}, query {}, sort {}", fieldName, query, sort);
			return null;
		}
		Object minValue = minDoc.get(fieldName);
//		if(minValue == null) {
////			throw new CoreException(MongoErrors.MIN_VALUE_IS_NULL, "Min value is null for field {}, query {}, sort {}", fieldName, query, sort);
//			TapLogger.info(TAG, "Min value is null for field {}, query {}, sort {}", fieldName, query, sort);
//			return null;
//		}
		fieldMinMaxValue.min(minValue).detectType(minValue);

		//Get max value
		FindIterable<Document> maxIterable = collection.find(query).sort(maxSort).projection(new Document().append(fieldName, 1)).limit(1);
		Document maxDoc = maxIterable.first();
		if (maxDoc == null) {
//			throw new CoreException(MongoErrors.NO_RECORD_WHILE_GET_MAX, "No record while get max for field {}, query {}, sort {}", fieldName, query, sort);
			TapLogger.info(TAG, "No record while get max for field {}, query {}, sort {}", fieldName, query, sort);
			return null;
		}
		Object maxValue = maxDoc.get(fieldName);
//		if(maxValue == null) {
////			throw new CoreException(MongoErrors.MAX_VALUE_IS_NULL, "Max value is null for field {}, query {}, sort {}", fieldName, query, sort);
//			TapLogger.info(TAG, "Max value is null for field {}, query {}, sort {}", fieldName, query, sort);
//			return null;
//		}
		fieldMinMaxValue.max(maxValue);

		return fieldMinMaxValue;
	}

	private Document reverseSort(Document sort) {
		Document newSort = new Document();
		for (Map.Entry<String, Object> entry : sort.entrySet()) {
			int value = (int) entry.getValue();
			newSort.put(entry.getKey(), -value);
		}
		return newSort;
	}

	private long countByPartitionFilter(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter partitionFilter) {
		Bson query = queryForPartitionFilter(partitionFilter, table.partitionIndex());
		return getCollectionNotAggregateCountByTableName(mongoClient, mongoConfig.getDatabase(), table.getId(), query);
	}

	private Bson queryForPartitionFilter(TapAdvanceFilter partitionFilter, TapIndexEx partitionKeys) {
		List<Bson> bsonList = new ArrayList<>();
		List<QueryOperator> ops = partitionFilter.getOperators();
		if (ops != null)
			for (QueryOperator op : ops) {
				if (op == null)
					continue;
				if (!partitionKeys.getIndexMap().containsKey(op.getKey())) {
					throw new CoreException(MongoErrors.KEY_OUTSIDE_OF_PARTITION_KEYS, "Key {} is not in partition keys {} in operators", op.getKey(), partitionKeys);
				}
				switch (op.getOperator()) {
					case QueryOperator.GT:
						bsonList.add(gt(op.getKey(), op.getValue()));
						break;
					case QueryOperator.GTE:
						bsonList.add(gte(op.getKey(), op.getValue()));
						break;
					case QueryOperator.LT:
						bsonList.add(lt(op.getKey(), op.getValue()));
						break;
					case QueryOperator.LTE:
						bsonList.add(lte(op.getKey(), op.getValue()));
						break;
				}
			}
		DataMap match = partitionFilter.getMatch();
		if (match != null) {
			for (Map.Entry<String, Object> entry : match.entrySet()) {
				if (!partitionKeys.getIndexMap().containsKey(entry.getKey())) {
					throw new CoreException(MongoErrors.KEY_OUTSIDE_OF_PARTITION_KEYS, "Key {} is not in partition keys {} in match", entry.getKey(), partitionKeys);
				}
				bsonList.add(eq(entry.getKey(), entry.getValue()));
			}
		}
		Bson query;
		if (bsonList.isEmpty())
			query = new Document();
		else
			query = and(bsonList.toArray(new Bson[0]));
		return query;
	}

	private void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) {
		options.getTypeSplitterMap().registerCustomSplitter(ObjectId.class, new ObjectIdSplitter());

		DatabaseReadPartitionSplitter.calculateDatabaseReadPartitions(connectorContext, table, options)
				.countByPartitionFilter(this::countByPartitionFilter)
				.queryFieldMinMaxValue(this::queryFieldMinMaxValue)
				.startSplitting();
	}

	protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
		RetryOptions retryOptions = RetryOptions.create();
		if (null != matchThrowable(throwable, MongoClientException.class)
				|| null != matchThrowable(throwable, MongoSocketException.class)
				|| null != matchThrowable(throwable, MongoConnectionPoolClearedException.class)
				|| null != matchThrowable(throwable, MongoSecurityException.class)
				|| null != matchThrowable(throwable, MongoConfigurationException.class)
				|| null != matchThrowable(throwable, MongoTimeoutException.class)
				|| null != matchThrowable(throwable, MongoSocketReadException.class)
				|| null != matchThrowable(throwable, MongoSocketClosedException.class)
				|| null != matchThrowable(throwable, MongoSocketOpenException.class)
				|| null != matchThrowable(throwable, MongoSocketWriteException.class)
				|| null != matchThrowable(throwable, MongoSocketReadTimeoutException.class)
				|| null != matchThrowable(throwable, MongoWriteConcernException.class)
				|| null != matchThrowable(throwable, MongoWriteException.class)
				|| null != matchThrowable(throwable, MongoNodeIsRecoveringException.class)
				|| null != matchThrowable(throwable, MongoNotPrimaryException.class)
				|| null != matchThrowable(throwable, MongoServerUnavailableException.class)
				|| null != matchThrowable(throwable, MongoQueryException.class)
				|| null != matchThrowable(throwable, MongoCommandException.class)
				|| null != matchThrowable(throwable, MongoInterruptedException.class)) {
			retryOptions.needRetry(true);
			return retryOptions;
		}
		if (null != matchThrowable(throwable, MongoCommandException.class)) {
			MongoCommandException mongoCommandException = (MongoCommandException) throwable;
			Pattern pattern = Pattern.compile("Cache Reader No keys found for .* that is valid for time.*");
			if (mongoCommandException.getErrorCode() == 211 && pattern.matcher(mongoCommandException.getErrorMessage()).matches()) {
				retryOptions.needRetry(true);
				return retryOptions;
			}
		}
		return retryOptions;
	}

	private void createIndex(TapConnectorContext tapConnectorContext, TapTable table, TapCreateIndexEvent tapCreateIndexEvent) {
		final List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
		if (CollectionUtils.isNotEmpty(indexList)) {
			for (TapIndex tapIndex : indexList) {
				final List<TapIndexField> indexFields = tapIndex.getIndexFields();
				if (CollectionUtils.isNotEmpty(indexFields)) {
					final MongoCollection<Document> collection = mongoDatabase.getCollection(table.getName());
					Document keys = new Document();
					for (TapIndexField indexField : indexFields) {
						keys.append(indexField.getName(), 1);
					}
					final IndexOptions indexOptions = new IndexOptions();
					if (indexFields.size() != 1 || !"_id".equals(indexFields.stream().findFirst().get().getName())) {
						indexOptions.unique(tapIndex.isUnique());
					}
					if (EmptyKit.isNotEmpty(tapIndex.getName())) {
						indexOptions.name(tapIndex.getName());
					}
					collection.createIndex(keys, indexOptions);
				}
			}
		}
	}

	private String memoryFetcher(List<String> mapKeys, String level) {
		ParagraphFormatter paragraphFormatter = new ParagraphFormatter(MongodbConnector.class.getSimpleName());
		paragraphFormatter.addRow("MongoConfig", mongoConfig != null ? mongoConfig.getDatabase() : null);
		return paragraphFormatter.toString();
	}

	public void onStart(TapConnectionContext connectionContext) throws Throwable {
		final DataMap connectionConfig = connectionContext.getConnectionConfig();
		if (MapUtils.isEmpty(connectionConfig)) {
			throw new RuntimeException("connection config cannot be empty");
		}
		mongoConfig = (MongodbConfig) new MongodbConfig().load(connectionConfig);
		mongoConfig.load(connectionContext.getNodeConfig());
		if (mongoConfig == null) {
			throw new RuntimeException("load mongo config failed from connection config");
		}
		if (mongoClient == null) {
			try {
				mongoClient = MongodbUtil.createMongoClient(mongoConfig);
				mongoDatabase = mongoClient.getDatabase(mongoConfig.getDatabase());
			} catch (Throwable e) {
				throw new RuntimeException(String.format("create mongodb connection failed %s", e.getMessage()), e);
			}
		}
	}

	private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
		getMongoCollection(dropTableEvent.getTableId()).drop();

	}

//    Object streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
//        //If don't support return stream offset by offsetStartTime, please throw NotSupportedException to let Flow engine knows, otherwise the result will be unpredictable.
////        if(offsetStartTime != null)
////            throw new NotSupportedException();
//        //TODO return stream offset
//        return null;
//    }


	/**
	 * The method invocation life circle is below,
	 * initiated ->
	 * if(needCreateTable)
	 * createTable
	 * if(needClearTable)
	 * clearTable
	 * if(needDropTable)
	 * dropTable
	 * writeRecord
	 * -> destroy -> ended
	 *
	 * @param connectorContext
	 * @param tapRecordEvents
	 * @param writeListResultConsumer
	 */
	private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
		if (mongodbWriter == null) {
			synchronized (this) {
				if (mongodbWriter == null) {
					mongodbWriter = new MongodbWriter(connectorContext.getGlobalStateMap(), mongoConfig, mongoClient);
					ConnectorCapabilities connectorCapabilities = connectorContext.getConnectorCapabilities();
					if (null != connectorCapabilities) {
						mongoConfig.setInsertDmlPolicy(null == connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY) ?
								ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS : connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
						mongoConfig.setUpdateDmlPolicy(null == connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY) ?
								ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS : connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY));
					} else {
						mongoConfig.setInsertDmlPolicy(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
						mongoConfig.setUpdateDmlPolicy(ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
					}
				}
			}
		}

		mongodbWriter.writeRecord(tapRecordEvents, table, writeListResultConsumer);
	}

	private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable table, Consumer<FilterResults> consumer) {
		MongoCollection<Document> collection = getMongoCollection(table.getId());
		List<Bson> bsonList = new ArrayList<>();
		DataMap match = tapAdvanceFilter.getMatch();
		Map<String, TapField> map = table.getNameFieldMap();
		if (match != null) {
			for (Map.Entry<String, Object> entry : match.entrySet()) {
				TapField tapField = map.get(entry.getKey());
				entry.setValue(formatValue(tapField, entry.getKey(), entry.getValue()));
				bsonList.add(eq(entry.getKey(), entry.getValue()));
			}
		}
		List<QueryOperator> ops = tapAdvanceFilter.getOperators();

		if (ops != null) {
			for (QueryOperator op : ops) {
				TapField tapField = map.get(op.getKey());
				op.setValue(formatValue(tapField, op.getKey(), op.getValue()));
				switch (op.getOperator()) {
					case QueryOperator.GT:
						bsonList.add(gt(op.getKey(), op.getValue()));
						break;
					case QueryOperator.GTE:
						bsonList.add(gte(op.getKey(), op.getValue()));
						break;
					case QueryOperator.LT:
						bsonList.add(lt(op.getKey(), op.getValue()));
						break;
					case QueryOperator.LTE:
						bsonList.add(lte(op.getKey(), op.getValue()));
						break;
				}
			}
		}

		Bson query;
		if (bsonList.isEmpty())
			query = new Document();
		else
			query = and(bsonList.toArray(new Bson[0]));

		Projection projection = tapAdvanceFilter.getProjection();
		Document projectionDoc = null;
		if (projection != null) {
			if (projection.getIncludeFields() != null && !projection.getIncludeFields().isEmpty()) {
				if (projectionDoc == null)
					projectionDoc = new Document();
				for (String includeField : projection.getIncludeFields()) {
					projectionDoc.put(includeField, 1);
				}
			}
			if (projection.getExcludeFields() != null && !projection.getExcludeFields().isEmpty()) {
				if (projectionDoc == null)
					projectionDoc = new Document();
				for (String excludeField : projection.getExcludeFields()) {
					projectionDoc.put(excludeField, -1);
				}
			}
		}

		FindIterable<Document> iterable = collection.find(query).projection(projectionDoc);

		Integer limit = tapAdvanceFilter.getLimit();
		if (limit != null) {
			iterable.limit(limit);
		}
		Integer skip = tapAdvanceFilter.getSkip();
		if (skip != null) {
			iterable.skip(skip);
		}

		List<SortOn> sortOnList = tapAdvanceFilter.getSortOnList();
		if (CollectionUtils.isNotEmpty(sortOnList)) {
			List<String> ascKeys = sortOnList.stream()
					.filter(s -> s.getSort() == SortOn.ASCENDING)
					.map(SortOn::getKey)
					.collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(ascKeys)) {
				iterable.sort(Sorts.ascending(ascKeys));
			}
			List<String> descKeys = sortOnList.stream()
					.filter(s -> s.getSort() == SortOn.DESCENDING)
					.map(SortOn::getKey)
					.collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(descKeys)) {
				iterable.sort(Sorts.descending(descKeys));
			}
		}
		FilterResults filterResults = new FilterResults();
		Integer batchSize = tapAdvanceFilter.getBatchSize();
		if (batchSize == null) {
			batchSize = 1000;
		}
		try (final MongoCursor<Document> mongoCursor = iterable.iterator()) {
			while (mongoCursor.hasNext()) {
				filterResults.add(mongoCursor.next());
				if (filterResults.resultSize() >= batchSize) {
					consumer.accept(filterResults);
					filterResults = new FilterResults();
				}
			}
		}
		if (filterResults.resultSize() > 0)
			consumer.accept(filterResults);
	}

	private Object formatValue(TapField tapField, String key, Object value) {
		if (tapField.getTapType() instanceof TapNumber && value instanceof String) {
			if (value.toString().contains(".")) {
				value = Double.valueOf(value.toString());
			} else {
				value = Long.valueOf(value.toString());
			}
		}
		if (value instanceof DateTime) {
			value = ((DateTime) value).toInstant();
		}
		return value;
	}

	/**
	 * The method invocation life circle is below,
	 * initiated ->
	 * if(batchEnabled)
	 * batchCount -> batchRead
	 * if(streamEnabled)
	 * streamRead
	 * -> destroy -> ended
	 * <p>
	 * In connectorContext,
	 * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
	 * current instance is serving for the table from connectorContext.
	 *
	 * @param connectorContext
	 * @return
	 */
	private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
//        MongoCollection<Document> collection = getMongoCollection(table.getId());
//        return collection.countDocuments();
		return getCollectionNotAggregateCountByTableName(mongoClient, mongoConfig.getDatabase(), table.getId(), null);
	}

	public static long getCollectionNotAggregateCountByTableName(MongoClient mongoClient, String db, String collectionName, Bson filter) {
		long dbCount = 0L;
		MongoDatabase database = mongoClient.getDatabase(db);
		Document countDocument = database.runCommand(
				new Document("count", collectionName)
						.append("query", filter == null ? new Document() : filter)
		);

		if (countDocument.containsKey("ok") && countDocument.containsKey("n")) {
			if (countDocument.get("ok").equals(1d)) {
//				dbCount = Long.valueOf(countDocument.get("n") + "");
				Object countObj = countDocument.get("n");
				String countStr = countObj + "";
				try {
					dbCount = Long.parseLong(countStr);
				} catch (NumberFormatException e) {
					TapLogger.warn("Count result parsing failure of the collection '{}.{}' and type is {}: {}", db, collectionName, countObj.getClass(), e.getMessage(), e);
					dbCount = (long) Double.parseDouble(countStr);
				}
			}
		}

		return dbCount;
	}

	/**
	 * The method invocation life circle is below,
	 * initiated ->
	 * if(batchEnabled)
	 * batchCount -> batchRead
	 * if(streamEnabled)
	 * streamRead
	 * -> destroy -> ended
	 * <p>
	 * In connectorContext,
	 * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
	 * current instance is serving for the table from connectorContext.
	 *
	 * @param connectorContext
	 * @param offset
	 * @param tapReadOffsetConsumer
	 */
	private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offset, int eventBatchSize, BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer) throws Throwable {
		List<TapEvent> tapEvents = list();
		MongoCursor<Document> mongoCursor;
		MongoCollection<Document> collection = getMongoCollection(table.getId());
		final int batchSize = eventBatchSize > 0 ? eventBatchSize : 5000;
		if (offset == null) {
			mongoCursor = collection.find().sort(Sorts.ascending(COLLECTION_ID_FIELD)).batchSize(batchSize).iterator();
		} else {
			MongoBatchOffset mongoOffset = (MongoBatchOffset) offset;//fromJson(offset, MongoOffset.class);
			Object offsetValue = mongoOffset.value();
			if (offsetValue != null) {
				mongoCursor = collection.find(queryCondition(COLLECTION_ID_FIELD, offsetValue)).sort(Sorts.ascending(COLLECTION_ID_FIELD))
						.batchSize(batchSize).iterator();
			} else {
				mongoCursor = collection.find().sort(Sorts.ascending(COLLECTION_ID_FIELD)).batchSize(batchSize).iterator();
				TapLogger.warn(TAG, "Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
			}
		}

		Document lastDocument;

		try {
			while (mongoCursor.hasNext()) {
				if (!isAlive()) return;
				lastDocument = mongoCursor.next();
				tapEvents.add(insertRecordEvent(lastDocument, table.getId()));

				if (tapEvents.size() == eventBatchSize) {
					Object value = lastDocument.get(COLLECTION_ID_FIELD);
					batchOffset = new MongoBatchOffset(COLLECTION_ID_FIELD, value);
					tapReadOffsetConsumer.accept(tapEvents, batchOffset);
					tapEvents = list();
				}
			}
			if (!tapEvents.isEmpty()) {
				tapReadOffsetConsumer.accept(tapEvents, null);
			}
		} catch (Exception e) {
			if (!isAlive() && e instanceof MongoInterruptedException) {
				// ignored
			} else {
				throw e;
			}
		}
	}

	private Object streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
		if (mongodbStreamReader == null) {
			mongodbStreamReader = createStreamReader();
		}
		return mongodbStreamReader.streamOffset(offsetStartTime);
	}

	/**
	 * The method invocation life circle is below,
	 * initiated ->
	 * if(batchEnabled)
	 * batchCount -> batchRead
	 * if(streamEnabled)
	 * streamRead
	 * -> destroy -> ended
	 * <p>
	 * In connectorContext,
	 * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
	 * current instance is serving for the table from connectorContext.
	 *
	 * @param connectorContext //     * @param offset
	 *                         //     * @param consumer
	 */
	private void streamRead(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
		if (mongodbStreamReader == null) {
			mongodbStreamReader = createStreamReader();
		}
		try {
			mongodbStreamReader.read(connectorContext, tableList, offset, eventBatchSize, consumer);
		} catch (Exception e) {
			try {
				mongodbStreamReader.onDestroy();
			} catch (Exception ignored) {
			}
			mongodbStreamReader = null;
			throw new RuntimeException(e);
		}

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
//    }
	private MongodbStreamReader createStreamReader() {
		final int version = MongodbUtil.getVersion(mongoClient, mongoConfig.getDatabase());
		MongodbStreamReader mongodbStreamReader;
		if (version >= 4) {
			mongodbStreamReader = new MongodbV4StreamReader();
		} else {
			mongodbStreamReader = new MongodbV3StreamReader();
		}
		mongodbStreamReader.onStart(mongoConfig);
		return mongodbStreamReader;
	}

	private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) throws Throwable {
		String database = mongoConfig.getDatabase();
		List<String> temp = new ArrayList<>();
		for (String tableName : mongoClient.getDatabase(database).listCollectionNames()) {
			if (getMongoCollection(tableName).estimatedDocumentCount() <= 0) {
				continue;
			}
			temp.add(tableName);
			if (temp.size() >= batchSize) {
				listConsumer.accept(temp);
				temp.clear();
			}
		}
		if (!temp.isEmpty()) {
			listConsumer.accept(temp);
			temp.clear();
		}
	}

	@Override
	public void onStop(TapConnectionContext connectionContext) throws Throwable {
		isShutDown.set(true);
		if (mongodbStreamReader != null) {
			mongodbStreamReader.onDestroy();
			mongodbStreamReader = null;
		}

		if (mongoClient != null) {
			mongoClient.close();
			mongoClient = null;
		}

		if (mongodbWriter != null) {
			mongodbWriter.onDestroy();
			mongodbWriter = null;
		}
	}

	private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) throws Throwable {
		String database = mongoConfig.getDatabase();
		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
		Document collStats = mongoDatabase.runCommand(new Document("collStats", tableName));
		TableInfo tableInfo = TableInfo.create();
		tableInfo.setNumOfRows(Long.valueOf(collStats.getInteger("count")));
		tableInfo.setStorageSize(Long.valueOf(collStats.getInteger("size")));
		return tableInfo;
	}
}
