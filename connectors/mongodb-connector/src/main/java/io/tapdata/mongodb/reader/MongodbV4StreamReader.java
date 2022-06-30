package io.tapdata.mongodb.reader;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.base.ConnectorBase.*;
import static java.util.Collections.singletonList;

/**
 * @author jackin
 * @date 2022/5/17 14:34
 **/
public class MongodbV4StreamReader implements MongodbStreamReader {

		public static final String TAG = MongodbV4StreamReader.class.getSimpleName();

		private BsonDocument resumeToken = null;
		private final AtomicBoolean running = new AtomicBoolean(false);
		private MongoClient mongoClient;
		private MongoDatabase mongoDatabase;
		private MongodbConfig mongodbConfig;
		private KVMap<Object> globalStateMap;

		@Override
		public void onStart(MongodbConfig mongodbConfig) {
				this.mongodbConfig = mongodbConfig;
				if (mongoClient == null) {
						mongoClient = MongodbUtil.createMongoClient(mongodbConfig);
						mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
				}
		}

		@Override
		public void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer){
				List<Bson> pipeline = singletonList(Aggregates.match(
								Filters.in("ns.coll", tableList)
				));

				if (this.globalStateMap == null) {
						this.globalStateMap = connectorContext.getGlobalStateMap();
				}
//        pipeline = new ArrayList<>();
//        List<Bson> collList = tableList.stream().map(t -> Filters.eq("ns.coll", t)).collect(Collectors.toList());
//        List<Bson> pipeline1 = asList(Aggregates.match(Filters.or(collList)));

				ConnectionString connectionString = new ConnectionString(mongodbConfig.getUri());
				while (!running.get()) {
						List<TapEvent> tapEvents = list();
						ChangeStreamIterable<Document> changeStream;
						if (offset != null) {
								//报错之后， 再watch一遍
								//如果完全没事件， 就需要从当前时间开始watch
								if (offset instanceof Integer) {
									changeStream = mongoDatabase.watch(pipeline).startAtOperationTime(new BsonTimestamp((Integer) offset, 0)).fullDocument(FullDocument.UPDATE_LOOKUP);
								} else {
									changeStream = mongoDatabase.watch(pipeline).resumeAfter((BsonDocument) offset).fullDocument(FullDocument.UPDATE_LOOKUP);
								}
						} else {
								changeStream = mongoDatabase.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
						}
						consumer.streamReadStarted();
						try (final MongoChangeStreamCursor<ChangeStreamDocument<Document>> streamCursor = changeStream.cursor()) {
										while (!running.get()) {
												ChangeStreamDocument<Document> event = streamCursor.tryNext();
												if (event == null) {
														if (!tapEvents.isEmpty()) {
																consumer.accept(tapEvents, offset);
																tapEvents = list();
														}
														continue;
												}
												if (tapEvents.size() >= eventBatchSize) {
														consumer.accept(tapEvents, offset);
														tapEvents = list();
												}

												MongoNamespace mongoNamespace = event.getNamespace();

												String collectionName = null;
												if (mongoNamespace != null) {
														collectionName = mongoNamespace.getCollectionName();
												}
												if (collectionName == null)
														continue;
												offset = event.getResumeToken();
												OperationType operationType = event.getOperationType();
												Document fullDocument = event.getFullDocument();
												if (operationType == OperationType.INSERT) {
														DataMap after = new DataMap();
														after.putAll(fullDocument);
														TapInsertRecordEvent recordEvent = insertRecordEvent(after, collectionName);
														recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
														tapEvents.add(recordEvent);
												} else if (operationType == OperationType.DELETE) {
														DataMap before = new DataMap();
														if (event.getDocumentKey() != null) {
																final Document documentKey = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
																before.put("_id", documentKey.get("_id"));
																final Map lookupData = MongodbLookupUtil.findDeleteCacheByOid(connectionString, collectionName, documentKey.get("_id"), globalStateMap);
																TapDeleteRecordEvent recordEvent = deleteDMLEvent(MapUtils.isNotEmpty(lookupData) ? lookupData : before, collectionName);
																recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
																tapEvents.add(recordEvent);
														} else {
																TapLogger.warn(TAG, "Document key is null, failed to delete. {}", event);
														}
												} else if (operationType == OperationType.UPDATE || operationType == OperationType.REPLACE) {
														DataMap after = new DataMap();
														if (MapUtils.isEmpty(fullDocument)) {
																TapLogger.warn(TAG, "Found update event already deleted in collection %s, _id %s", collectionName, event.getDocumentKey().get("_id"));
																continue;
														}
														if (event.getDocumentKey() != null) {
																after.putAll(fullDocument);

																TapUpdateRecordEvent recordEvent = updateDMLEvent(null, after, collectionName);
																recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
																tapEvents.add(recordEvent);
														} else {
																throw new RuntimeException(String.format("Document key is null, failed to update. %s", event));
														}
												}
										}
						} catch (Throwable throwable) {
								if (!running.get()) {
										final String message = throwable.getMessage();
										if (throwable instanceof IllegalStateException && EmptyKit.isNotEmpty(message) && ( message.contains("state should be: open") || message.contains("Cursor has been closed"))) {
												return;
										}

										if (throwable instanceof MongoInterruptedException || throwable instanceof InterruptedException) {
												return;
										}
								}

								if (throwable instanceof MongoCommandException) {
										MongoCommandException mongoCommandException = (MongoCommandException) throwable;
										if (mongoCommandException.getErrorCode() == 286) {
												TapLogger.error(TAG, "offset " + offset + " is too old, will stop, error " + getStackString(throwable));
										}
								} else {
										TapLogger.error(TAG, "Read change stream from {}, failed {}, error {}", MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage(), getStackString(throwable));
								}
								throw throwable;
						}
				}
		}

		@Override
		public Object streamOffset(Long offsetStartTime) {
				Object offset = null;
				ChangeStreamIterable<Document> changeStream = mongoDatabase.watch();
				if (offsetStartTime != null) {
						changeStream = changeStream.startAtOperationTime(new BsonTimestamp((int) (offsetStartTime / 1000), 0));
						try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();) {
								BsonDocument theResumeToken = cursor.getResumeToken();

								if (theResumeToken != null) {
										offset = theResumeToken;
								}
						}
				}

				if (offset == null) {
						final long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoDatabase);
					offset = (int)(serverTimestamp/1000);
//						offset = new BsonTimestamp((int) (serverTimestamp / 1000), 0);
				}

				return offset;
		}

		@Override
		public void onDestroy() {
				running.compareAndSet(true, false);
				if (mongoClient != null) {
						mongoClient.close();
				}
		}
}
