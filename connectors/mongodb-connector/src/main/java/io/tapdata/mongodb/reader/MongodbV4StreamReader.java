package io.tapdata.mongodb.reader;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapdata.exception.TapPdkRetryableEx;
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

import java.util.ArrayList;
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
		running.compareAndSet(false, true);
	}

	@Override
	public void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
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
		FullDocument fullDocumentOption = FullDocument.UPDATE_LOOKUP;
		List<TapEvent> tapEvents = list();
		while (running.get()) {
			ChangeStreamIterable<Document> changeStream;
			if (offset != null) {
				//报错之后， 再watch一遍
				//如果完全没事件， 就需要从当前时间开始watch
				if (offset instanceof Integer) {
					changeStream = mongoDatabase.watch(pipeline).startAtOperationTime(new BsonTimestamp((Integer) offset, 0)).fullDocument(fullDocumentOption);
				} else {
					changeStream = mongoDatabase.watch(pipeline).resumeAfter((BsonDocument) offset).fullDocument(fullDocumentOption);
				}
			} else {
				changeStream = mongoDatabase.watch(pipeline).fullDocument(fullDocumentOption);
			}
			consumer.streamReadStarted();
			try (final MongoChangeStreamCursor<ChangeStreamDocument<Document>> streamCursor = changeStream.cursor()) {
				while (running.get()) {
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
							TapDeleteRecordEvent recordEvent = deleteDMLEvent(MapUtils.isNotEmpty(lookupData) && lookupData.containsKey("data") && lookupData.get("data") instanceof Map
									? (Map<String, Object>) lookupData.get("data") : before, collectionName);
							recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
							tapEvents.add(recordEvent);
						} else {
							TapLogger.warn(TAG, "Document key is null, failed to delete. {}", event);
						}
					} else if (operationType == OperationType.UPDATE || operationType == OperationType.REPLACE) {
						DataMap after = new DataMap();
						if (MapUtils.isEmpty(fullDocument)) {
							if (fullDocumentOption == FullDocument.DEFAULT) {
								final Document documentKey = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
								try (final MongoCursor<Document> mongoCursor = mongoDatabase.getCollection(collectionName).find(documentKey).iterator();) {
									if (mongoCursor.hasNext()) {
										fullDocument = mongoCursor.next();
									}
								}
							}

							if (MapUtils.isEmpty(fullDocument)) {
								TapLogger.warn(TAG, "Found update event already deleted in collection {}, _id {}", collectionName, event.getDocumentKey().get("_id"));
								continue;
							}
						}

						if (event.getDocumentKey() != null) {
							after.putAll(fullDocument);

							TapUpdateRecordEvent recordEvent = updateDMLEvent(null, after, collectionName);
//							Map<String, Object> info = new DataMap();
//							Map<String, Object> unset = new DataMap();
							List<String> removedFields = new ArrayList<>();
							UpdateDescription updateDescription = event.getUpdateDescription();
							if (updateDescription != null) {
								for (String f:updateDescription.getRemovedFields()) {
									if (after.keySet().stream().noneMatch(v -> v.equals(f) || v.startsWith(f + ".") || f.startsWith(v + "."))) {
//										unset.put(f, true);
										removedFields.add(f);
									}
//									if (!after.containsKey(f)) {
//										unset.put(f, true);
//									}
								}
//								if (unset.size() > 0) {
//									info.put("$unset", unset);
//								}
								if (removedFields.size() > 0) {
									recordEvent.removedFields(removedFields);
								}
							}
//							recordEvent.setInfo(info);
							recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
							tapEvents.add(recordEvent);
						} else {
							throw new RuntimeException(String.format("Document key is null, failed to update. %s", event));
						}




						// The default mode FullDocument.DEFAULT indicates that the reverse lookup phase is entered
						// and need to switch to FullDocument.UPDATE_LOOKUP
						if (fullDocumentOption == FullDocument.DEFAULT) {
							if (!tapEvents.isEmpty()) {
								consumer.accept(tapEvents, offset);
								tapEvents = list();
							}
							fullDocumentOption = FullDocument.UPDATE_LOOKUP;
							break;
						}
					}
				}
			} catch (Throwable throwable) {
				if (!running.get()) {
					final String message = throwable.getMessage();
					if (throwable instanceof IllegalStateException && EmptyKit.isNotEmpty(message) && (message.contains("state should be: open") || message.contains("Cursor has been closed"))) {
						return;
					}

					if (throwable instanceof MongoInterruptedException || throwable instanceof InterruptedException) {
						return;
					}
				}

				if (throwable instanceof MongoCommandException) {
					MongoCommandException mongoCommandException = (MongoCommandException) throwable;
					if (mongoCommandException.getErrorCode() == 286) {
						throw new TapPdkOffsetOutOfLogEx(connectorContext.getSpecification().getId(), offset, throwable);
					}

					if (mongoCommandException.getErrorCode() == 10334) {
						fullDocumentOption = FullDocument.DEFAULT;
						continue;
					}

					if (mongoCommandException.getErrorCode() == 211) {
						throw new TapPdkRetryableEx(connectorContext.getSpecification().getId(), throwable);
					}
				}

				if (throwable instanceof MongoQueryException) {
					MongoQueryException mongoQueryException = (MongoQueryException) throwable;
					if (mongoQueryException.getErrorCode() == 10334) {
						fullDocumentOption = FullDocument.DEFAULT;
						continue;
					}
				}
				//else {
				//TapLogger.warn(TAG,"Read change stream from {}, failed {} " ,MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage());
				//TapLogger.debug(TAG, "Read change stream from {}, failed {}, error {}", MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage(), getStackString(throwable));
				//}
				throw throwable;
			}
		}
	}

	@Override
	public Object streamOffset(Long offsetStartTime) {
		if (null == offsetStartTime) {
			offsetStartTime = MongodbUtil.mongodbServerTimestamp(mongoDatabase);
		}
		return (int) (offsetStartTime / 1000);
	}

	@Override
	public void onDestroy() {
		running.compareAndSet(true, false);
		if (mongoClient != null) {
			mongoClient.close();
		}
	}
}
