package io.tapdata.mongodb.reader.v3;

import com.mongodb.ConnectionString;
import com.mongodb.CursorType;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.mongodb.MongodbConnector;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbStreamReader;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.base.ConnectorBase.*;

/**
 * @author jackin
 * @date 2022/5/17 17:51
 **/
public class MongodbV3StreamReader implements MongodbStreamReader {

		public static final String TAG = MongodbConnector.class.getSimpleName();

		private final static String LOCAL_DATABASE = "local";
		private final static String OPLOG_COLLECTION = "oplog.rs";

		private MongodbConfig mongodbConfig;

		private MongoClient mongoClient;

		private Set<String> namespaces = new HashSet<>();

		private Map<String, String> nodesURI;

		private Map<String, MongoV3StreamOffset> offset;

		private final AtomicBoolean running = new AtomicBoolean(false);

		private ThreadPoolExecutor replicaSetReadThreadPool;

		private LinkedBlockingDeque<TapEventOffset> tapEventQueue;

		private Exception error;

		private KVMap<Object> globalStateMap;

		private ConnectionString connectionString;

		@Override
		public void onStart(MongodbConfig mongodbConfig) {
				this.mongodbConfig = mongodbConfig;
				mongoClient = MongodbUtil.createMongoClient(mongodbConfig);

				nodesURI = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());
				running.compareAndSet(false, true);
				connectionString = new ConnectionString(mongodbConfig.getUri());
				replicaSetReadThreadPool = new ThreadPoolExecutor(nodesURI.size(), nodesURI.size(), 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
		}

		@Override
		public void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) throws Exception {
				if (CollectionUtils.isNotEmpty(tableList)) {
						for (String tableName : tableList) {
								namespaces.add(new StringBuilder(mongodbConfig.getDatabase()).append(".").append(tableName).toString());
						}
				}

				if (tapEventQueue == null) {
						tapEventQueue = new LinkedBlockingDeque<>(eventBatchSize);
				}

				if (this.globalStateMap == null) {
						this.globalStateMap = connectorContext.getGlobalStateMap();
				}

				if (offset != null) {
						this.offset = (Map<String, MongoV3StreamOffset>) offset;
				} else {
						this.offset = new ConcurrentHashMap<>();
				}

				if (MapUtils.isNotEmpty(nodesURI)) {
						for (Map.Entry<String, String> entry : nodesURI.entrySet()) {
								final String replicaSetName = entry.getKey();
								final String mongodbURI = entry.getValue();

								replicaSetReadThreadPool.submit(() -> {
									if (running.get()) {
										try {
											Thread.currentThread().setName("replicaSet-read-thread-" + replicaSetName);
											readFromOplog(replicaSetName, mongodbURI, eventBatchSize, consumer);
										} catch (Exception e) {
											running.compareAndSet(true, false);
											TapLogger.error(TAG, "read oplog event from {} failed {}", replicaSetName, e.getMessage(), e);
											error = e;
										}
									}
								});
						}
				}

				List<TapEvent> tapEvents = new ArrayList<>(eventBatchSize);
				while (running.get()) {
					try {
						final TapEventOffset tapEventOffset = tapEventQueue.poll(3, TimeUnit.SECONDS);
						if (tapEventOffset != null) {
								tapEvents.add(tapEventOffset.getTapEvent());
								this.offset.put(tapEventOffset.getReplicaSetName(), tapEventOffset.getOffset());
								if (tapEvents.size() >= eventBatchSize) {
										consumer.accept(tapEvents, this.offset);
										tapEvents = new ArrayList<>(eventBatchSize);
								}
						} else if (tapEvents.size() > 0) {
								consumer.accept(tapEvents, this.offset);
								tapEvents = new ArrayList<>(eventBatchSize);
						}
					} catch (InterruptedException e) {
						TapLogger.info("Stream polling failed: {}", e.getMessage(), e);
						break;
					}
				}

				if (error != null) {
						throw error;
				}
		}

		@Override
		public Object streamOffset(Long offsetStartTime) {
				if (offsetStartTime == null) {
					offsetStartTime = MongodbUtil.mongodbServerTimestamp(mongoClient.getDatabase(mongodbConfig.getDatabase()));
				}
			ConcurrentHashMap<Object, Object> offset = new ConcurrentHashMap<>();
			for (String rs : nodesURI.keySet()) {
				offset.put(rs, new MongoV3StreamOffset((int) (offsetStartTime / 1000), 0));
			}
			return offset;
		}

		@Override
		public void onDestroy() {
				running.compareAndSet(true, false);
				if (mongoClient != null) {
						mongoClient.close();
				}

				if (replicaSetReadThreadPool != null) {
						replicaSetReadThreadPool.shutdownNow();

						while (!replicaSetReadThreadPool.isTerminated()) {
								TapLogger.info(TAG, "Waiting replicator thread(s) exit.");
								try {
										Thread.sleep(500l);
								} catch (InterruptedException e) {
										// nothing to do
								}
						}
				}
		}

		private void readFromOplog(String replicaSetName, String mongodbURI, int eventBatchSize, StreamReadConsumer consumer) {
				Bson filter = null;

				BsonTimestamp startTs = null;
				if (MapUtils.isNotEmpty(offset) && offset.containsKey(replicaSetName)) {
						final MongoV3StreamOffset mongoV3StreamOffset = (MongoV3StreamOffset) offset.get(replicaSetName);
						final int seconds = mongoV3StreamOffset.getSeconds();
						startTs = new BsonTimestamp(seconds, mongoV3StreamOffset.getInc());
				} else {
						startTs = new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0);
				}

				final Bson fromMigrateFilter = Filters.exists("fromMigrate", false);

				try (MongoClient mongoclient = MongoClients.create(mongodbURI)) {
						final MongoCollection<Document> oplogCollection = mongoclient.getDatabase(LOCAL_DATABASE).getCollection(OPLOG_COLLECTION);
//						List<TapEvent> tapEvents = new ArrayList<>(eventBatchSize);
						// todo exception retry
						while (running.get()) {
								filter = Filters.and(Filters.gte("ts", startTs), fromMigrateFilter);
								try (final MongoCursor<Document> mongoCursor = oplogCollection.find(filter)
												.sort(new Document("$natural", 1))
												.oplogReplay(true)
												.cursorType(CursorType.TailableAwait)
												.noCursorTimeout(true).iterator()) {

										consumer.streamReadStarted();
										while (running.get()) {
												if (mongoCursor.hasNext()) {
														final Document event = mongoCursor.next();
														final TapBaseEvent tapBaseEvent = handleOplogEvent(event);
														if (tapBaseEvent == null) {
																continue;
														}

														final BsonTimestamp bsonTimestamp = event.get("ts", BsonTimestamp.class);
														tapBaseEvent.setReferenceTime((long) (bsonTimestamp.getTime()) * 1000);

														while (running.get()) {
																if (tapEventQueue.offer(
																				new TapEventOffset(
																								tapBaseEvent,
																								new MongoV3StreamOffset(bsonTimestamp.getTime(), bsonTimestamp.getInc()),
																								replicaSetName
																				),
																				3,
																				TimeUnit.SECONDS
																)) {
																		break;
																}
														}
//														Map<String, MongoV3StreamOffset> snapshotOffset = new ConcurrentHashMap<>(offset);
//														tapEvents.add(tapEvent);

//														snapshotOffset.put(replicaSetName, new MongoV3StreamOffset(bsonTimestamp.getTime(), bsonTimestamp.getInc()));
//														if (tapEvents.size() % eventBatchSize == 0) {
//																consumer.accept(tapEvents, snapshotOffset);
//																tapEvents = new ArrayList<>();
//														}
														startTs = bsonTimestamp;
												}
//												else {
//														if (tapEvents.size() > 0) {
//																consumer.accept(tapEvents);
//																tapEvents = new ArrayList<>();
//														}
//														Thread.sleep(500L);
//												}
										}
								} catch (InterruptedException | MongoInterruptedException e) {
										running.compareAndSet(true, false);
								}
						}
				}
		}

		protected TapBaseEvent handleOplogEvent(Document event) {
				TapLogger.debug(TAG, "Found event: {}", event);
				String ns = event.getString("ns");
				Document object = event.get("o", Document.class);
				if (object == null) {
						TapLogger.warn(TAG, "Missing 'o' field in event, so skipping {}", event.toJson());
						return null;
				}
				TapBaseEvent tapBaseEvent = null;
				if (ns == null || ns.isEmpty()) {
						// These are replica set events ...
//						String msg = object.getString("msg");
//						if ("new primary".equals(msg)) {
//								AtomicReference<ServerAddress> address = new AtomicReference<>();
//								try {
//										primaryClient.executeBlocking("conn", mongoClient -> {
//												ServerAddress currentPrimary = mongoClient.getAddress();
//												address.set(currentPrimary);
//										});
//								} catch (InterruptedException e) {
//										logger.error("Get current primary executeBlocking", e);
//								}
//								ServerAddress serverAddress = address.get();
//
//								if (serverAddress != null && !serverAddress.equals(primaryAddress)) {
//										logger.info("Found new primary event in oplog, so stopping use of {} to continue with new primary",
//														primaryAddress);
//										// There is a new primary, so stop using this server and instead use the new primary ...
//										return false;
//								} else {
//										logger.info("Found new primary event in oplog, current {} is new primary. " +
//														"Continue to process oplog event.", primaryAddress);
//								}
//						}
						// Otherwise, ignore this event ...
						TapLogger.debug(TAG, "Skipping event with no namespace: {}", event.toJson());
						return null;
				}
				int delimIndex = ns.indexOf('.');
				if (delimIndex > 0) {
						String dbName = ns.substring(0, delimIndex);
						String collectionName = ns.substring(delimIndex + 1);

						// Otherwise, it is an event on a document in a collection ...
						if (!namespaces.contains(ns)) {
								TapLogger.debug(TAG, "Skipping the event for database {} based on database.whitelist");
//								try {
//										// generate last msg event's timestamp event
//										factory.recordEvent(event, clock.currentTimeInMillis(), false);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
								return null;
						}

						if (namespaces.contains(ns)) {
								Document o = event.get("o", Document.class);
								if ("u".equalsIgnoreCase(event.getString("op"))) {
										final Document o2 = event.get("o2", Document.class);
										Object _id = o2 != null ? o2.get("_id") : o.get("_id");
										Document after = null;
										Map<String, Object> info = new HashMap<>();
										if (mongodbConfig.isEnableFillingModifiedData()) {
											try (final MongoCursor<Document> mongoCursor = mongoClient.getDatabase(dbName).getCollection(collectionName).find(new Document("_id", _id)).iterator();) {
												if (mongoCursor.hasNext()) {
													after = mongoCursor.next();
												}
											}
											if (after == null) {
												TapLogger.warn(TAG, "Found update event _id {} already deleted in collection {}, event {}", _id, collectionName, event.toJson());
												return null;
											}
										} else {
											after = new Document("_id", _id);
											info.put("_id", _id);
											info.put("$op", o);
										}
										tapBaseEvent = updateDMLEvent(null, after, collectionName);
										Map<String, Object> originUnset = o.get("$unset", Map.class);
										Map<String, Object> finalUnset = new DataMap();
										if (originUnset != null) {
											for (Map.Entry<String, Object> entry : originUnset.entrySet()) {
												if (after == null || after.keySet().stream().noneMatch(v -> v.equals(entry.getKey()) || v.startsWith(entry.getKey() + ".") || entry.getKey().startsWith(v + "."))) {
													finalUnset.put(entry.getKey(), true);
												}
//												if (!after.containsKey(entry.getKey())) {
//													finalUnset.put(entry.getKey(), true);
//												}
											}
										}
										if (finalUnset.size() > 0) {
											info.put("$unset", finalUnset);
										}
										tapBaseEvent.setInfo(info);
								} else if ("i".equalsIgnoreCase(event.getString("op"))) {
										tapBaseEvent = insertRecordEvent(o, collectionName);
								} else if ("d".equalsIgnoreCase(event.getString("op"))) {
										final Map lookupData = MongodbLookupUtil.findDeleteCacheByOid(connectionString, collectionName, o.get("_id"), globalStateMap);
										tapBaseEvent = deleteDMLEvent(lookupData != null ? (Map)lookupData.get("data") : o, collectionName);
								}
//								try {
//										factory.recordEvent(event, clock.currentTimeInMillis(), true);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
						}
//						else {
//								try {
//										// generate last msg event's timestamp event
//										factory.recordEvent(event, clock.currentTimeInMillis(), false);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
//						}
				}
				return tapBaseEvent;
		}
}
