package io.tapdata.task.metadata;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MetadataUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.MetadataInstance;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.task.Task;
import io.tapdata.task.TaskContext;
import io.tapdata.task.TaskResult;
import io.tapdata.task.TaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@TaskType(type = "META_DATA_STATS_MONGO_INDEXES")
public class StatsMongoDBIndexesTask implements Task {

	private Logger logger = LogManager.getLogger(StatsMongoDBIndexesTask.class);

	private final static String[] INVALID_KEY_CHAR = {"$"};
	private final static String REPLACEMENT = "_";

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult result = new TaskResult();
		result.setPassResult();
		try {
			statsMongoDBIndexes();

			statsCreateDeleteIndexes();
			result.setTaskResultCode(200);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			result.setTaskResultCode(201);
			result.setTaskResult(e.getMessage());
		}

		callback.accept(result);
	}

	private void statsCreateDeleteIndexes() {
		try {
			ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
			MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
			if (mongoTemplate != null) {
				MongoDatabase metaDatabase = mongoTemplate.getDb();
				for (Document statsTask : metaDatabase.getCollection(ConnectorConstant.SCHEDULE_TASK_COLLECTION).find(new Query(where("task_name").is("meta_data_stats_mongo_indexes_task")).getQueryObject())) {
					if (MapUtils.isNotEmpty(statsTask)) {
						Query queryTaskFilter = new Query(where("status").is("paused").orOperator(where("task_name").is("mongodb_create_index"), where("task_name").is("mongodb_drop_index")));

						Object lastProcessOffset = statsTask.get("statsOffset");
						if (lastProcessOffset != null) {
							queryTaskFilter.addCriteria(where("last_updated").gt(lastProcessOffset));
						}

//						logger.info("Start stats create/delete indexes task, start offset is {}", lastProcessOffset == null ? lastProcessOffset : "scan all task.");
						for (Document scheduleTask : metaDatabase.getCollection(ConnectorConstant.SCHEDULE_TASK_COLLECTION).find(queryTaskFilter.getQueryObject()).sort(new Document("last_updated", 1))) {
							if (MapUtils.isNotEmpty(scheduleTask) && scheduleTask.containsKey("task_data")) {
								Document taskData = (Document) scheduleTask.get("task_data");
								String taskName = scheduleTask.getString("task_name");
								String indexName = taskData.getString("name");
								String collectionName = taskData.getString("collection_name");
								String databaseURI = taskData.getString("uri");
								String databaseName = new MongoClientURI(databaseURI).getDatabase();
								String metaId = taskData.containsKey("meta_id") ? taskData.getString("meta_id") : "";
								Object key = taskData.get("key");

								if (StringUtils.isBlank(metaId)) {
									logger.warn("Task {} is a dirty data, will skip it.");
									lastProcessOffset = scheduleTask.getDate("last_updated");
									continue;
								}

								logger.info("Found collection {} create/delete index {}", collectionName, key);

								if ("mongodb_create_index".equals(taskName)) {
									logger.info("Found create index task {}", scheduleTask.toJson());

									Document taskHistoryFilter = new Document("task_id", scheduleTask.getObjectId("_id").toHexString());
//									taskHistoryFilter.append("task_result_code", new Document("$ne", 200));
									for (Document createIndexResult : metaDatabase.getCollection("TaskHistories").find(taskHistoryFilter)
											.sort(new Document("task_result_code", -1)).limit(1)) {

										if (!createIndexResult.getInteger("task_result_code").equals(200)) {
											Document updateFileter = new Document("_id", new ObjectId(metaId));
											updateFileter.append("indexes", new Document("$elemMatch", new Document("name", indexName)));

											Document update = new Document("$set",
													new Document("indexes.$.status", "creation_failed")
															.append("indexes.$.error_msg", createIndexResult.get("task_result"))
											);

											metaDatabase.getCollection("MetadataInstances").updateMany(updateFileter, update);
										}
										lastProcessOffset = scheduleTask.getDate("last_updated");
									}
								} else {

									logger.info("Found delete index task {}.", scheduleTask.toJson());

									Document taskHistoryFilter = new Document("task_id", scheduleTask.getObjectId("_id").toHexString());
//									taskHistoryFilter.append("task_result_code", new Document("$eq", 200));

									long taskExecSuccCount = metaDatabase.getCollection("TaskHistories").countDocuments(taskHistoryFilter);
									if (taskExecSuccCount > 0) {
										Document updateFileter = new Document("_id", new ObjectId(metaId));
										updateFileter.append("indexes", new Document("$elemMatch", new Document("name", indexName)));

										Document update = new Document("$pull",
												new Document("indexes", new Document("name", indexName))
										);

										metaDatabase.getCollection("MetadataInstances").updateMany(updateFileter, update);
										lastProcessOffset = scheduleTask.getDate("last_updated");
									}
								}

							} else {
								logger.warn("Stats create/delete index task info failed, task data is null. ", scheduleTask);
							}
						}

						if (lastProcessOffset != null) {
							metaDatabase.getCollection("ScheduleTasks").updateMany(
									new Document("task_name", "meta_data_stats_mongo_indexes_task"),
									new Document("$set", new Document("statsOffset", lastProcessOffset))
							);
						}

//						logger.info("Completed stats create/delete indexes task, last process offset is {}", lastProcessOffset);
					}
				}
			}

		} catch (Exception e) {
			throw new RuntimeException("Stats target mongodb create/delete index failed ", e);
		}
	}

	private void statsMongoDBIndexes() {
//		logger.info("Start stats target mongodb indexes.");
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();

		try {
			Query query = new Query(where("status").is("ready").and("database_type").is("mongodb").orOperator(where("connection_type").is("target"), where("connection_type").is("source_and_target")));
			query.fields().exclude("schema").exclude("response_body");
			List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
			if (CollectionUtils.isNotEmpty(connections)) {
				for (Connections connection : connections) {

					try (MongoClient mongoClient = MongodbUtil.createMongoClient(connection, MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {

						String database = MongodbUtil.getDatabase(connection.getDatabase_uri());

						ListCollectionsIterable<Document> collectionInfos = mongoClient.getDatabase(database).listCollections();
						for (Document collectionInfo : collectionInfos) {
							String type = collectionInfo.getString("type");
							String collectionName = collectionInfo.getString("name");

							if (MongodbUtil.systemTables.contains(collectionName)) {
								continue;
							}
							String databaseType = connection.getDatabase_type();
							String databaseUri = connection.getDatabase_uri();
							MongoClientURI mongoClientURI = new MongoClientURI(databaseUri);
							String databaseName = mongoClientURI.getDatabase();

							if ("collection".equals(type)) {
								String qualifiedName = MetadataUtil.formatQualifiedName("MC_" + databaseType + "_" + databaseName + "_" + collectionName + "_" + connection.getId());

								List<MetadataInstance> metadataInstances = clientMongoOperator.find(new Query(where("qualified_name").is(qualifiedName)), ConnectorConstant.METADATA_INSTANCE_COLLECTION, MetadataInstance.class);
								Map<String, Map<String, Object>> metadataIndexesMap = new HashMap<>();

								if (CollectionUtils.isNotEmpty(metadataInstances)) {
									MetadataInstance metadataInstance = metadataInstances.get(0);
									if (CollectionUtils.isNotEmpty(metadataInstance.getIndexes())) {
										for (Map<String, Object> index : metadataInstance.getIndexes()) {
											String name = (String) index.get("name");
											Object key = index.get("key");
											String keyStr = "";

											if (key != null && key instanceof Map) {
												keyStr = JSONUtil.map2Json((Map) key);
											} else if (key instanceof String) {
												keyStr = (String) key;
											}
											if (!"_id_".equals(name)) {
												metadataIndexesMap.put(keyStr, index);
											}
										}
									}
								}

								for (Document index : mongoClient.getDatabase(database).getCollection(collectionName).listIndexes()) {
									String indexName = index.getString("name");
									String keyStr = JSONUtil.map2Json((Map) index.get("key"));
									if (metadataIndexesMap.containsKey(keyStr)) {
										Map<String, Object> metaIndexInfo = metadataIndexesMap.get(keyStr);
										metaIndexInfo.put("name", indexName);
										metaIndexInfo.put("status", "created");
										if (StringUtils.isBlank(metaIndexInfo.getOrDefault("create_by", "") + "")) {
											metaIndexInfo.put("create_by", "user");
										}
										metaIndexInfo.remove("error_msg");
									} else {
										index.put("status", "created");
										index.put("create_by", "dba");
										metadataIndexesMap.put(indexName, index);
									}
								}

								if (MapUtils.isNotEmpty(metadataIndexesMap)) {
									Collection<Map<String, Object>> indexes = metadataIndexesMap.values();
									if (CollectionUtils.isNotEmpty(indexes)) {
										indexes.forEach(index -> replaceInvalidKeys(index));
									}
									clientMongoOperator.update(new Query(where("qualified_name").is(qualifiedName)),
											new Update().set("indexes", indexes), ConnectorConstant.METADATA_INSTANCE_COLLECTION);
								}
							}
						}
					} catch (Exception e) {
						logger.error("Get target connection {}'s indexes info failed.", connection.getName(), e);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Stats target mongodb indexes failed ", e);
		}

//		logger.info("Completed stats target mongodb indexes.");
	}

	private void replaceInvalidKeys(Map<String, Object> map) {
		if (MapUtils.isEmpty(map)) {
			return;
		}

		Iterator<String> keyIter = map.keySet().iterator();
		while (keyIter.hasNext()) {
			String key = keyIter.next();
			Object value = map.get(key);

			if (StringUtils.startsWithAny(key, INVALID_KEY_CHAR)) {
				String newKey = REPLACEMENT + key.substring(1);
				map.put(newKey, value);
				map.remove(key);
			}

			Optional.ofNullable(value).ifPresent(v -> {
				if (v instanceof Map) {
					replaceInvalidKeys((Map<String, Object>) v);
				} else if (v instanceof List) {
					Iterator<?> listIter = ((List<?>) v).iterator();
					while (listIter.hasNext()) {
						Object listNode = listIter.next();
						Optional.ofNullable(listNode).ifPresent(node -> {
							if (node instanceof Map) {
								replaceInvalidKeys((Map<String, Object>) node);
							}
						});
					}
				}
			});
		}
	}
}
