package io.tapdata.task;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.MongodbUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@TaskType(type = "MONGODB_CREATE_INDEX")
public class MongodbCreateIndexTask implements Task {
	private TaskContext context;
	private String indexName = "";
	private String uri = "";
	private Map<String, Object> keys;
	private boolean unique = false;
	private long expireAfterSeconds = -1;
	private String collectionName = "";
	private boolean background = false;
	private MongoClient mongoClient = null;
	private MongoCollection collection = null;
	private Document keyDoc = new Document();

	private Logger logger = LogManager.getLogger(getClass());

	@Override
	public void initialize(TaskContext taskContext) {
		this.context = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult result = new TaskResult();

		String err = validateTaskData();
		if (StringUtils.isNotBlank(err)) {
			result.setTaskResultCode(201);
			result.setTaskResult(err);
		} else {

			try {
				try {
					readTaskData();
				} catch (Exception e) {
					result.setTaskResultCode(201);
					result.setTaskResult("Read task_data error, message: " + e.getMessage());
					callback.accept(result);
					return;
				}

				MongoClientURI mongoClientURI = new MongoClientURI(uri);
				String databaseName = mongoClientURI.getDatabase();
				if (StringUtils.isEmpty(databaseName)) {
					result.setTaskResultCode(201);
					result.setTaskResult("Failed to create index, database is empty, uri: " + uri);
					callback.accept(result);
					return;
				}

				try {
					mongoClient = MongodbUtil.createMongoClient(uri);
				} catch (Throwable e) {
					result.setTaskResultCode(201);
					result.setTaskResult("Failed to connect database, uri: " + uri + ", message: " + e.getMessage());
					callback.accept(result);
					return;
				}

				try {
					collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
				} catch (Exception e) {
					result.setTaskResultCode(201);
					result.setTaskResult("Get collection error, uri: " + uri + ", collection name: " + collectionName + ", message: " + e.getMessage());
					callback.accept(result);
					return;
				}
				if (collection != null) {

					if (keys != null) {
						buildKeyDocument();
					}

					IndexOptions indexOptions = buildIndexOptions();

					try {
						logger.info("Create index option " + indexOptions);
						String index = collection.createIndex(keyDoc, indexOptions);
						if (StringUtils.isNotBlank(index)) {
							result.setTaskResultCode(200);
							result.setTaskResult("Succeed create index: " + index);
						} else {
							result.setTaskResultCode(201);
							result.setTaskResult("Failed to create index.");
						}
					} catch (Exception e) {
						result.setTaskResultCode(201);
						result.setTaskResult("Create index error, message: " + e.getMessage());
					}
				} else {
					result.setTaskResultCode(201);
					result.setTaskResult("Failed to get collection, uri: " + uri + ", collection name: " + collectionName);
				}

			} finally {
				if (mongoClient != null) {
					mongoClient.close();
				}
			}
		}

		callback.accept(result);
	}

	private void readTaskData() {
		Map<String, Object> taskData = context.getTaskData();
		if (MapUtils.isNotEmpty(taskData)) {
			if (taskData.get("name") != null) {
				indexName = taskData.get("name").toString();
			}
			if (taskData.get("uri") != null) {
				uri = taskData.get("uri").toString();
			}
			keys = (Map) taskData.get("key");
			if (taskData.containsKey("unique")) {
				unique = (boolean) taskData.get("unique");
			}
			if (taskData.containsKey("expireAfterSeconds")
					&& StringUtils.isNotBlank(taskData.get("expireAfterSeconds").toString())) {
				expireAfterSeconds = Long.parseLong(taskData.get("expireAfterSeconds").toString());
			}
			collectionName = taskData.get("collection_name").toString();
			if (taskData.containsKey("background")) {
				background = (boolean) taskData.get("background");
			}
		}

	}

	private void buildKeyDocument() {
		Iterator<Map.Entry<String, Object>> iterator = keys.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Object> next = iterator.next();

			String key = null;
			Object value = null;
			try {
				key = next.getKey();
				value = next.getValue();
				keyDoc.append(key, value);
			} catch (NumberFormatException e) {
				continue;
			}
		}
	}

	private IndexOptions buildIndexOptions() {
		IndexOptions indexOptions = new IndexOptions().unique(unique).background(background);
		if (expireAfterSeconds >= 0) {
			indexOptions.expireAfter(expireAfterSeconds, TimeUnit.SECONDS);
		}
		if (StringUtils.isNotBlank(indexName) && !MongodbUtil.checkIndexNameIfExists(collection, indexName)) {
			indexOptions.name(indexName);
		}

		return indexOptions;
	}

	private String validateTaskData() {
		String err = "";
		Map<String, Object> taskData = context.getTaskData();

		if (MapUtils.isEmpty(taskData)) {
			err = "task_data cannot be empty.";
		} else if (!taskData.containsKey("key")) {
			err = "task_data.key cannot be empty.";
		} else if (!taskData.containsKey("uri")) {
			err = "task_data.uri cannot be empty.";
		} else if (taskData.containsKey("unique") && !(taskData.get("unique") instanceof Boolean)) {
			err = "task_data.unique type wrong, only can be true/false";
		} else if (taskData.containsKey("background") && !(taskData.get("background") instanceof Boolean)) {
			err = "task_data.background type wrong, only can be true/false.";
		}

		try {
			Object keyTemp = taskData.get("key");
			Map<String, Object> key = null;
			if (keyTemp instanceof Map) {
				key = (Map) keyTemp;
			} else if (keyTemp instanceof String) {
				key = JSONUtil.json2Map((String) keyTemp);
				taskData.put("key", key);
			}
			if (MapUtils.isEmpty(key)) {
				err = "task_data.key cannot be empty.";
				return err;
			}
		} catch (Exception e) {
			err = "task_data.key type wrong, only can be Map<String,Integer>.";
			return err;
		}

		if (taskData.containsKey("expireAfterSeconds")) {
			try {
				if (StringUtils.isNotBlank(taskData.get("expireAfterSeconds").toString())) {
					long expireAfterSeconds = Long.parseLong(taskData.get("expireAfterSeconds").toString());
					if (expireAfterSeconds < 0) {
						err = "task_data.expireAfterSeconds must greater than 0.";
						return err;
					}
				}
			} catch (Exception e) {
				err = "task_data.expireAfterSeconds type wrong, only can be number.";
				return err;
			}
		}

		return err;
	}
}
