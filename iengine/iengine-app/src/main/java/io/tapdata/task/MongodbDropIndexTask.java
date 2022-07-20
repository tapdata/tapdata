package io.tapdata.task;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.tapdata.constant.MongodbUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "MONGODB_DROP_INDEX")
public class MongodbDropIndexTask implements Task {
	private TaskContext context;

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

			Map<String, Object> taskData = context.getTaskData();
			String collectionName = (String) taskData.get("collection_name");
			String uri = (String) taskData.get("uri");
			String indexName = (String) taskData.get("name");

			MongoClient mongoClient = null;
			MongoCollection collection = null;

			try {

				MongoClientURI mongoClientURI = new MongoClientURI(uri);
				String databaseName = mongoClientURI.getDatabase();
				if (StringUtils.isEmpty(databaseName)) {
					result.setTaskResultCode(201);
					result.setTaskResult("Failed to drop index, database is empty, uri: " + uri);
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
				} catch (Throwable e) {
					result.setTaskResultCode(201);
					result.setTaskResult("Get collection error, uri: " + uri + ", collection name: " + collectionName + ", message: " + e.getMessage());
					callback.accept(result);
					return;
				}

				try {
					collection.dropIndex(indexName);
					result.setTaskResultCode(200);
					result.setTaskResult("Succeed drop index: " + indexName);
				} catch (Throwable e) {
					result.setTaskResultCode(201);

					if (e instanceof MongoCommandException && ((MongoCommandException) e).getErrorCode() == 27) {
						result.setTaskResultCode(200);
					}
					String msg = e instanceof MongoCommandException ? ((MongoCommandException) e).getErrorCodeName() : e.getMessage();

					result.setTaskResult("Failed to drop index, uri: " + uri + ", collection: " + collectionName + ", message: " + msg);
				}

			} finally {
				if (mongoClient != null) {
					mongoClient.close();
				}
			}
		}

		callback.accept(result);
	}

	private String validateTaskData() {
		String err = "";
		Map<String, Object> taskData = context.getTaskData();

		if (MapUtils.isEmpty(taskData)) {
			err = "task_data cannot be empty.";

		} else if (!taskData.containsKey("collection_name")) {
			err = "task_data.collection_name cannot be empty.";
		} else if (!(taskData.get("collection_name") instanceof String)) {
			err = "task_data.collection_name must be string.";

		} else if (!taskData.containsKey("uri")) {
			err = "task_data.uri cannot be empty.";
		} else if (!(taskData.get("uri") instanceof String)) {
			err = "task_data.uri must be string.";

		} else if (!taskData.containsKey("name")) {
			err = "task_data.name cannot be empty.";
		} else if (!(taskData.get("name") instanceof String)) {
			err = "task_data.name must be string.";
		}

		return err;
	}
}
