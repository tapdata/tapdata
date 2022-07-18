package io.tapdata.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import io.tapdata.common.JSONUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author lg
 * Create by lg on 10/21/19 2:11 PM
 * <p>
 * Apply a user-defined MongoDB pipeline to the target MongoDB.
 */
@TaskType(type = "MONGODB_CANCEL_PIPELINE")
public class MongoDBCancelPipeline implements Task {

	private Logger logger = LogManager.getLogger(getClass());
	private TaskContext context;

	public void initialize(TaskContext taskContext) {
		try {
			logger.info("Initialize cancel pipeline task, taskData is " + JSONUtil.obj2Json(taskContext.getTaskData()));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		this.context = taskContext;
	}

	public void execute(Consumer<TaskResult> callback) {
		logger.info("Execute cancel pipeline task.");
		TaskResult result = new TaskResult();

		String err = validateTaskData();
		if (StringUtils.isNotBlank(err)) {
			result.setTaskResultCode(201);
			result.setTaskResult(err);
			callback.accept(result);
			logger.info("Validate taskData params fail: " + err);
			return;
		}

		Map<String, Object> taskData = context.getTaskData();

		MongoClient mongoClient = null;
		String databaseName = null;

		// get connection config
		String connectionId = (String) taskData.get("source");
		Query query = new Query(where("_id").is(connectionId));
		query.fields().exclude("schema");
		List<Connections> connections = context.getClientMongoOperator().find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
		Connections connection = null;
		if (connections == null || connections.size() == 0 || connections.get(0) == null) {
			result.setTaskResultCode(201);
			result.setTaskResult("Failed to apply pipeline, can't find mongodb connection: " + connectionId);
			callback.accept(result);
			logger.error(result.getTaskResult());
			return;
		} else {
			connection = connections.get(0);
		}

		if (StringUtils.isNotEmpty(connection.getDatabase_uri())) {
			MongoClientURI uri = new MongoClientURI(connection.getDatabase_uri());
			databaseName = uri.getDatabase();
		} else {
			databaseName = connection.getDatabase_name();
		}
		if (StringUtils.isEmpty(databaseName)) {
			result.setTaskResultCode(201);
			result.setTaskResult("Failed to apply pipeline, database is empty, uri: " + connection.getDatabase_uri());
			callback.accept(result);
			logger.error(result.getTaskResult());
			return;
		}

		try {

			// connect to database
			try {
				mongoClient = MongodbUtil.createMongoClient(connection);
			} catch (UnsupportedEncodingException e) {
				logger.error("Apply a user-defined MongoDB pipeline fail, can't connect to target mongodb.", e);
				result.setTaskResultCode(201);
				result.setTaskResult("Can't connect to target mongodb " + connectionId + ".");
				callback.accept(result);
				logger.error(result.getTaskResult());
				return;
			}

			MongoDatabase db = mongoClient.getDatabase(databaseName);


			String viewName = (String) taskData.get("name");
//			String viewOn = (String) taskData.get("collection");
//			String pipelineJson = (String) taskData.get("pipeline");
			String type = (String) taskData.get("type");

			if ("view".equals(type)) {

				try {
					List<Document> existsNamespaces = this.existsNamespace(db, viewName);
					Document existsNamespace =
							existsNamespaces != null && existsNamespaces.size() > 0 ?
									existsNamespaces.get(0) : null;
					if (existsNamespace != null) {
						boolean isView = "view".equals(existsNamespace.getString("type"));

						if (isView) {

							logger.info("Drop view " + viewName + " on connection " + connectionId);
							db.getCollection(viewName).drop();
						} else {
							logger.error(viewName + " is " + existsNamespace.getString("type") + ", not view, action is canceled");
							result.setTaskResultCode(201);
							result.setTaskResult(viewName + " is " + existsNamespace.getString("type") + ", not view, action is canceled");
							return;
						}
					}

					result.setTaskResultCode(200);
					result.setTaskResult("View " + viewName + " dropd.");
					logger.info(result.getTaskResult());

					callback.accept(result);

				} catch (MongoCommandException e) {
					logger.error("Drop view error", e);
					result.setTaskResultCode(201);
					result.setTaskResult("Drop view " + viewName + " failed: " + e.getErrorCodeName() + "(" + e.getErrorCode() + "), " + e.getErrorMessage());
				}

			} else if ("mview".equals(type)) {
				// db.getCollection("").aggregate()
				logger.error("Cancel apply a user-defined MongoDB pipeline fail, not support materialized views.");
				result.setTaskResultCode(201);
				result.setTaskResult("Not support materialized views.");
			} else {
				logger.error("Cancel apply a user-defined MongoDB pipeline fail, not support pipeline type " + type);
				result.setTaskResultCode(201);
				result.setTaskResult("Not support pipeline type.");
			}

			callback.accept(result);
		} finally {
			if (mongoClient != null) {
				mongoClient.close();
			}
		}
	}

	/**
	 * 判断 命名空间 是否已存在，存在则返回已存在的
	 *
	 * @param db
	 * @param name
	 * @return
	 */
	private List<Document> existsNamespace(MongoDatabase db, String name) {
		List<Document> result = new ArrayList<>();
		Document filter = new Document();
		filter.append("name", name);
		ListCollectionsIterable<Document> collections = db.listCollections().filter(filter);
		for (Document col : collections) {
			result.add(col);
		}
		return result;
	}

	private String validateTaskData() {
		String err = "";
		Map<String, Object> taskData = context.getTaskData();

		if (MapUtils.isEmpty(taskData)) {
			err = "task_data cannot be empty.";
		} else if (!taskData.containsKey("name")) {
			err = "task_data.name cannot be empty.";
		} else if (!taskData.containsKey("source")) {
			err = "task_data.source cannot be empty.";
//		} else if (!taskData.containsKey("collection")) {
//			err = "task_data.collection cannot be empty.";
//		} else if (!taskData.containsKey("pipeline")) {
//			err = "task_data.pipeline cannot be empty.";
		} else if (!taskData.containsKey("type")) {
			err = "task_data.type cannot be empty.";
		}

		return err;
	}
}
