package io.tapdata.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.MetadataInstance;
import io.tapdata.common.JSONUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
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
@TaskType(type = "MONGODB_APPLY_PIPELINE")
public class MongoDBApplyPipeline implements Task {

	private Logger logger = LogManager.getLogger(getClass());
	private TaskContext context;

	public void initialize(TaskContext taskContext) {
		try {
			logger.info("Initialize apply pipeline task, taskData is " + JSONUtil.obj2Json(taskContext.getTaskData()));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		this.context = taskContext;
	}

	public void execute(Consumer<TaskResult> callback) {
		logger.info("Execute apply pipeline task.");
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
		String metaId = (String) taskData.get("id");
		Query query = new Query(where("_id").is((metaId)));
		List<MetadataInstance> metadataInstances = context.getClientMongoOperator().find(query, ConnectorConstant.METADATA_INSTANCE_COLLECTION, MetadataInstance.class);
		MetadataInstance metadataInstance = null;
		if (CollectionUtils.isEmpty(metadataInstances)) {
			result.setTaskResultCode(201);
			result.setTaskResult("Failed to apply pipeline, can't find mongodb metadata: " + metaId);
			callback.accept(result);
			logger.error(result.getTaskResult());
			return;
		}
		metadataInstance = metadataInstances.get(0);
		if (MapUtils.isEmpty(metadataInstance.getSource())) {
			result.setTaskResultCode(201);
			result.setTaskResult("Failed to apply pipeline, can't find mongodb connection.");
			callback.accept(result);
			logger.error(result.getTaskResult());
			return;
		}
		Connections connection = com.tapdata.constant.JSONUtil.map2POJO(metadataInstance.getSource(), Connections.class);

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
				result.setTaskResult("Can't connect to target mongodb " + connection.getId() + ".");
				callback.accept(result);
				logger.error(result.getTaskResult());
				return;
			}

			MongoDatabase db = mongoClient.getDatabase(databaseName);


			String viewName = (String) taskData.get("name");
			String viewOn = (String) taskData.get("collection");
			String pipelineJson = (String) taskData.get("pipeline");
			String type = (String) taskData.get("type");
			boolean dropIfExists = false;

			if (taskData.containsKey("dropIfExists")) {
				if (taskData.get("dropIfExists") instanceof String)
					dropIfExists = "true".equals(taskData.get("dropIfExists"));
				else if (taskData.get("dropIfExists") instanceof Boolean)
					dropIfExists = (Boolean) taskData.get("dropIfExists");
			}

			// if enable dropIfExists, drop the view before createView.
			List<Document> existsNamespaces = this.existsNamespace(db, viewName);
			Document existsNamespace =
					existsNamespaces != null && existsNamespaces.size() > 0 ?
							existsNamespaces.get(0) : null;
			if (existsNamespace != null) {
				boolean isView = "view".equals(existsNamespace.getString("type"));

				if (isView && dropIfExists) {

					logger.info("Drop view " + viewName + " on connection " + connection.getId());
					db.getCollection(viewName).drop();

				} else {
					logger.error(existsNamespace.getString("type") + " " + viewName + " exists, action is canceled");
					result.setTaskResultCode(201);
					result.setTaskResult(existsNamespace.getString("type") + " " + viewName + " exists.");
					callback.accept(result);
					return;
				}
			}


			if ("view".equals(type)) {
				// db.createView();
				List<Bson> pipeline = null;
				try {
					BsonArray bson = BsonArray.parse(pipelineJson);
					pipeline = new ArrayList<>(bson.size());
					List<Bson> finalPipeline = pipeline;
					bson.forEach(bsonValue -> finalPipeline.add(bsonValue.asDocument()));

					try {
						db.createView(viewName, viewOn, pipeline);

						result.setTaskResultCode(200);
						result.setTaskResult("View " + viewName + " created.");
						logger.info(result.getTaskResult());
					} catch (MongoCommandException e) {
						logger.error("Create view error", e);
						result.setTaskResultCode(201);
						result.setTaskResult("Create view " + viewName + " failed: " + e.getErrorCodeName() + "(" + e.getErrorCode() + "), " + e.getErrorMessage());
					}
				} catch (Exception e) {
					logger.error("Create view error.", e);
					result.setFailedResult("Create view " + viewName + " failed: " + e.getMessage());
				}

			} else if ("mview".equals(type)) {
				// db.getCollection("").aggregate()
				logger.error("Apply a user-defined MongoDB pipeline fail, not support materialized views.");
				result.setTaskResultCode(201);
				result.setTaskResult("Not support materialized views.");
			} else {
				logger.error("Apply a user-defined MongoDB pipeline fail, not support pipeline type " + type);
				result.setTaskResultCode(201);
				result.setTaskResult("Not support pipeline type.");
			}

			updatePipelineStatus(metaId, result);

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
		} else if (!taskData.containsKey("id")) {
			err = "task_data.id cannot be empty.";
		} else if (!taskData.containsKey("collection")) {
			err = "task_data.collection cannot be empty.";
		} else if (!taskData.containsKey("pipeline")) {
			err = "task_data.pipeline cannot be empty.";
		} else if (!taskData.containsKey("type")) {
			err = "task_data.type cannot be empty.";
		}

		return err;
	}

	private void updatePipelineStatus(String metaId, TaskResult taskResult) {
		if (StringUtils.isNotBlank(metaId) && taskResult != null) {
			logger.info("Update create view status, id: {}, task result: {}.", metaId, taskResult);
			try {
				MongoCollection<Document> collection = context.getClientMongoOperator().getMongoTemplate().getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION);
				Document update = new Document();

				if (taskResult.getTaskResultCode() == 200) {
					update.put("pipeline_status", "succeed");
				} else {
					update.put("pipeline_status", "failed");
					update.put("pipeline_message", taskResult.getTaskResult());
				}

				collection.updateOne(new Document("_id", new ObjectId(metaId)),
						new Document("$set", update));
			} catch (Exception e) {
				logger.error("Update create view status error: {}, id: {}, task result: {}.", e.getMessage(), metaId, taskResult, e);
			}
		} else {
			logger.error("Failed to update create view status, missing params, id: {}, task result: {}.", metaId, taskResult);
		}
	}

}
