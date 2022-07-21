package com.tapdata.processor;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


public class MongoDBScriptConnection implements ScriptConnection {

	protected Logger logger = LogManager.getLogger(getClass());

	protected MongoClient mongoClient;

	private Connections connections;

	protected String defaultDatabaseName;

	private AtomicBoolean isOpen = new AtomicBoolean(true);

	@Override
	public void initialize(Connections connections) {
		try {
			mongoClient = MongodbUtil.createMongoClient(connections, MongodbUtil.getForJavaCoedcRegistry());

			isOpen.compareAndSet(false, true);
			this.connections = connections;

			defaultDatabaseName = MongodbUtil.getDatabase(connections);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(String.format("Init mongodb script connection {} failed {}", connections.getName(), e.getMessage()), e);
		}
	}

	@Override
	public Connections getConnections() {
		return connections;
	}

	@Override
	public long execute(Map<String, Object> executeObj) {

		long resultRows = 0;

		if (MapUtils.isNotEmpty(executeObj)) {
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewMap(executeObj, newMap);

			ExecuteObject executeObject = new ExecuteObject(newMap);
			String op = executeObject.getOp();
			if (StringUtils.isBlank(op)) {
				throw new RuntimeException("Mapping javascript process failed, op cannot be blank");
			}

			String database = StringUtils.isBlank(executeObject.getDatabase()) ? defaultDatabaseName : executeObject.getDatabase();
			String collection = executeObject.getCollection();
			if (StringUtils.isBlank(collection)) {
				throw new RuntimeException("Mapping javascript process failed, collection Name cannot be blank");
			}
			Map<String, Object> opObject = executeObject.getOpObject();
			Map<String, Object> filter = executeObject.getFilter();
			try {
				switch (op) {
					case ExecuteObject.INSERT_OP:

						if (MapUtils.isEmpty(opObject)) {
							throw new RuntimeException("Mapping javascript process failed, opObject cannot be empty for insert operation");
						}

						mongoClient.getDatabase(database).getCollection(collection).insertOne(new Document(opObject));
						resultRows = 1;
						break;
					case ExecuteObject.DELETE_OP:
						if (MapUtils.isEmpty(filter)) {
							throw new RuntimeException("Mapping javascript process failed, filter cannot be empty for delete operation");
						}

						DeleteResult deleteResult = mongoClient.getDatabase(database).getCollection(collection).deleteMany(new Document(filter));
						resultRows = deleteResult.getDeletedCount();
						break;
					case ExecuteObject.UPDATE_OP:

						if (MapUtils.isEmpty(opObject)) {
							throw new RuntimeException("Mapping javascript process failed, opObject cannot be empty for update operation");
						}

						if (MapUtils.isEmpty(filter)) {
							throw new RuntimeException("Mapping javascript process failed, filter cannot be empty for update operation");
						}
						UpdateResult updateResult;
						UpdateOptions options = new UpdateOptions().upsert(executeObject.isUpsert());

						final Document updateDoc = new Document();
						if (opObject.keySet().stream().anyMatch(u -> u.startsWith("$"))) {
							updateDoc.putAll(opObject);
						} else {
							updateDoc.append("$set", new Document(opObject));
						}
						if (executeObject.isMulti()) {
							updateResult = mongoClient.getDatabase(database).getCollection(collection).updateMany(new Document(filter), updateDoc, options);
						} else {
							updateResult = mongoClient.getDatabase(database).getCollection(collection).updateOne(new Document(filter), updateDoc, options);
						}
						if (updateResult != null) {
							resultRows = updateResult.getModifiedCount();
						}
						break;
					default:
						throw new RuntimeException(String.format("Mapping javascript process failed, unsupported this op %s", op));
				}
			} catch (Exception e) {
				throw new RuntimeException(String.format("Mapping javascript process for connection %s failed %s", connections.getName(), e.getMessage()), e);
			}
		} else {
			throw new RuntimeException("Mapping javascript process failed, execute object can not be null");
		}

		return resultRows;
	}

	@Override
	public List<Map<String, Object>> executeQuery(Map<String, Object> executeObj) {

		if (MapUtils.isNotEmpty(executeObj)) {

			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewMap(executeObj, newMap);

			ExecuteObject executeObject = new ExecuteObject(newMap);
			return executeQuery(executeObject);
		} else {
			throw new RuntimeException(String.format("Mapping javascript process execute %s failed, execute object can not be null", executeObj));
		}

	}

	public List<Map<String, Object>> executeQuery(ExecuteObject executeObject) {

		String database = StringUtils.isBlank(executeObject.getDatabase()) ? defaultDatabaseName : executeObject.getDatabase();
		String collection = executeObject.getCollection();
		if (StringUtils.isBlank(collection)) {
			throw new RuntimeException(String.format("Mapping javascript process execute %s failed, collection Name cannot be blank", executeObject));
		}
		Map<String, Object> filter = executeObject.getFilter();
		Document filterDocument = filter == null ? new Document() : new Document(filter);

		Map<String, Object> projection = executeObject.getProjection();
		Document projectionDocument = projection == null ? new Document() : new Document(projection);
		FindIterable<Document> findIterable = mongoClient.getDatabase(database).getCollection(collection).find(filterDocument).projection(projectionDocument);
		Map<String, Object> sort = executeObject.getSort();
		if (MapUtils.isNotEmpty(sort)) {
			Document sortDocument = new Document(sort);
			findIterable.sort(sortDocument);
		}

		int limit = executeObject.getLimit();
		if (limit > 0) {
			findIterable.limit(limit);
		}

		try (MongoCursor<Document> mongoCursor = findIterable.iterator()) {

			List<Map<String, Object>> resultList = new ArrayList<>();
			while (mongoCursor.hasNext()) {
				resultList.add(mongoCursor.next());
			}

			return resultList;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Mapping javascript process execute %s for connection %s failed %s", executeObject, connections.getName(), e.getMessage()), e);
		}
	}

	public long count(Map<String, Object> parameters) {
		ExecuteObject executeObject = new ExecuteObject(parameters);
		String database = StringUtils.isBlank(executeObject.getDatabase()) ? defaultDatabaseName : executeObject.getDatabase();
		String collection = executeObject.getCollection();
		if (StringUtils.isBlank(collection)) {
			throw new RuntimeException(String.format("Mapping javascript process count %s failed, collection Name cannot be blank", executeObject));
		}
		Map<String, Object> filter = executeObject.getFilter();
		Document filterDocument = filter == null ? new Document() : new Document(filter);

		return mongoClient.getDatabase(database).getCollection(collection).count(filterDocument);
	}

	@Override
	public Object call(String funcName, List<Map<String, Object>> params) {
		return null;
	}

	@Override
	public void close() {
		MongodbUtil.releaseConnection(mongoClient, null);
		isOpen.compareAndSet(true, false);
	}

	@Override
	public boolean isClosed() {
		return !isOpen.get();
	}
}
