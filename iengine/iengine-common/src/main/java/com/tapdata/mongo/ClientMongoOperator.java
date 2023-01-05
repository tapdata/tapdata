package com.tapdata.mongo;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Job;
import com.tapdata.entity.TapLog;
import com.tapdata.mongo.error.BulkWriteErrorHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ClientMongoOperator implements Serializable {

	private static final long serialVersionUID = -5292522598032369977L;
	private Logger logger = LogManager.getLogger(getClass());

	private MongoTemplate mongoTemplate;

	private MongoClient mongoClient;

	private MongoClientURI mongoClientURI;

	private static final String MONGODB_DUPLICATE_ERROR_STRING = "E11000 duplicate key error";

	protected String cloudRegion;

	public ClientMongoOperator() {
	}

	public ClientMongoOperator(MongoTemplate template, MongoClient mongoClient) {
		this.mongoTemplate = template;
		this.mongoClient = mongoClient;
	}

	public ClientMongoOperator(MongoTemplate mongoTemplate, MongoClient mongoClient, MongoClientURI mongoClientURI) {
		this.mongoTemplate = mongoTemplate;
		this.mongoClient = mongoClient;
		this.mongoClientURI = mongoClientURI;
	}

	public MongoTemplate getMongoTemplate() {
		return mongoTemplate;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void upsert(Map<String, Object> params, Map<String, Object> insert, String collection) {
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(insert);
		mongoTemplate.upsert(query, update, collection);
	}

	public <T> T upsert(Map<String, Object> params, Map<String, Object> insert, String collection, Class<T> clazz) {
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(insert);
		mongoTemplate.upsert(query, update, collection);

		T t = mongoTemplate.findOne(new Query(getAndCriteria(params)), clazz);
		return t;
	}

	public void delete(Map<String, Object> params, String collection) {
		Query query = new Query(getAndCriteria(params));
		mongoTemplate.remove(query, collection);
	}

	public BulkOperations getBulkOperations(String collection) {
		BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collection);
		return bulkOperations;
	}

	public void delete(Query query, String collection) {
		mongoTemplate.remove(query, collection);
	}

	public void deleteByMap(Map<String, Object> params, String collection) {
		mongoTemplate.remove(params, collection);
	}

	public void deleteAll(Map<String, Object> params, String collection) {
		if (MapUtils.isNotEmpty(params)) {
			mongoTemplate.remove(new Query(getAndCriteria(params)));
		}
	}

	public <T> T findAndModify(Map<String, Object> params, Map<String, Object> updateParams, Class<T> className, String collection) {
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(updateParams);
		preModifyJobColl(update, collection);
		T modify = mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), className, collection);
		return modify;
	}

	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection) {
		preModifyJobColl(update, collection);
		T modify = mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), className, collection);
		return modify;
	}

	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection, boolean returnNew) {
		preModifyJobColl(update, collection);
		T modify = mongoTemplate.findAndModify(query, update, new FindAndModifyOptions().returnNew(returnNew), className, collection);
		return modify;
	}


	public UpdateResult updateAndParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(updateParams);
		preModifyJobColl(update, collection);
		return mongoTemplate.updateFirst(query, update, collection);
	}

	public void updateOrParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
		Query query = new Query(getAndCriteria(params));
		Update update = getUpdate(updateParams);

		mongoTemplate.updateMulti(query, update, collection);
	}

	public <T> List<T> find(Map<String, Object> params, String collection, Class<T> className) {
		Query query = new Query(getAndCriteria(params));
		return mongoTemplate.find(query, className, collection);
	}

	public <T> T findOne(Query query, String collection, Class<T> className) {
		return mongoTemplate.findOne(query, className, collection);
	}

	public <T> T findOne(Query query, String collection, Class<T> className, Predicate<?> stop) {
		return mongoTemplate.findOne(query, className, collection);
	}

	public <T> T findOne(Map<String, Object> params, String collection, Class<T> className) {
		Criteria criteria = new Criteria();
		params.forEach((k, v) -> criteria.and(k).is(v));
		Query query = new Query(criteria);
		return mongoTemplate.findOne(query, className, collection);
	}

	public UpdateResult update(Query query, Update update, String collection) {
		preModifyJobColl(update, collection);
		return mongoTemplate.updateFirst(query, update, collection);
	}

	public <T> T updateById(Update update, String collection, String id, Class<T> className) {
		Query query = new Query(where("_id").is(new ObjectId(id)));
		return mongoTemplate.findAndModify(query, update, className, collection);
	}

	public <T> List<T> find(Query query, String collection, Class<T> className) {
		return mongoTemplate.find(query, className, collection);
	}

	public <T> List<T> find(Query query, String collection, Class<T> className, Predicate<?> stop) {
		return mongoTemplate.find(query, className, collection);
	}

	public com.tapdata.entity.BulkWriteResult executeBulkWrite(List<WriteModel<Document>> value,
															   BulkWriteOptions options,
															   String collectionName,
															   int errorRetry,
															   long retryInteval,
															   Job job) throws Exception {
		Boolean stopOnError = job.getStopOnError();

		MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);

		com.tapdata.entity.BulkWriteResult result = new com.tapdata.entity.BulkWriteResult();
		BulkWriteResult bulkWriteResult = BulkWriteResult.acknowledged(0, 0, 0, 0, new ArrayList<>());
		int retry = 0;
		while (CollectionUtils.isNotEmpty(value)) {
			try {
				if (options != null) {
					BulkWriteResult writeResult = collection.bulkWrite(value, options);
					bulkWriteResult = statsBulkResult(bulkWriteResult, writeResult);
				} else {
					BulkWriteResult writeResult = collection.bulkWrite(value);
					bulkWriteResult = statsBulkResult(bulkWriteResult, writeResult);
				}
				break;
			} catch (MongoSocketException |
					 MongoNotPrimaryException |
					 MongoClientException |
					 MongoNodeIsRecoveringException |
					 MongoInterruptedException e) {

				retry = retryNetworkException(retryInteval, errorRetry, retry, e);

			} catch (MongoBulkWriteException e) {
				boolean errorHandleResult = handleBulkWriteError(e, value, options);
				if (!errorHandleResult) {
					value = continueExecuteBulkWrite(collectionName, value, options, stopOnError, e);
					// stats all bulk result
					BulkWriteResult writeResult = e.getWriteResult();
					bulkWriteResult = statsBulkResult(bulkWriteResult, writeResult);
				}
			} catch (Exception e) {
				throw new RuntimeException(
						String.format(
								"Bulk write to collection %s failed %s, %s",
								collectionName,
								e.getMessage(),
								CollectionUtils.isNotEmpty(value) ? value.get(0) : null
						),
						e
				);
			}
		}

		result.setBulkWriteResult(bulkWriteResult);

		return result;
	}

	private BulkWriteResult statsBulkResult(BulkWriteResult totalWriteResult, BulkWriteResult writeResult) {
		if (totalWriteResult == null || !totalWriteResult.wasAcknowledged()) {
			totalWriteResult = writeResult;
		} else {
			int deletedCount = writeResult.getDeletedCount() + totalWriteResult.getDeletedCount();
			int insertedCount = writeResult.getInsertedCount() + totalWriteResult.getInsertedCount();
			int modifiedCount = writeResult.getModifiedCount() + totalWriteResult.getModifiedCount();
			int matchedCount = writeResult.getMatchedCount() + totalWriteResult.getMatchedCount();
			List<BulkWriteUpsert> bulkWriteUpserts = totalWriteResult.getUpserts() == null ? new ArrayList<>() : totalWriteResult.getUpserts();
			if (CollectionUtils.isNotEmpty(writeResult.getUpserts())) {
				bulkWriteUpserts.addAll(writeResult.getUpserts());
			}
			totalWriteResult = BulkWriteResult.acknowledged(insertedCount, matchedCount, deletedCount, modifiedCount, bulkWriteUpserts);
		}
		return totalWriteResult;
	}

	private List<WriteModel<Document>> continueExecuteBulkWrite(String collectionName, List<WriteModel<Document>> value, BulkWriteOptions options, Boolean stopOnError, MongoBulkWriteException e) {

		List<BulkWriteError> bulkWriteErrors = e.getWriteErrors();

		if (CollectionUtils.isNotEmpty(value) && CollectionUtils.isNotEmpty(bulkWriteErrors)) {

			if (stopOnError) {
				throw e;
			}

			int size = value.size();
			int errorSize = bulkWriteErrors.size();

			if ((options == null || options.isOrdered()) && errorSize < size) {
				if (CollectionUtils.isNotEmpty(bulkWriteErrors)) {
					BulkWriteError bulkWriteError = bulkWriteErrors.get(0);
					int index = bulkWriteError.getIndex();
					WriteModel<Document> errorWrite = value.get(index);
					value = index + 1 < size ? value.subList(index + 1, size) : null;
					logger.warn(
							TapLog.TRAN_ERROR_0028.getMsg(),
							collectionName,
							e.getMessage(),
							size,
							bulkWriteErrors.size(),
							value == null ? 0 : value.size(),
							errorWrite
					);
				} else {
					logger.warn(TapLog.TRAN_ERROR_0014.getMsg(), collectionName, e.getMessage());
				}

			} else {
				String message = e.getMessage();
				if (StringUtils.containsIgnoreCase(message, MONGODB_DUPLICATE_ERROR_STRING)) {
					logger.warn(TapLog.W_TRAN_LOG_0004.getMsg(), collectionName, size, size - errorSize, errorSize, message.length() > 500 ? message.substring(0, 500) : message, e);
				} else {
					logger.error(TapLog.TRAN_ERROR_0013.getMsg(), collectionName, message.length() > 500 ? message.substring(0, 500) : message, size, size - errorSize, errorSize, e);
				}
				value = null;
			}
		} else {
			if (options != null && !options.isOrdered()) {
				value = null;
			}
		}
		return value;
	}

	private int retryNetworkException(long retryInteval, int errorRetry, int retry, MongoException e) throws Exception {
		retry++;

		if (retry > errorRetry) {
			throw new Exception(e);
		}
		logger.warn(TapLog.TRAN_ERROR_0012.getMsg(), e.getMessage(), retryInteval, e);
		try {
			Thread.sleep(retryInteval);
		} catch (InterruptedException e1) {
			//abort
		}
		return retry;
	}

	public long count(Query query, String collection) {
		return count(query, collection, null);
	}

	public long postCount(Query query, String collection) {
		return count(query, collection, null);
	}

	public long count(Query query, String collection, Class clazz) {
		return mongoTemplate.count(query, collection);
	}

	public void pullObjectToArray(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
		Query query = new Query(getAndCriteria(params));
		Update update = new Update();
		updateParams.forEach((key, value) -> {
			update.push(key, value);
		});
		preModifyJobColl(update, collection);
		mongoTemplate.updateFirst(query, update, collection);

	}

	private Criteria getAndCriteria(Map<String, Object> queryParam) {
		Criteria criteria = new Criteria();
		if (queryParam != null && !queryParam.isEmpty()) {
			queryParam.forEach((s, o) -> criteria.and(s).is(o));
		}
		return criteria;
	}

	private Criteria getOrCriteria(Map<String, Object> queryParam) {
		Criteria criteria = new Criteria();
		if (queryParam != null && !queryParam.isEmpty()) {
			queryParam.forEach((s, o) -> criteria.in(o));
		}
		return criteria;
	}

	public void createIndexes(String collection, List<IndexModel> indexModels) {
		MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
		mongoCollection.createIndexes(indexModels);
	}

	public void createIndex(String collection, Bson keys, IndexOptions options) {
		MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
		mongoCollection.createIndex(keys, options);
	}

	public boolean existIndex(String collection, String indexName) {
		MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collection);
		for (Document doc : mongoCollection.listIndexes()) {
			if (indexName.equalsIgnoreCase(doc.getString("name"))) {
				return true;
			}
		}
		return false;
	}

	public void insertOne(Object obj, String collection) {
		mongoTemplate.insert(obj, collection);
	}

	public void insertList(List<? extends Object> list, String collection) {
		mongoTemplate.insert(list, collection);
	}

	public void insertMany(List<? extends Object> list, String collection) {
		mongoTemplate.insert(list, collection);
	}

	public void insertMany(List<? extends Object> list, String collection, Predicate<?> stop) {
		mongoTemplate.insert(list, collection);
	}

	public void batch(List<? extends Object> list, String collection, Predicate<?> stop) {
		throw new UnsupportedOperationException();
	}

	public void dropCollection(String collection) {
		mongoTemplate.dropCollection(collection);
	}

	public Set<String> getCollectionNames() {
		return mongoTemplate.getCollectionNames();
	}

	private Update getUpdate(Map<String, Object> insert) {
		Update update = new Update();
		if (insert != null && !insert.isEmpty()) {
			insert.forEach((key, value) -> {
				update.set(key, value);
			});
		}
		return update;
	}

	public void releaseResource() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	private Update preModifyJobColl(Update update, String collection) {
		if (ConnectorConstant.JOB_COLLECTION.equals(collection)) {
			update.set("last_update", System.currentTimeMillis());
		}
		return update;
	}

	public MongoClientURI getMongoClientURI() {
		return mongoClientURI;
	}

	public void setCloudRegion(String cloudRegion) {
		this.cloudRegion = cloudRegion;
	}

	private boolean handleBulkWriteError(MongoBulkWriteException e, List<WriteModel<Document>> writeModels, BulkWriteOptions bulkWriteOptions) throws Exception {
		List<BulkWriteError> writeErrors = e.getWriteErrors();

		for (BulkWriteError writeError : writeErrors) {
			BulkWriteErrorRetrievableCode bulkWriteErrorRetrievableCode = BulkWriteErrorRetrievableCode.fromCode(writeError.getCode());
			if (bulkWriteErrorRetrievableCode == null) {
				return false;
			}
			String implementClass = bulkWriteErrorRetrievableCode.getImplementClass();
			Class<?> errorHandlerClazz = Class.forName(implementClass);
			Object errorHandler = errorHandlerClazz.newInstance();
			if (!(errorHandler instanceof BulkWriteErrorHandler)) {
				return false;
			}
			boolean result = ((BulkWriteErrorHandler) errorHandler).handle(writeModels.get(writeError.getIndex()), writeError);
			if (!result) {
				return false;
			}
		}

		if (bulkWriteOptions == null || bulkWriteOptions.isOrdered()) {
			int index = writeErrors.get(0).getIndex();
			writeModels = writeModels.subList(index, writeModels.size());
		}
		return true;
	}

	enum BulkWriteErrorRetrievableCode {
		ERROR_28(28, "Cannot create field '.*' in element \\{.*: null\\}", "com.tapdata.mongo.error.handler.Code28Handler"),
		;
		private int code;
		private String errorMessageRegex;
		private String implementClass;

		private static Map<Integer, BulkWriteErrorRetrievableCode> codeMap = new HashMap<>();

		static {
			for (BulkWriteErrorRetrievableCode value : BulkWriteErrorRetrievableCode.values()) {
				codeMap.put(value.getCode(), value);
			}
		}

		BulkWriteErrorRetrievableCode(int code, String errorMessageRegex, String implementClass) {
			this.code = code;
			this.errorMessageRegex = errorMessageRegex;
			this.implementClass = implementClass;
		}

		public static BulkWriteErrorRetrievableCode fromCode(int code) {
			return codeMap.get(code);
		}

		public int getCode() {
			return code;
		}

		public String getErrorMessageRegex() {
			return errorMessageRegex;
		}

		public String getImplementClass() {
			return implementClass;
		}
	}

	public GridFSBucket getGridFSBucket() {
		MongoDatabase mongoDatabase = mongoTemplate.getDb();
		return GridFSBuckets.create(mongoDatabase);
	}

	public <T> T postOne(Map<String, Object> obj, String resource, Class<T> className) {
		throw new UnsupportedOperationException();
	}
}
