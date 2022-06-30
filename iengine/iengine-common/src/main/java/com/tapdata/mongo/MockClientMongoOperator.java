package com.tapdata.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.BulkWriteResult;
import com.tapdata.entity.Job;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2022-02-22 16:49
 **/
public class MockClientMongoOperator extends ClientMongoOperator {

	public MockClientMongoOperator() {
	}

	public MockClientMongoOperator(MongoTemplate template, MongoClient mongoClient) {
		super(template, mongoClient);
	}

	public MockClientMongoOperator(MongoTemplate mongoTemplate, MongoClient mongoClient, MongoClientURI mongoClientURI) {
		super(mongoTemplate, mongoClient, mongoClientURI);
	}

	@Override
	public MongoTemplate getMongoTemplate() {
		throw new UnsupportedOperationException();
	}

	@Override
	public MongoClient getMongoClient() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void upsert(Map<String, Object> params, Map<String, Object> insert, String collection) {
	}

	@Override
	public <T> T upsert(Map<String, Object> params, Map<String, Object> insert, String collection, Class<T> clazz) {
		return map2Pojo(insert, clazz);
	}

	@Override
	public void delete(Map<String, Object> params, String collection) {
	}

	@Override
	public BulkOperations getBulkOperations(String collection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(Query query, String collection) {

	}

	@Override
	public void deleteAll(Map<String, Object> params, String collection) {

	}

	@Override
	public <T> T findAndModify(Map<String, Object> params, Map<String, Object> updateParams, Class<T> className, String collection) {
		return map2Pojo(updateParams, className);
	}

	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection) {
		return map2Pojo(update.getUpdateObject(), className);
	}

	@Override
	public <T> T findAndModify(Query query, Update update, Class<T> className, String collection, boolean returnNew) {
		return map2Pojo(update.getUpdateObject(), className);
	}

	@Override
	public UpdateResult updateAndParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
		return UpdateResult.acknowledged(1L, 1L, null);
	}

	@Override
	public void updateOrParam(Map<String, Object> params, Map<String, Object> updateParams, String collection) {

	}

	@Override
	public <T> List<T> find(Map<String, Object> params, String collection, Class<T> className) {
		T t = map2Pojo(params, className);
		List<T> list = new ArrayList<>();
		list.add(t);
		return list;
	}

	@Override
	public <T> T findOne(Query query, String collection, Class<T> className) {
		return map2Pojo(query.getQueryObject(), className);
	}

	@Override
	public <T> T findOne(Map<String, Object> params, String collection, Class<T> className) {
		return map2Pojo(params, className);
	}

	@Override
	public UpdateResult update(Query query, Update update, String collection) {
		return UpdateResult.acknowledged(1L, 1L, null);
	}

	@Override
	public <T> T updateById(Update update, String collection, String id, Class<T> className) {
		return map2Pojo(update.getUpdateObject(), className);
	}

	@Override
	public <T> List<T> find(Query query, String collection, Class<T> className) {
		List<T> list = new ArrayList<>();
		T t = map2Pojo(query.getQueryObject(), className);
		list.add(t);
		return list;
	}

	@Override
	public BulkWriteResult executeBulkWrite(List<WriteModel<Document>> value, BulkWriteOptions options, String collectionName, int errorRetry, long retryInteval, Job job) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public long count(Query query, String collection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long postCount(Query query, String collection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long count(Query query, String collection, Class clazz) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void pullObjectToArray(Map<String, Object> params, Map<String, Object> updateParams, String collection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createIndexes(String collection, List<IndexModel> indexModels) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createIndex(String collection, Bson keys, IndexOptions options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean existIndex(String collection, String indexName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void insertOne(Object obj, String collection) {
	}

	@Override
	public void insertList(List<?> list, String collection) {
	}

	@Override
	public void insertMany(List<?> list, String collection) {
	}

	@Override
	public void dropCollection(String collection) {
	}

	@Override
	public Set<String> getCollectionNames() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void releaseResource() {
	}

	@Override
	public MongoClientURI getMongoClientURI() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCloudRegion(String cloudRegion) {
		throw new UnsupportedOperationException();
	}

	@Override
	public GridFSBucket getGridFSBucket() {
		throw new UnsupportedOperationException();
	}

	private <T> T map2Pojo(Map<String, Object> map, Class<T> clazz) {
		T t;
		try {
			t = JSONUtil.map2POJO(map, clazz);
		} catch (Exception e) {
			throw new RuntimeException("Convert map-pojo failed, map: " + map + ", pojo type: " + clazz.getName() + "; Error: " + e.getMessage(), e);
		}
		return t;
	}
}
