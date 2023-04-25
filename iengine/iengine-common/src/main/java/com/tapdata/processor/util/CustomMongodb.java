package com.tapdata.processor.util;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.MongodbUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.voovan.tools.collection.CacheMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomMongodb {

	private static final Logger logger = LogManager.getLogger(CustomMongodb.class);

	private static final CacheMap<String, MongoClient> cacheMap = new CacheMap<>();

	static {
		cacheMap.supplier(MongodbUtil::createMongoClient)
				.destory((mongodbUri, mongoClient) -> {
					mongoClient.close();
					logger.info("{} mongoClient close", mongodbUri);
					return -1L;
				})
				//连接失效被清理
				.autoRemove(true)
				//最大连接100个
				.maxSize(100)
				//每60s检查一次
				.interval(60)
				//连接空闲5min后被回收
				.expire(300)
				.create();
	}

	public static List<Map<String, Object>> getData(String mongodbUri, String collection, Map<String, Object> filter, int limit, Map<String, Object> sort) {
		if (StringUtils.isBlank(mongodbUri) || StringUtils.isBlank(MongodbUtil.getDatabase(mongodbUri)) || StringUtils.isBlank(collection)) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.getData missing mongodb uri/database name/collection, mongodb uri=%s, database name=%s, collection=%s, inserts=%s",
					mongodbUri,
					MongodbUtil.getDatabase(mongodbUri),
					collection,
					filter));
		}

		Document filterDocument = filter == null ? new Document() : new Document(filter);
		Document sortDocument = sort == null ? new Document() : new Document(sort);

		MongoClient mongoClient = cacheMap.getAndRefresh(mongodbUri);
		FindIterable<Document> documents = mongoClient.getDatabase(MongodbUtil.getDatabase(mongodbUri)).getCollection(collection).find(filterDocument);
		if (limit > 0) {
			documents.limit(limit);
		}

		if (MapUtils.isNotEmpty(sort)) {
			documents.sort(sortDocument);
		}
		try (
				MongoCursor<Document> mongoCursor = documents.iterator()
		) {
			List<Map<String, Object>> resultList = new ArrayList<>();
			while (mongoCursor.hasNext()) {
				resultList.add(mongoCursor.next());
			}

			return resultList;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.getData: %s", e.getMessage()), e);
		}
	}

	public static List<Map<String, Object>> getData(String mongodbUri, String collection) {
		return getData(mongodbUri, collection, null, -1, new HashMap<>());
	}

	public static List<Map<String, Object>> getData(String mongodbUri, String collection, Map<String, Object> filter) {
		return getData(mongodbUri, collection, filter, -1, new HashMap<>());
	}

	public static void insert(String mongodbUri, String collection, Map<String, Object> inserts) {
		insert(mongodbUri, collection, Arrays.asList(inserts));
	}

	public static void insert(String mongodbUri, String collection, List<Map<String, Object>> inserts) {
		if (StringUtils.isBlank(mongodbUri) || StringUtils.isBlank(MongodbUtil.getDatabase(mongodbUri)) || StringUtils.isBlank(collection)) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.insert missing mongodb uri/database name/collection, mongodb uri=%s, database name=%s, collection=%s, inserts=%s",
					mongodbUri,
					MongodbUtil.getDatabase(mongodbUri),
					collection,
					inserts.get(0)));
		}

		if (CollectionUtils.isEmpty(inserts)) return;
		List<Document> insertDocs = new ArrayList<>();
		for (Map<String, Object> insert : inserts) {
			if (MapUtils.isEmpty(insert)) continue;
			insertDocs.add(new Document(insert));
		}

		try {
			MongoClient mongoClient = cacheMap.getAndRefresh(mongodbUri);
			MongoDatabase database = mongoClient.getDatabase(MongodbUtil.getDatabase(mongodbUri));
			MongoCollection<Document> mongoCollection = database.getCollection(collection);
			mongoCollection.insertMany(insertDocs);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.insert: %s", e.getMessage()), e);
		}
	}

	public static long update(String mongodbUri, String collection, Map<String, Object> filter, Map<String, Object> update) {
		if (StringUtils.isBlank(mongodbUri) || StringUtils.isBlank(MongodbUtil.getDatabase(mongodbUri)) || StringUtils.isBlank(collection) || MapUtils.isEmpty(update)) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.update missing mongodb uri/database name/collection/update, mongodb uri=%s, database name=%s, collection=%s, filter=%s, update=%s",
					mongodbUri,
					MongodbUtil.getDatabase(mongodbUri),
					collection,
					filter,
					update));
		}

		Document filterDocument = filter == null ? new Document() : new Document(filter);
		Document updateDocument = new Document(update);
		Set<String> keySet = updateDocument.keySet();
		String firstKey = keySet.iterator().next();
		if (!firstKey.startsWith("$")) {
			updateDocument = new Document("$set", updateDocument);
		}

		try {
			MongoClient mongoClient = cacheMap.getAndRefresh(mongodbUri);
			UpdateResult updateResult = mongoClient.getDatabase(MongodbUtil.getDatabase(mongodbUri))
					.getCollection(collection)
					.updateMany(filterDocument, updateDocument);

			return updateResult.getModifiedCount();

		} catch (Exception e) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.update: %s", e.getMessage()), e);
		}
	}

	public static long delete(String mongodbUri, String collection, Map<String, Object> filter) {
		if (StringUtils.isBlank(mongodbUri) || StringUtils.isBlank(MongodbUtil.getDatabase(mongodbUri)) || StringUtils.isBlank(collection)) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.delete missing mongodb uri/database name/collection, mongodb uri=%s, database name=%s, collection=%s, inserts=%s",
					mongodbUri,
					MongodbUtil.getDatabase(mongodbUri),
					collection,
					filter));
		}

		Document filterDocument = filter == null ? new Document() : new Document(filter);

		try {
			MongoClient mongoClient = cacheMap.getAndRefresh(mongodbUri);
			DeleteResult deleteResult = mongoClient.getDatabase(MongodbUtil.getDatabase(mongodbUri))
					.getCollection(collection)
					.deleteMany(filterDocument);

			return deleteResult.getDeletedCount();

		} catch (Exception e) {
			throw new RuntimeException(String.format("Custom connection run script error, mongo.delete: %s", e.getMessage()), e);
		}
	}
}
