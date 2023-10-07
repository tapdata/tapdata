package com.tapdata.constant;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2020-08-26 21:35
 **/
public class MongodbLookupUtil {

	private static Logger logger = LogManager.getLogger(MongodbLookupUtil.class);

	public static void lookUpAndSaveDeleteMessage(List<MessageEntity> messageEntities,
												  Job job,
												  ClientMongoOperator clientMongoOperator,
												  ClientMongoOperator targetMongoOperator) throws Exception {
		String validate = lookupAndSaveValidate(messageEntities, job, clientMongoOperator, targetMongoOperator);
		if (StringUtils.isNotBlank(validate)) {
			throw new IllegalArgumentException(validate);
		}

		String mongodbUri = MongodbUtil.getSimpleMongodbUri(targetMongoOperator.getMongoClientURI());
		if (StringUtils.isBlank(mongodbUri)) {
			throw new Exception(String.format("Cannot get mongodb uri from target connection, connection uri: %s", targetMongoOperator.getMongoClientURI()));
		}

		// look up oneone delete documents
		List<Document> lookupDocuments = new ArrayList<>();
		lookupDocuments(
				messageEntities,
				targetMongoOperator,
				messageEntity -> ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageEntity.getOp()),
				lookupDocument -> {
					Document mongodbDeleteCache = new Document()
							.append("mongodbUri", mongodbUri)
							.append("collectionName", lookupDocument.getCollectionName())
							.append("timestamp", new Date())
							.append("data",
									CollectionUtils.isNotEmpty(lookupDocument.getData()) ? lookupDocument.getData().get(0) : null
							);
					lookupDocuments.add(mongodbDeleteCache);
				},
				0
		);

		if (CollectionUtils.isEmpty(lookupDocuments)) {
			return;
		}

		// write into tapdata mongodb
		try {
			clientMongoOperator.getMongoTemplate().getDb().getCollection(ConnectorConstant.DELETE_CACHE_COLLECTION).insertMany(lookupDocuments);
//      clientMongoOperator.insertMany(lookupDocuments, ConnectorConstant.DELETE_CACHE_COLLECTION+"/batch");
		} catch (Exception e) {
			logger.warn("Write mongodb delete cache failed; " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
		}
	}

	/**
	 * 根据事件反查出mongodb中与之相关的所有记录
	 *
	 * @param messageEntities
	 * @param mongoOperator
	 * @param lookupPredicate
	 * @param lookupDocumentConsumer
	 * @param batchSize              按批返回，小于等于0：一次返回所有数据；大于0：按批返回数据
	 */
	public static void lookupDocuments(
			List<MessageEntity> messageEntities,
			ClientMongoOperator mongoOperator,
			Predicate<MessageEntity> lookupPredicate,
			Consumer<LookupDocument> lookupDocumentConsumer,
			int batchSize
	) {

		for (MessageEntity messageEntity : messageEntities) {
			lookupDocuments(messageEntity, mongoOperator, lookupPredicate, lookupDocumentConsumer, batchSize);
		}
	}

	public static void lookupDocuments(MessageEntity messageEntity, ClientMongoOperator mongoOperator, Predicate<MessageEntity> lookupPredicate, Consumer<LookupDocument> lookupDocumentConsumer, int batchSize) {
		if (!lookupPredicate.test(messageEntity)) {
			return;
		}

		Mapping mapping = messageEntity.getMapping();
		if (mapping == null) {
			return;
		}

		Map<String, Object> dataRow = MapUtils.isNotEmpty(messageEntity.getAfter()) ? messageEntity.getAfter() : messageEntity.getBefore();
		if (MapUtils.isEmpty(dataRow)) {
			return;
		}

		String toTable = mapping.getTo_table();
		MongoCollection<Document> collection = mongoOperator.getMongoTemplate().getCollection(toTable);
		String relationship = mapping.getRelationship();
		List<Map<String, String>> condition = new ArrayList<>();
		switch (relationship) {
			case ConnectorConstant.RELATIONSHIP_ONE_ONE:
			case ConnectorConstant.RELATIONSHIP_ONE_MANY:
				condition = mapping.getJoin_condition();
				break;
			default:
				break;
		}

		if (CollectionUtils.isEmpty(condition)) {
			return;
		}

		Document lookupFilter = new Document();
		condition.forEach(map -> map.forEach((k, v) -> {
			Object value = MapUtil.getValueByKey(dataRow, v);
			lookupFilter.append(k, value);
		}));

		List lookupData = new ArrayList<>();
		try (final MongoCursor<Document> mongoCursor = collection.find(lookupFilter).noCursorTimeout(true).iterator()) {
			while (mongoCursor.hasNext()) {
				lookupData.add(mongoCursor.next());
				if (batchSize > 0 && lookupData.size() >= batchSize) {
					lookupDocumentConsumer.accept(
							new LookupDocument(lookupData, toTable, messageEntity)
					);
					lookupData.clear();
				}
			}
		}

		if (CollectionUtils.isNotEmpty(lookupData)) {
			lookupDocumentConsumer.accept(
					new LookupDocument(lookupData, toTable, messageEntity)
			);
		}
	}

	private static String lookupAndSaveValidate(List<MessageEntity> messageEntities,
												Job job,
												ClientMongoOperator clientMongoOperator,
												ClientMongoOperator targetMongoOperator) {
		if (CollectionUtils.isEmpty(messageEntities)) {
			return "Missing input args messages";
		}

		if (job == null) {
			return "Missing input args job";
		}

		if (clientMongoOperator == null) {
			return "Missing input args client mongo operator";
		}

		if (targetMongoOperator == null) {
			return "Missing input args target mongo operator";
		}

		return "";
	}

	public static Map findDeleteCacheByOid(Connections connections, String collectionName, Object id, ClientMongoOperator clientMongoOperator) throws Exception {
		Map<String, Object> data = new HashMap();

		String validate = findValidate(connections, collectionName, id, clientMongoOperator);
		if (StringUtils.isNotBlank(validate)) {
			throw new IllegalArgumentException(validate);
		}

		String mongodbUri = MongodbUtil.getSimpleMongodbUri(connections);

		Document filter = new Document()
				.append("mongodbUri", mongodbUri)
				.append("collectionName", collectionName)
				.append("data._id", id);
		FindIterable<Document> iterable = clientMongoOperator.getMongoTemplate().getDb().getCollection(ConnectorConstant.DELETE_CACHE_COLLECTION)
				.find(filter).sort(new Document("timestamp", -1)).limit(1);

		for (Document document : iterable) {
			Document deletedRecord = document.get("data", Document.class);
			if (MapUtils.isEmpty(deletedRecord)) {
				continue;
			}
			data.putAll(deletedRecord);
			break;
		}

		return data;
	}

	public static Document lookupByChangeStreamDocument(ChangeStreamDocument<Document> changeStreamDocument, MongoClient mongoClient) {
		Document result = null;
		if (changeStreamDocument == null) {
			return result;
		}
		final MongoNamespace namespace = changeStreamDocument.getNamespace();
		if (namespace == null) {
			return result;
		}
		final BsonDocument documentKey = changeStreamDocument.getDocumentKey();
		if (documentKey == null) {
			return null;
		}

		final String collectionName = namespace.getCollectionName();
		final String databaseName = namespace.getDatabaseName();
		final MongoCursor<Document> documentMongoCursor = mongoClient
				.getDatabase(databaseName)
				.getCollection(collectionName)
				.find(
						new Document(
								"_id", documentKey.getObjectId("_id")
						)
				)
				.iterator();

		while (documentMongoCursor.hasNext()) {
			result = documentMongoCursor.next();
		}

		return result;
	}

	private static String findValidate(Connections connections, String collectionName, Object oid, ClientMongoOperator clientMongoOperator) {
		if (connections == null) {
			return "Missing input args connection";
		}

		if (StringUtils.isBlank(connections.getDatabase_uri())) {
			return "Mongodb uri cannot be empty";
		}

		if (StringUtils.isBlank(collectionName)) {
			return "Missing input args collectionName";
		}

		if (oid == null) {
			return "Missing input args ObjectId";
		}

		if (clientMongoOperator == null) {
			return "Missing input args client mongo operator";
		}

		return "";
	}

	public static class LookupDocument {

		private List<Map<String, Object>> data;

		private String collectionName;

		private MessageEntity messageEntity;

		public LookupDocument(List<Map<String, Object>> data, String collectionName, MessageEntity messageEntity) {
			this.data = data;
			this.collectionName = collectionName;
			this.messageEntity = messageEntity;
		}

		public List<Map<String, Object>> getData() {
			return data;
		}

		public void setData(List<Map<String, Object>> data) {
			this.data = data;
		}

		public String getCollectionName() {
			return collectionName;
		}

		public void setCollectionName(String collectionName) {
			this.collectionName = collectionName;
		}

		public MessageEntity getMessageEntity() {
			return messageEntity;
		}

		public void setMessageEntity(MessageEntity messageEntity) {
			this.messageEntity = messageEntity;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("LookupDocument{");
			sb.append("data=").append(data);
			sb.append(", collectionName='").append(collectionName).append('\'');
			sb.append('}');
			return sb.toString();
		}
	}
}
