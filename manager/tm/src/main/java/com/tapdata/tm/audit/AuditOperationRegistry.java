package com.tapdata.tm.audit;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 异步操作的受理登记：请求事件写入后登记 objectType+objectId → operationId，最终结果到达时原子取出，
 * 多个 TM 实例只有一个能取到，保证最终事件只写一次。登记 7 天后自动过期。
 */
@Slf4j
@Component
public class AuditOperationRegistry {
	public static final String COLLECTION = "AuditOperations";

	private final MongoTemplate mongoTemplate;

	public AuditOperationRegistry(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	public void register(String objectType, String objectId, String operationId, AuditAction action, String objectName) {
		if (objectId == null || operationId == null) {
			return;
		}
		try {
			Query query = Query.query(Criteria.where("_id").is(key(objectType, objectId)));
			Update update = new Update().set("operationId", operationId).set("action", action.getKey())
					.set("objectType", objectType).set("objectId", objectId).set("objectName", objectName)
					.set("createdAt", new Date());
			mongoTemplate.upsert(query, update, COLLECTION);
		} catch (Exception e) {
			log.warn("Register async audit operation failed, type={}, id={}", objectType, objectId, e);
		}
	}

	/** 取出并移除登记；没有登记（例如引擎自发的状态变化）时返回 null。 */
	public Pending take(String objectType, String objectId) {
		if (objectId == null) {
			return null;
		}
		try {
			Query query = Query.query(Criteria.where("_id").is(key(objectType, objectId)));
			return mongoTemplate.findAndRemove(query, Pending.class, COLLECTION);
		} catch (Exception e) {
			log.warn("Take async audit operation failed, type={}, id={}", objectType, objectId, e);
			return null;
		}
	}

	/** 只读查看登记，不移除。 */
	public Pending peek(String objectType, String objectId) {
		try {
			return mongoTemplate.findOne(Query.query(Criteria.where("_id").is(key(objectType, objectId))), Pending.class, COLLECTION);
		} catch (Exception e) {
			return null;
		}
	}

	static String key(String objectType, String objectId) {
		return objectType + ":" + objectId;
	}

	@Data
	@Document(COLLECTION)
	public static class Pending {
		@Id
		private String id;
		private String operationId;
		private String action;
		private String objectType;
		private String objectId;
		private String objectName;
		@Indexed(expireAfterSeconds = 7 * 24 * 3600)
		private Date createdAt;
	}
}
