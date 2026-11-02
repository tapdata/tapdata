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

	/**
	 * 仅在没有既有登记时登记；已有登记属于更早的、仍待完成的操作，
	 * 被新请求覆盖会让前一次操作失去 FINAL 归属（TAP-13018 review P2）。
	 *
	 * @return 本次是否成功占有登记
	 */
	public boolean registerIfAbsent(String objectType, String objectId, String operationId, AuditAction action, String objectName) {
		if (objectId == null || operationId == null) {
			return false;
		}
		try {
			if (peek(objectType, objectId) != null) {
				return false;
			}
			Query query = Query.query(Criteria.where("_id").is(key(objectType, objectId)));
			Update update = new Update().set("operationId", operationId).set("action", action.getKey())
					.set("objectType", objectType).set("objectId", objectId).set("objectName", objectName)
					.set("createdAt", new Date());
			mongoTemplate.upsert(query, update, COLLECTION);
			// 以写入后回读为准，不依赖驱动的 upsert 返回值
			Pending stored = peek(objectType, objectId);
			return stored != null && operationId.equals(stored.getOperationId());
		} catch (Exception e) {
			log.warn("Register async audit operation failed, type={}, id={}", objectType, objectId, e);
			return false;
		}
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

	/**
	 * 撤销仍属于该 Operation 的登记（预登记后业务失败、或同步完成已自行写最终事件时使用）。
	 * 登记已被完成路径取走或已改属其它 Operation 时不做处理。
	 */
	/**
	 * 撤销本次占有的登记。已经完成（登记被取走）或已属其它操作的记录不动，
	 * 避免业务内完成的路径被误撤销后又被重新登记（TAP-13018 review P2）。
	 */
	public void release(String objectType, String objectId, String operationId) {
		if (objectId == null || operationId == null) {
			return;
		}
		try {
			Query query = Query.query(Criteria.where("_id").is(key(objectType, objectId)).and("operationId").is(operationId));
			mongoTemplate.findAndRemove(query, Pending.class, COLLECTION);
		} catch (Exception e) {
			log.warn("Release async audit operation failed, type={}, id={}", objectType, objectId, e);
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
