package com.tapdata.tm.userLog.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.AuditEventType;
import com.tapdata.tm.userLog.constant.AuditOutcome;
import com.tapdata.tm.userLog.param.AuditLogParam;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class ConnectionAuditService {

	public static final String ACTION_TEST_CONNECTION = "testConnection";
	public static final String ACTION_LOAD_SCHEMA = "loadSchema";
	private static final String COLLECTION = "AuditOperationContexts";
	private static final long CONTEXT_TTL_MILLIS = 24L * 60L * 60L * 1000L;

	private final MongoTemplate mongoTemplate;
	private final UserLogService userLogService;

	public ConnectionAuditService(MongoTemplate mongoTemplate, UserLogService userLogService) {
		this.mongoTemplate = mongoTemplate;
		this.userLogService = userLogService;
	}

	public void prepare(String action, String connectionId, String receiver, UserDetail user, String sourceIp, String objectName) {
		if (user == null || StringUtils.isBlank(action) || StringUtils.isBlank(receiver)) {
			return;
		}
		mongoTemplate.remove(Query.query(Criteria.where("createdAt").lt(new Date(System.currentTimeMillis() - CONTEXT_TTL_MILLIS))), COLLECTION);
		Query query = Query.query(Criteria.where("action").is(action)
				.and("connectionId").is(value(connectionId))
				.and("receiver").is(receiver));
		Update update = new Update()
				.set("action", action)
				.set("connectionId", value(connectionId))
				.set("receiver", receiver)
				.set("userId", user.getUserId())
				.set("customerId", user.getCustomerId())
				.set("username", user.getUsername())
				.set("sourceIp", sourceIp)
				.set("objectName", objectName)
				.set("createdAt", new Date());
		mongoTemplate.upsert(query, update, COLLECTION);
	}

	public void completeConnectionTest(String connectionId, String receiver, boolean success) {
		Document context = findLatest(connectionId, receiver, null, false);
		if (context == null) {
			return;
		}
		String action = context.getString("action");
		if (ACTION_LOAD_SCHEMA.equals(action) && success) {
			return;
		}
		context = removeById(context);
		if (context != null) {
			record(context, success, success ? null : ACTION_LOAD_SCHEMA.equals(action)
					? "schema_load_failed" : "connection_test_failed");
		}
	}

	public void completeDispatchFailure(String connectionId, String receiver) {
		Document context = findLatest(connectionId, receiver, null, true);
		if (context != null) {
			record(context, false, "connection_agent_unavailable");
		}
	}

	public void completeSchemaLoad(String connectionId, String loadFieldsStatus) {
		if (!"finished".equals(loadFieldsStatus) && !"error".equals(loadFieldsStatus)) {
			return;
		}
		Document context = findLatest(connectionId, null, ACTION_LOAD_SCHEMA, true);
		if (context != null) {
			boolean success = "finished".equals(loadFieldsStatus);
			record(context, success, success ? null : "schema_load_failed");
		}
	}

	private Document findLatest(String connectionId, String receiver, String action, boolean remove) {
		if (StringUtils.isBlank(connectionId) && StringUtils.isBlank(receiver)) {
			return null;
		}
		Criteria criteria = new Criteria();
		if (StringUtils.isBlank(receiver) && StringUtils.isNotBlank(connectionId)) {
			criteria.and("connectionId").is(connectionId);
		}
		if (StringUtils.isNotBlank(receiver)) {
			criteria.and("receiver").is(receiver);
		}
		if (StringUtils.isNotBlank(action)) {
			criteria.and("action").is(action);
		}
		Query query = Query.query(criteria).with(Sort.by(Sort.Direction.DESC, "createdAt"));
		return remove ? mongoTemplate.findAndRemove(query, Document.class, COLLECTION)
				: mongoTemplate.findOne(query, Document.class, COLLECTION);
	}

	private Document removeById(Document context) {
		return mongoTemplate.findAndRemove(Query.query(Criteria.where("_id").is(context.get("_id"))), Document.class, COLLECTION);
	}

	private void record(Document context, boolean success, String failureReason) {
		AuditLogParam param = new AuditLogParam();
		param.setEventType(AuditEventType.USER_OPERATION);
		param.setOutcome(success ? AuditOutcome.SUCCESS : AuditOutcome.FAILURE);
		param.setUserId(context.getString("userId"));
		param.setCustomerId(context.getString("customerId"));
		param.setUsername(context.getString("username"));
		param.setSourceIp(context.getString("sourceIp"));
		param.setAction(context.getString("action"));
		param.setObjectName(context.getString("objectName"));
		param.setFailureReason(failureReason);
		userLogService.addAuditLog(param);
	}

	private String value(String value) {
		return value == null ? "" : value;
	}
}
