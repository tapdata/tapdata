package com.tapdata.tm.audit;

import com.mongodb.client.MongoDatabase;
import com.tapdata.tm.audit.service.AuditEventServiceImpl;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.entity.UserLogs;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2 审计集成测试的公共装配：审计服务、切面依赖、异步登记与生命周期组件均为真实实现；MongoDB 是存储边界，
 * 用内存集合替身承接本模块实际使用的读写，测试直接检查落库文档（旁路验证），不经审计查询接口读回。
 */
class AuditItFixture {
	final MongoTemplate mongoTemplate = mock(MongoTemplate.class);
	final MongoDatabase database = mock(MongoDatabase.class);
	final List<UserLogs> events = new CopyOnWriteArrayList<>();
	final List<Document> commands = new CopyOnWriteArrayList<>();
	final Map<String, Map<Object, Document>> collections = new LinkedHashMap<>();
	final AuditEventServiceImpl auditEventService;
	final AuditObjectResolver objectResolver;
	final AuditOperationRegistry registry;
	final AuditFinalizer finalizer;
	RuntimeException insertFailure;

	AuditItFixture() {
		doAnswer(inv -> {
			if (insertFailure != null) {
				throw insertFailure;
			}
			UserLogs doc = inv.getArgument(0);
			events.add(doc);
			return doc;
		}).when(mongoTemplate).insert(any(UserLogs.class));
		when(mongoTemplate.upsert(any(Query.class), any(Update.class), anyString())).thenAnswer(inv -> {
			Query query = inv.getArgument(0);
			Update update = inv.getArgument(1);
			Object id = query.getQueryObject().get("_id");
			Document doc = collection(inv.getArgument(2)).computeIfAbsent(id, k -> new Document("_id", k));
			doc.putAll((Document) update.getUpdateObject().get("$set"));
			return null;
		});
		when(mongoTemplate.findOne(any(Query.class), eq(AuditOperationRegistry.Pending.class), anyString()))
				.thenAnswer(inv -> toPending(first(inv.getArgument(2), inv.getArgument(0))));
		when(mongoTemplate.findAndRemove(any(Query.class), eq(AuditOperationRegistry.Pending.class), anyString())).thenAnswer(inv -> {
			Document doc = first(inv.getArgument(2), inv.getArgument(0));
			if (doc != null) {
				collection(inv.getArgument(2)).remove(doc.get("_id"));
			}
			return toPending(doc);
		});
		when(mongoTemplate.findOne(any(Query.class), eq(Document.class), anyString()))
				.thenAnswer(inv -> copy(first(inv.getArgument(2), inv.getArgument(0))));
		when(mongoTemplate.find(any(Query.class), eq(Document.class), anyString()))
				.thenAnswer(inv -> matching(inv.getArgument(2), inv.getArgument(0)).stream().map(AuditItFixture::copy).toList());
		when(mongoTemplate.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(Document.class), anyString()))
				.thenAnswer(inv -> {
					Query query = inv.getArgument(0);
					Update update = inv.getArgument(1);
					FindAndModifyOptions options = inv.getArgument(2);
					String name = inv.getArgument(4);
					Document existing = first(name, query);
					Document previous = copy(existing);
					if (existing == null) {
						if (!options.isUpsert()) {
							return null;
						}
						existing = new Document("_id", query.getQueryObject().get("_id"));
						collection(name).put(existing.get("_id"), existing);
					}
					existing.putAll((Document) update.getUpdateObject().get("$set"));
					return previous;
				});
		when(mongoTemplate.getDb()).thenReturn(database);
		when(database.runCommand(any(Document.class))).thenAnswer(inv -> {
			commands.add(inv.getArgument(0));
			return new Document("ok", 1);
		});
		auditEventService = new AuditEventServiceImpl(mongoTemplate);
		objectResolver = new AuditObjectResolver(mongoTemplate);
		registry = new AuditOperationRegistry(mongoTemplate);
		finalizer = new AuditFinalizer(auditEventService, registry);
	}

	Map<Object, Document> collection(String name) {
		return collections.computeIfAbsent(name, k -> new LinkedHashMap<>());
	}

	void put(String collection, Document doc) {
		collection(collection).put(doc.get("_id"), doc);
	}

	List<UserLogs> events(AuditAction action) {
		return events.stream().filter(e -> action.getKey().equals(e.getAction())).toList();
	}

	UserLogs only(AuditAction action) {
		List<UserLogs> found = events(action);
		if (found.size() != 1) {
			throw new AssertionError("expected exactly one " + action.getKey() + " event but found " + found.size() + ": " + events);
		}
		return found.get(0);
	}

	static UserDetail user(String id, String name, String authType) {
		UserDetail user = new UserDetail(id, "customer", name, "unused", List.of());
		user.setEmail(name);
		user.setAuthType(authType);
		return user;
	}

	static String oid() {
		return new ObjectId().toHexString();
	}

	private Document first(String name, Query query) {
		List<Document> found = matching(name, query);
		return found.isEmpty() ? null : found.get(0);
	}

	/** 只支持本模块使用到的等值与 $in 条件。 */
	private List<Document> matching(String name, Query query) {
		Document criteria = query.getQueryObject();
		List<Document> result = new ArrayList<>();
		for (Document doc : collection(name).values()) {
			boolean ok = true;
			for (Map.Entry<String, Object> condition : criteria.entrySet()) {
				Object actual = doc.get(condition.getKey());
				Object expected = condition.getValue();
				if (expected instanceof Document operator && operator.containsKey("$in")) {
					ok = ((Collection<?>) operator.get("$in")).contains(actual);
				} else {
					ok = Objects.equals(String.valueOf(actual), String.valueOf(expected));
				}
				if (!ok) {
					break;
				}
			}
			if (ok) {
				result.add(doc);
			}
		}
		return result;
	}

	private static Document copy(Document doc) {
		return doc == null ? null : new Document(doc);
	}

	private static AuditOperationRegistry.Pending toPending(Document doc) {
		if (doc == null) {
			return null;
		}
		AuditOperationRegistry.Pending pending = new AuditOperationRegistry.Pending();
		pending.setId(String.valueOf(doc.get("_id")));
		pending.setOperationId(doc.getString("operationId"));
		pending.setAction(doc.getString("action"));
		pending.setObjectType(doc.getString("objectType"));
		pending.setObjectId(doc.getString("objectId"));
		pending.setObjectName(doc.getString("objectName"));
		return pending;
	}
}
