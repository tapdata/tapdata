package com.tapdata.tm.audit;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.Map;

/** 按对象类型解析可读名称；删除前由切面预取，保证删除后仍能识别原对象。 */
@Slf4j
@Component
public class AuditObjectResolver {
	private static final Map<String, String[]> NAMES = Map.ofEntries(
			Map.entry("task", new String[]{"Task", "name"}),
			Map.entry("live_cache", new String[]{"Task", "name"}),
			Map.entry("log_cache", new String[]{"Task", "name"}),
			Map.entry("datasource", new String[]{"Connections", "name"}),
			Map.entry("role", new String[]{"Role", "name"}),
			Map.entry("user", new String[]{"User", "email"}),
			Map.entry("user_group", new String[]{"UserGroup", "name"}),
			Map.entry("inspect", new String[]{"Inspect", "name"}),
			Map.entry("api", new String[]{"Modules", "name"}),
			Map.entry("api_app", new String[]{"MetadataDefinition", "value"}),
			Map.entry("api_client", new String[]{"Application", "clientName"}),
			Map.entry("external_storage", new String[]{"ExternalStorage", "name"}),
			Map.entry("agent", new String[]{"Workers", "hostname"}),
			Map.entry("agent_group", new String[]{"AgentGroup", "name"}),
			Map.entry("service", new String[]{"ClusterState", "systemInfo.hostname"}),
			Map.entry("connector", new String[]{"DatabaseTypes", "name"}));

	private final MongoTemplate mongoTemplate;

	public AuditObjectResolver(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	public String resolveName(String objectType, String objectId) {
		String[] target = NAMES.get(objectType);
		if (target == null || objectId == null || !ObjectId.isValid(objectId)) {
			return null;
		}
		try {
			Query query = Query.query(Criteria.where("_id").is(new ObjectId(objectId)));
			query.fields().include(target[1]);
			Document doc = mongoTemplate.findOne(query, Document.class, target[0]);
			if (doc == null) {
				return null;
			}
			Object value = doc.getEmbedded(java.util.List.of(target[1].split("\\.")), Object.class);
			return value == null ? null : String.valueOf(value);
		} catch (Exception e) {
			log.debug("Resolve audit object name failed, type={}, id={}", objectType, objectId, e);
			return null;
		}
	}
}
