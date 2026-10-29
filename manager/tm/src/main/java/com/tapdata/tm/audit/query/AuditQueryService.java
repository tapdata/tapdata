package com.tapdata.tm.audit.query;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.AuditValues;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.userLog.constant.UserLogType;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.entity.UserLogs;
import com.tapdata.tm.userLog.service.UserLogServiceImpl;
import com.tapdata.tm.utils.MessageUtil;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 审计查询：列表按逻辑操作聚合并稳定倒序（时间、标识）；详情返回完整时间线；导出使用原始事件粒度。
 * 正常查询与查看详情不产生审计事件（需求 BR-14）。
 */
@Service
public class AuditQueryService {
	public static final int MAX_PAGE_SIZE = 100;
	public static final int MAX_EXPORT = 100_000;
	private static final String COLLECTION = "UserLogs";
	private static final List<String> TYPES = List.of(AuditValues.SCHEMA_TYPE, UserLogType.USER_OPERATION.getValue());

	private final MongoTemplate mongoTemplate;
	private final UserLogServiceImpl userLogService;

	public AuditQueryService(MongoTemplate mongoTemplate, UserLogServiceImpl userLogService) {
		this.mongoTemplate = mongoTemplate;
		this.userLogService = userLogService;
	}

	public Page<AuditOperationVo> list(AuditQuery query, String scopeUserId) {
		int size = Math.max(1, Math.min(query.getSize(), MAX_PAGE_SIZE));
		int page = Math.max(1, query.getPage());
		List<Document> pipeline = List.of(
				new Document("$match", toDocument(criteria(query, scopeUserId))),
				new Document("$project", new Document("key", groupKey()).append("ts", new Document("$ifNull", List.of("$timestamp", "$createTime")))),
				new Document("$group", new Document("_id", "$key").append("ts", new Document("$max", "$ts"))),
				new Document("$sort", new Document("ts", -1).append("_id", -1)),
				new Document("$facet", new Document("items", List.of(new Document("$skip", (long) (page - 1) * size), new Document("$limit", size)))
						.append("total", List.of(new Document("$count", "count")))));
		Document result = mongoTemplate.getCollection(COLLECTION).aggregate(pipeline).allowDiskUse(true).first();
		List<String> keys = new ArrayList<>();
		long total = 0;
		if (result != null) {
			result.getList("items", Document.class).forEach(d -> keys.add(d.getString("_id")));
			List<Document> totals = result.getList("total", Document.class);
			total = totals.isEmpty() ? 0 : ((Number) totals.get(0).get("count")).longValue();
		}
		Map<String, AuditOperationVo> operations = load(keys);
		List<AuditOperationVo> items = keys.stream().map(operations::get).filter(o -> o != null).toList();
		return new Page<>(total, items);
	}

	/** 按 Event ID 或 Operation ID 查询完整逻辑操作。 */
	public List<AuditOperationVo> detail(String id, String scopeUserId) {
		Criteria criteria = new Criteria().orOperator(Criteria.where("eventId").is(id), Criteria.where("operationId").is(id));
		List<UserLogs> seeds = mongoTemplate.find(Query.query(criteria).limit(MAX_PAGE_SIZE), UserLogs.class);
		if (seeds.isEmpty() && ObjectId.isValid(id)) {
			seeds = mongoTemplate.find(Query.query(Criteria.where("_id").is(new ObjectId(id))), UserLogs.class);
		}
		if (scopeUserId != null && seeds.stream().noneMatch(e -> scopeUserId.equals(e.getUserId()))) {
			return List.of();
		}
		List<String> keys = seeds.stream().map(AuditQueryService::keyOf).distinct().toList();
		return new ArrayList<>(load(keys).values());
	}

	public long count(AuditQuery query, String scopeUserId) {
		return mongoTemplate.count(Query.query(criteria(query, scopeUserId)), COLLECTION);
	}

	/** 导出原始事件；超过上限时拒绝导出，避免截断后的证据被误认为完整。 */
	public long export(AuditQuery query, String scopeUserId, Consumer<AuditEventVo> sink) {
		long count = count(query, scopeUserId);
		if (count > MAX_EXPORT) {
			throw new BizException("Audit.Export.TooMany", count, MAX_EXPORT);
		}
		Query find = Query.query(criteria(query, scopeUserId))
				.with(Sort.by(Sort.Order.desc("timestamp"), Sort.Order.desc("createTime"), Sort.Order.desc("eventId"), Sort.Order.desc("_id")));
		Locale locale = MessageUtil.getLocale();
		try (Stream<UserLogs> stream = mongoTemplate.stream(find, UserLogs.class)) {
			stream.forEach(e -> sink.accept(toVo(e, locale)));
		}
		return count;
	}

	Criteria criteria(AuditQuery q, String scopeUserId) {
		List<Criteria> and = new ArrayList<>();
		and.add(Criteria.where("type").in(TYPES));
		if (scopeUserId != null) {
			and.add(Criteria.where("user_id").is(scopeUserId));
		}
		if (q.getStart() != null || q.getEnd() != null) {
			and.add(new Criteria().orOperator(range("timestamp", q), new Criteria().andOperator(
					Criteria.where("timestamp").exists(false), range("createTime", q))));
		}
		eq(and, "eventType", q.getEventType());
		eq(and, "source", q.getSource());
		eq(and, "action", q.getAction());
		eq(and, "outcome", q.getOutcome());
		eq(and, "eventId", q.getEventId());
		eq(and, "operationId", q.getOperationId());
		if (StringUtils.isNotBlank(q.getSourceIp())) {
			and.add(Criteria.where("sourceIp").regex(contains(q.getSourceIp())));
		}
		if (StringUtils.isNotBlank(q.getUser())) {
			Pattern p = contains(q.getUser());
			and.add(new Criteria().orOperator(Criteria.where("user_id").is(q.getUser().trim()), Criteria.where("username").regex(p),
					Criteria.where("attemptedAccount").regex(p)));
		}
		if (StringUtils.isNotBlank(q.getObject())) {
			Pattern p = contains(q.getObject());
			and.add(new Criteria().orOperator(Criteria.where("objectId").is(q.getObject().trim()), Criteria.where("objectName").regex(p),
					Criteria.where("parameter1").regex(p)));
		}
		if (StringUtils.isNotBlank(q.getKeyword())) {
			Pattern p = contains(q.getKeyword());
			and.add(new Criteria().orOperator(Criteria.where("sourceIp").regex(p), Criteria.where("action").regex(p),
					Criteria.where("objectName").regex(p), Criteria.where("objectId").regex(p), Criteria.where("eventId").regex(p),
					Criteria.where("operationId").regex(p), Criteria.where("username").regex(p), Criteria.where("parameter1").regex(p),
					Criteria.where("summary").regex(p)));
		}
		return new Criteria().andOperator(and.toArray(new Criteria[0]));
	}

	private Map<String, AuditOperationVo> load(List<String> keys) {
		Map<String, AuditOperationVo> result = new LinkedHashMap<>();
		if (keys.isEmpty()) {
			return result;
		}
		List<String> operationIds = new ArrayList<>();
		List<ObjectId> legacyIds = new ArrayList<>();
		for (String key : keys) {
			int sep = key.indexOf('|');
			if (sep > 0) {
				operationIds.add(key.substring(0, sep));
			} else if (ObjectId.isValid(key)) {
				legacyIds.add(new ObjectId(key));
			}
		}
		Criteria criteria = new Criteria().orOperator(Criteria.where("operationId").in(operationIds), Criteria.where("_id").in(legacyIds));
		Locale locale = MessageUtil.getLocale();
		Map<String, List<AuditEventVo>> grouped = new LinkedHashMap<>();
		for (UserLogs e : mongoTemplate.find(Query.query(criteria), UserLogs.class)) {
			grouped.computeIfAbsent(keyOf(e), k -> new ArrayList<>()).add(toVo(e, locale));
		}
		for (String key : keys) {
			List<AuditEventVo> events = grouped.get(key);
			if (events != null) {
				result.put(key, aggregate(key, events));
			}
		}
		return result;
	}

	static AuditOperationVo aggregate(String key, List<AuditEventVo> events) {
		events.sort(Comparator.comparing(AuditEventVo::getTimestamp, Comparator.nullsFirst(Comparator.naturalOrder()))
				.thenComparing(e -> StringUtils.defaultString(e.getEventId())));
		AuditOperationVo op = new AuditOperationVo();
		op.setOperationId(key.contains("|") ? key.substring(0, key.indexOf('|')) : null);
		op.setEvents(events);
		AuditEventVo initiator = events.stream().filter(e -> !"FINAL".equals(e.getPhase())).findFirst().orElse(events.get(0));
		AuditEventVo last = events.stream().filter(e -> "FINAL".equals(e.getPhase())).reduce((a, b) -> b).orElse(null);
		op.setInitiator(initiator);
		op.setTimestamp(initiator.getTimestamp());
		op.setOutcome(last != null ? last.getOutcome() : initiator.getOutcome());
		op.setFinalTimestamp(last == null ? null : last.getTimestamp());
		op.setPending(last == null && "REQUEST".equals(initiator.getPhase()) && "ACCEPTED".equals(initiator.getOutcome()));
		return op;
	}

	AuditEventVo toVo(UserLogs e, Locale locale) {
		AuditEventVo vo = new AuditEventVo();
		if (e.getSchemaVersion() == null) {
			vo.setLegacy(true);
			vo.setEventId(e.getId() == null ? null : e.getId().toHexString());
			vo.setTimestamp(e.getCreateAt());
			vo.setUserId(e.getUserId());
			vo.setUsername(e.getUsername());
			vo.setObjectId(e.getSourceId() == null ? null : e.getSourceId().toHexString());
			vo.setObjectName(e.getParameter1());
			UserLogDto dto = BeanUtil.copyProperties(e, UserLogDto.class);
			vo.setLegacyMessage(userLogService.renderLegacyMessage(dto, locale));
			vo.setLegacyFields(legacyFields(e));
			return vo;
		}
		BeanUtil.copyProperties(e, vo);
		AuditAction action = AuditAction.ofKey(e.getAction());
		vo.setActionName(action == null ? e.getAction()
				: StringUtils.defaultIfBlank(MessageUtil.getBundleMessageOrNull(locale, "auditMessages", action.messageKey()), e.getAction()));
		return vo;
	}

	/** 历史记录字段，供页面按原渲染组件还原描述（含占位符替换）。 */
	static Map<String, String> legacyFields(UserLogs e) {
		Map<String, String> fields = new LinkedHashMap<>();
		fields.put("modular", e.getModular());
		fields.put("operation", e.getOperation());
		fields.put("parameter1", e.getParameter1());
		fields.put("parameter2", e.getParameter2());
		fields.put("parameter3", e.getParameter3());
		fields.put("sourceName", e.getParameter1());
		fields.put("sourceId", e.getSourceId() == null ? null : e.getSourceId().toHexString());
		return fields;
	}

	static String keyOf(UserLogs e) {
		if (e.getSchemaVersion() != null && e.getOperationId() != null) {
			return e.getOperationId() + "|" + e.getAction();
		}
		return e.getId() == null ? "" : e.getId().toHexString();
	}

	private static Document groupKey() {
		return new Document("$cond", List.of(
				new Document("$and", List.of(new Document("$ifNull", List.of("$schemaVersion", false)), new Document("$ifNull", List.of("$operationId", false)))),
				new Document("$concat", List.of("$operationId", "|", "$action")),
				new Document("$toString", "$_id")));
	}

	private Document toDocument(Criteria criteria) {
		return (Document) mongoTemplate.getConverter().convertToMongoType(criteria.getCriteriaObject());
	}

	private static Criteria range(String field, AuditQuery q) {
		Criteria c = Criteria.where(field);
		if (q.getStart() != null) {
			c = c.gte(new Date(q.getStart()));
		}
		if (q.getEnd() != null) {
			c = c.lte(new Date(q.getEnd()));
		}
		return c;
	}

	private static void eq(List<Criteria> and, String field, String value) {
		if (StringUtils.isNotBlank(value)) {
			and.add(Criteria.where(field).is(value.trim()));
		}
	}

	private static Pattern contains(String value) {
		return Pattern.compile(Pattern.quote(value.trim()), Pattern.CASE_INSENSITIVE);
	}
}
