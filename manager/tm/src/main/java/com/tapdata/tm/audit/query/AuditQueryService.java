package com.tapdata.tm.audit.query;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.AuditBatch;
import com.tapdata.tm.audit.AuditBatchItem;
import com.tapdata.tm.audit.AuditChange;
import com.tapdata.tm.audit.AuditOutcome;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
		List<AuditEventVo> finals = events.stream().filter(e -> "FINAL".equals(e.getPhase())).toList();
		AuditEventVo last = finals.isEmpty() ? null : finals.get(finals.size() - 1);
		op.setInitiator(initiator);
		op.setTimestamp(initiator.getTimestamp());
		op.setOutcome(resolveOutcome(initiator, finals));
		op.setFinalTimestamp(last == null ? null : last.getTimestamp());
		op.setPending(resolvePending(initiator, finals));
		return op;
	}

	/**
	 * 操作级结果：按对象归并受理与最终事件（TAP-13018 review P1）。
	 * 一个批量操作下每个对象各自上报最终结果，整体结果由各对象结果合并得出：
	 * 全部成功才算成功，全部失败才算失败，混合即部分成功；请求阶段已失败的对象保持失败。
	 */
	static String resolveOutcome(AuditEventVo initiator, List<AuditEventVo> finals) {
		String requested = initiator == null ? null : initiator.getOutcome();
		AuditBatch batch = initiator == null ? null : initiator.getBatch();

		// 请求阶段已有对象级结果：按逐项结果判定，最终事件只补充尚未得出结论的对象
		if (batch != null && batch.getTotal() > 0) {
			Map<String, String> perObject = new LinkedHashMap<>();
			Set<String> accepted = new LinkedHashSet<>();
			for (AuditBatchItem item : batch.getItems()) {
				if (item == null || item.getObjectId() == null) {
					continue;
				}
				perObject.put(item.getObjectId(), item.getOutcome() == null ? null : item.getOutcome().name());
				if (item.getOutcome() != AuditOutcome.FAILED) {
					accepted.add(item.getObjectId());
				}
			}
			for (AuditEventVo fin : finals) {
				String objectId = fin.getObjectId();
				if (objectId == null || !accepted.contains(objectId)) {
					continue;
				}
				perObject.put(objectId, fin.getOutcome());
			}
			return combine(perObject.values());
		}

		// 非批量或请求阶段未带逐项结果：沿用请求结果，最终结果仅在请求未给出失败时覆盖
		String finalOutcome = finals.isEmpty() ? null : finals.get(finals.size() - 1).getOutcome();
		if (AuditOutcome.PARTIAL.name().equals(requested)) {
			return AuditOutcome.PARTIAL.name();
		}
		if (AuditOutcome.FAILED.name().equals(requested)) {
			return requested;
		}
		return finalOutcome != null ? finalOutcome : requested;
	}

	/** 由各对象结果合并出整体结果；同一对象多次上报时取最后一次。 */
	private static String combine(Collection<String> outcomes) {
		int success = 0;
		int failed = 0;
		int accepted = 0;
		for (String outcome : outcomes) {
			if (outcome == null) {
				continue;
			}
			switch (outcome) {
				case "FAILED" -> failed++;
				case "ACCEPTED", "PARTIAL" -> accepted++;
				default -> success++;
			}
		}
		if (failed == 0 && accepted == 0) {
			return success == 0 ? null : AuditOutcome.SUCCESS.name();
		}
		if (success == 0 && accepted == 0) {
			return AuditOutcome.FAILED.name();
		}
		// 还有对象未出最终结果：先按受理展示，等待最终结果
		if (accepted > 0 && failed == 0) {
			return AuditOutcome.ACCEPTED.name();
		}
		return AuditOutcome.PARTIAL.name();
	}

	/**
	 * 异步操作只有在所有已受理对象都拿到最终结果后才结束 pending
	 * （TAP-13018 review P1：部分对象完成就提前结束会让操作看起来已完成）。
	 */
	static boolean resolvePending(AuditEventVo initiator, List<AuditEventVo> finals) {
		if (initiator == null || !"REQUEST".equals(initiator.getPhase())
				|| !AuditOutcome.ACCEPTED.name().equals(initiator.getOutcome())
				&& !AuditOutcome.PARTIAL.name().equals(initiator.getOutcome())) {
			return false;
		}
		Set<String> accepted = acceptedObjects(initiator);
		if (accepted.isEmpty()) {
			return finals.isEmpty();
		}
		Set<String> finished = new HashSet<>();
		for (AuditEventVo fin : finals) {
			if (fin.getObjectId() != null) {
				finished.add(fin.getObjectId());
			}
		}
		return !finished.containsAll(accepted);
	}

	/** 请求阶段仍可能产生最终结果的对象：批量取未失败项，单对象取请求对象本身。 */
	private static Set<String> acceptedObjects(AuditEventVo initiator) {
		Set<String> accepted = new LinkedHashSet<>();
		AuditBatch batch = initiator.getBatch();
		if (batch != null) {
			for (AuditBatchItem item : batch.getItems()) {
				if (item != null && item.getObjectId() != null && item.getOutcome() != AuditOutcome.FAILED) {
					accepted.add(item.getObjectId());
				}
			}
			return accepted;
		}
		if (initiator.getObjectId() != null && !AuditOutcome.FAILED.name().equals(initiator.getOutcome())) {
			for (String id : initiator.getObjectId().split(",")) {
				if (StringUtils.isNotBlank(id)) {
					accepted.add(id.trim());
				}
			}
		}
		return accepted;
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
		fillMissingChangeLabels(e, vo);
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

	/** 修复前记录的变更没有 label，读取时按「分类.键」回查设置项 key_label，历史记录同样显示可读名称。 */
	void fillMissingChangeLabels(UserLogs e, AuditEventVo vo) {
		List<AuditChange> changes = vo.getChanges();
		if (changes == null || changes.isEmpty()) {
			return;
		}
		boolean missing = changes.stream().anyMatch(c -> StringUtils.isBlank(c.getLabel()) && c.getField() != null && c.getField().contains("."));
		if (!missing) {
			return;
		}
		List<String> keys = changes.stream().filter(c -> StringUtils.isBlank(c.getLabel()) && c.getField() != null)
				.map(c -> c.getField().substring(c.getField().indexOf('.') + 1)).filter(StringUtils::isNotBlank).distinct().toList();
		if (keys.isEmpty()) {
			return;
		}
		Map<String, String> labels = new LinkedHashMap<>();
		try {
			for (Document doc : mongoTemplate.find(Query.query(Criteria.where("key").in(keys)), Document.class, "Settings")) {
				String label = doc.getString("key_label");
				if (StringUtils.isNotBlank(label)) {
					labels.putIfAbsent(doc.getString("key"), label);
				}
			}
		} catch (Exception ex) {
			return;
		}
		changes.forEach(c -> {
			if (StringUtils.isBlank(c.getLabel()) && c.getField() != null && c.getField().contains(".")) {
				c.setLabel(labels.get(c.getField().substring(c.getField().indexOf('.') + 1)));
			}
		});
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
