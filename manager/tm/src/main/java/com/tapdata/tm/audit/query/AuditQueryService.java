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
import java.util.Optional;
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
		if (scopeUserId != null) {
			// 列表与详情使用同一可见性规则：旧版本遗留的跨用户 Operation ID 冲突记录
			// 不能通过列表带出他人的事件或 initiator（TAP-13018 review）
			List<AuditOperationVo> masked = new ArrayList<>();
			for (AuditOperationVo op : items) {
				visibleOperation(op, scopeUserId).ifPresent(masked::add);
			}
			items = masked;
		}
		return new Page<>(total, items);
	}

	/** 按 Event ID 或 Operation ID 查询完整逻辑操作。 */
	public List<AuditOperationVo> detail(String id, String scopeUserId) {
		Criteria criteria = new Criteria().orOperator(Criteria.where("eventId").is(id), Criteria.where("operationId").is(id));
		List<UserLogs> seeds = mongoTemplate.find(Query.query(criteria).limit(MAX_PAGE_SIZE), UserLogs.class);
		if (seeds.isEmpty() && ObjectId.isValid(id)) {
			seeds = mongoTemplate.find(Query.query(Criteria.where("_id").is(new ObjectId(id))), UserLogs.class);
		}
		if (scopeUserId != null) {
			// 先定位所属操作，再按与列表相同的可见性规则加载，
			// 避免本人 FINAL（actor=SYSTEM）因不是本人用户而看不到（TAP-13018 review）
			seeds = seeds.stream().filter(e -> scopeUserId.equals(e.getUserId())
					|| AuditValues.SYSTEM.equals(e.getUserId())).toList();
			if (seeds.isEmpty()) {
				return List.of();
			}
		}
		List<String> keys = seeds.stream().map(AuditQueryService::keyOf).distinct().toList();
		List<AuditOperationVo> operations = new ArrayList<>(load(keys).values());
		if (scopeUserId == null) {
			return operations;
		}
		List<AuditOperationVo> visible = new ArrayList<>();
		for (AuditOperationVo op : operations) {
			visibleOperation(op, scopeUserId).ifPresent(visible::add);
		}
		return visible;
	}

	/**
	 * 受限用户可见的操作视图：按与列表/详情一致的规则裁剪事件后重新聚合。
	 * 旧版本可能产生跨用户的 Operation ID 冲突记录，这里不能凭相同 Operation ID 就放行。
	 */
	Optional<AuditOperationVo> visibleOperation(AuditOperationVo op, String scopeUserId) {
		List<AuditEventVo> events = visibleEvents(op.getEvents(), scopeUserId);
		if (events.isEmpty()) {
			return Optional.empty();
		}
		List<AuditEventVo> ordered = new ArrayList<>(events);
		return Optional.of(aggregate(op.getOperationId() == null ? "" : op.getOperationId() + "|" + keyAction(events), ordered));
	}

	/**
	 * 本人事件始终保留；SYSTEM FINAL 只能关联到真实受理的 REQUEST 对象。
	 * 旧 Operation ID 可能跨用户冲突，必须同时匹配 action、对象类型和对象 ID，
	 * 且该对象的受理用户唯一；失败尝试、批量失败项与无对象事件均不能扩大可见范围。
	 */
	List<AuditEventVo> visibleEvents(List<AuditEventVo> events, String scopeUserId) {
		Map<VisibilityObject, Set<String>> acceptedOwners = new LinkedHashMap<>();
		for (AuditEventVo e : events) {
			if (StringUtils.isBlank(e.getUserId()) || AuditValues.SYSTEM.equals(e.getUserId())
					|| !"REQUEST".equals(e.getPhase())
					|| (!AuditOutcome.ACCEPTED.name().equals(e.getOutcome())
						&& !AuditOutcome.PARTIAL.name().equals(e.getOutcome()))) {
				continue;
			}
			for (VisibilityObject object : eventObjects(e, true)) {
				acceptedOwners.computeIfAbsent(object, k -> new LinkedHashSet<>()).add(e.getUserId());
			}
		}
		Set<String> ownUser = Set.of(scopeUserId);
		List<AuditEventVo> visible = new ArrayList<>();
		for (AuditEventVo e : events) {
			if (scopeUserId.equals(e.getUserId())) {
				visible.add(e);
				continue;
			}
			if (!AuditValues.SYSTEM.equals(e.getUserId()) || !"FINAL".equals(e.getPhase())) {
				continue;
			}
			List<VisibilityObject> objects = eventObjects(e, false);
			if (!objects.isEmpty() && objects.stream().allMatch(object -> ownUser.equals(acceptedOwners.get(object)))) {
				visible.add(e);
			}
		}
		return visible;
	}

	/** 可见性关联键必须完整；批量受理只取明确成功或受理的逐项结果，不回退顶层对象。 */
	private static List<VisibilityObject> eventObjects(AuditEventVo event, boolean acceptedOnly) {
		if (StringUtils.isBlank(event.getOperationId()) || StringUtils.isBlank(event.getAction())
				|| StringUtils.isBlank(event.getObjectType())) {
			return List.of();
		}
		List<String> ids = new ArrayList<>();
		AuditBatch batch = event.getBatch();
		if (batch != null) {
			if (batch.getItems() == null) {
				return List.of();
			}
			for (AuditBatchItem item : batch.getItems()) {
				if (item == null || StringUtils.isBlank(item.getObjectId())) {
					if (!acceptedOnly) {
						return List.of();
					}
					continue;
				}
				if (!acceptedOnly || item.getOutcome() == AuditOutcome.ACCEPTED || item.getOutcome() == AuditOutcome.SUCCESS) {
					ids.add(item.getObjectId());
				}
			}
		} else if (!acceptedOnly || AuditOutcome.ACCEPTED.name().equals(event.getOutcome())) {
			ids.addAll(splitIds(event.getObjectId()));
		}
		return ids.stream().map(id -> new VisibilityObject(event.getOperationId(), event.getAction(), event.getObjectType(), id)).toList();
	}

	private record VisibilityObject(String operationId, String action, String objectType, String objectId) {
	}

	private static String keyAction(List<AuditEventVo> events) {
		return events.isEmpty() || events.get(0).getAction() == null ? "" : events.get(0).getAction();
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
		// 同一 Operation 下可能有多个 REQUEST：集群操作按节点各产生一条受理事件（TAP-13018 review P1）。
		List<AuditEventVo> requests = events.stream().filter(e -> !"FINAL".equals(e.getPhase())).toList();
		AuditEventVo initiator = requests.isEmpty() ? events.get(0) : requests.get(0);
		List<AuditEventVo> finals = events.stream().filter(e -> "FINAL".equals(e.getPhase())).toList();
		AuditEventVo last = finals.isEmpty() ? null : finals.get(finals.size() - 1);
		op.setInitiator(initiator);
		op.setTimestamp(initiator.getTimestamp());
		op.setOutcome(resolveOutcome(requests.isEmpty() ? List.of(initiator) : requests, finals));
		op.setFinalTimestamp(last == null ? null : last.getTimestamp());
		op.setPending(resolvePending(requests.isEmpty() ? List.of(initiator) : requests, finals));
		return op;
	}

	/**
	 * 操作级结果：按对象归并全部受理与最终事件（TAP-13018 review P1）。
	 * 一个批量或集群操作下每个对象各自受理、各自上报最终结果，
	 * 整体结果由各对象结果合并得出：全部成功才算成功，全部失败才算失败，混合即部分成功。
	 */
	static String resolveOutcome(List<AuditEventVo> requests, List<AuditEventVo> finals) {
		Map<String, String> perObject = new LinkedHashMap<>();
		Set<String> accepted = new LinkedHashSet<>();
		for (AuditEventVo request : requests) {
			AuditBatch batch = request.getBatch();
			if (batch != null && batch.getTotal() > 0) {
				for (AuditBatchItem item : batch.getItems()) {
					if (item == null || item.getObjectId() == null) {
						continue;
					}
					perObject.put(item.getObjectId(), item.getOutcome() == null ? null : item.getOutcome().name());
					if (item.getOutcome() != AuditOutcome.FAILED) {
						accepted.add(item.getObjectId());
					}
				}
				continue;
			}
			for (String objectId : splitIds(request.getObjectId())) {
				perObject.put(objectId, request.getOutcome());
				if (!AuditOutcome.FAILED.name().equals(request.getOutcome())) {
					accepted.add(objectId);
				}
			}
		}
		for (AuditEventVo fin : finals) {
			List<String> targets = splitIds(fin.getObjectId());
			if (targets.isEmpty()) {
				// 最终事件未携带对象（单对象操作或历史数据）：作用于全部已受理对象
				targets = new ArrayList<>(accepted);
			}
			for (String objectId : targets) {
				if (accepted.contains(objectId)) {
					perObject.put(objectId, fin.getOutcome());
				}
			}
		}
		if (perObject.isEmpty()) {
			// 请求未携带对象标识：退回请求结果与最终结果的简单合并
			String requested = requests.isEmpty() ? null : requests.get(0).getOutcome();
			String finalOutcome = finals.isEmpty() ? null : finals.get(finals.size() - 1).getOutcome();
			if (AuditOutcome.FAILED.name().equals(requested) || AuditOutcome.PARTIAL.name().equals(requested)) {
				return requested;
			}
			return finalOutcome != null ? finalOutcome : requested;
		}
		return combine(perObject.values());
	}

	private static List<String> splitIds(String objectId) {
		if (StringUtils.isBlank(objectId)) {
			return List.of();
		}
		List<String> ids = new ArrayList<>();
		for (String id : objectId.split(",")) {
			if (StringUtils.isNotBlank(id)) {
				ids.add(id.trim());
			}
		}
		return ids;
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
	 * 同一 Operation 下可能有多条 REQUEST（集群按节点受理），需要合并它们的受理对象。
	 */
	static boolean resolvePending(List<AuditEventVo> requests, List<AuditEventVo> finals) {
		Set<String> accepted = new LinkedHashSet<>();
		boolean anyAccepted = false;
		for (AuditEventVo request : requests) {
			if (!"REQUEST".equals(request.getPhase())) {
				continue;
			}
			if (!AuditOutcome.ACCEPTED.name().equals(request.getOutcome())
					&& !AuditOutcome.PARTIAL.name().equals(request.getOutcome())) {
				continue;
			}
			anyAccepted = true;
			accepted.addAll(acceptedObjects(request));
		}
		if (!anyAccepted) {
			return false;
		}
		if (accepted.isEmpty()) {
			return finals.isEmpty();
		}
		Set<String> finished = new HashSet<>();
		for (AuditEventVo fin : finals) {
			finished.addAll(splitIds(fin.getObjectId()));
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
		if (!AuditOutcome.FAILED.name().equals(initiator.getOutcome())) {
			accepted.addAll(splitIds(initiator.getObjectId()));
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
