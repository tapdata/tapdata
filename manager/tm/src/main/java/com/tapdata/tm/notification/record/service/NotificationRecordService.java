package com.tapdata.tm.notification.record.service;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemNotificationMapping;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import com.tapdata.tm.notification.delivery.service.DeliveryAggregation;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.notification.record.dto.NotificationRecordDetailVo;
import com.tapdata.tm.notification.record.dto.NotificationRecordQuery;
import com.tapdata.tm.notification.record.dto.NotificationRecordVo;
import com.tapdata.tm.notification.record.dto.ReadStatePreviewVo;
import com.tapdata.tm.notification.record.dto.SystemNotificationInventoryVo;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 通知记录：同一告警事件的站内告警通知聚合为一行，附带各渠道投递结果。
 * 只读取告警通知（msg=alarm），系统通知与许可证消息不在此列。
 */
@Slf4j
@Service
public class NotificationRecordService {
    public static final String LEGACY_PREFIX = "msg:";
    /** 单次聚合读取的消息上限，避免无边界扫描 */
    static final int MAX_SCAN = 5000;
    /** 单次批量已读/未读的记录上限 */
    static final int MAX_BATCH = 1000;
    /** 分组键：有告警事件按告警聚合，历史消息按消息 ID */
    private static final Document GROUP_KEY = new Document("$cond", Arrays.asList(
            new Document("$gt", Arrays.asList(new Document("$strLenCP", new Document("$ifNull", Arrays.asList("$alarmEventId", ""))), 0)),
            "$alarmEventId", new Document("$concat", Arrays.asList(LEGACY_PREFIX, new Document("$toString", "$_id")))));
    private static final String LATEST = "latest";
    private static final String MESSAGE_ID = "_messageId";
    private static final String OWN = "_own";
    private static final String OWN_UNREAD = "_ownUnread";
    private static final String IS_ALARM = "_isAlarm";
    private static final String DELIVERIES = "_deliveries";
    /** 关联投递时不适用一侧的占位值 */
    private static final String NO_MATCH = "-";

    private final MongoTemplate mongoTemplate;
    private final NotificationDeliveryService deliveryService;
    private final UserLogService userLogService;
    private final ModuleAccess moduleAccess;

    public NotificationRecordService(MongoTemplate mongoTemplate, NotificationDeliveryService deliveryService,
                                     UserLogService userLogService, ModuleAccess moduleAccess) {
        this.mongoTemplate = mongoTemplate;
        this.deliveryService = deliveryService;
        this.userLogService = userLogService;
        this.moduleAccess = moduleAccess;
    }

    public Page<NotificationRecordVo> list(NotificationRecordQuery q, UserDetail user, Locale locale) {
        NotificationRecordQuery query = q == null ? new NotificationRecordQuery() : q;
        int page = Math.max(1, Objects.requireNonNullElse(query.getPage(), 1));
        int size = Math.min(500, Math.max(1, Objects.requireNonNullElse(query.getSize(), 20)));
        GroupPage groups = groups(query, user, null, (long) (page - 1) * size, size);
        return Page.page(records(groups.keys, query, user, locale), groups.total);
    }

    /** 铃铛角标：当前用户未读的站内告警通知（按告警聚合后的行数） */
    public long unreadCount(UserDetail user) {
        Query query = new Query(new Criteria().andOperator(ownScope(user), Criteria.where("read").ne(true)));
        query.fields().include("_id", "alarmEventId");
        return mongoTemplate.find(query.limit(MAX_SCAN), MessageEntity.class).stream()
                .map(NotificationRecordService::groupKey).distinct().count();
    }

    public NotificationRecordDetailVo detail(String id, UserDetail user, Locale locale) {
        List<MessageEntity> messages = mongoTemplate.find(new Query(new Criteria().andOperator(viewScope(user, allData(user)), groupCriteria(id)))
                .with(Sort.by(Sort.Direction.DESC, "createTime")).limit(MAX_SCAN), MessageEntity.class);
        if (messages.isEmpty()) {
            throw new BizException("Notification.NotFound");
        }
        Map<String, List<NotificationDelivery>> allDeliveries = deliveriesOf(Collections.singletonMap(id, messages));
        NotificationRecordVo base = build(id, messages, alarms(Collections.singletonList(messages.get(0).getAlarmEventId())),
                allDeliveries, locale, user);
        NotificationRecordDetailVo detail = new NotificationRecordDetailVo();
        org.springframework.beans.BeanUtils.copyProperties(base, detail);
        detail.setContent(base.getTitle());
        detail.setReadAt(messages.stream().filter(m -> isOwn(m, user)).map(MessageEntity::getReadAt).filter(Objects::nonNull)
                .max(Comparator.naturalOrder()).orElse(null));
        List<NotificationDelivery> deliveries = allDeliveries.getOrDefault(id, Collections.emptyList());
        Map<String, List<NotificationDelivery>> byBatch = deliveries.stream()
                .collect(Collectors.groupingBy(d -> StringUtils.defaultIfBlank(d.getBatchId(), "-"), LinkedHashMap::new, Collectors.toList()));
        byBatch.forEach((batchId, list) -> {
            NotificationRecordDetailVo.Batch batch = new NotificationRecordDetailVo.Batch();
            batch.setBatchId(batchId);
            batch.setTime(list.stream().map(d -> d.getSentAt() != null ? d.getSentAt() : d.getCreateAt())
                    .filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null));
            batch.setOccurrenceCountSnapshot(list.stream().map(NotificationDelivery::getOccurrenceCountSnapshot)
                    .filter(Objects::nonNull).findFirst().orElse(null));
            list.forEach(d -> {
                NotificationRecordDetailVo.Recipient r = new NotificationRecordDetailVo.Recipient();
                r.setChannel(d.getChannel());
                r.setAddress(d.getRecipientAddress());
                r.setStatus(d.getStatus());
                r.setSentAt(d.getSentAt());
                r.setFailureMessage(d.getFailureMessage());
                r.setSkipReason(d.getSkipReason());
                batch.getRecipients().add(r);
            });
            detail.getBatches().add(batch);
        });
        detail.getBatches().sort(Comparator.comparing(NotificationRecordDetailVo.Batch::getTime,
                Comparator.nullsLast(Comparator.reverseOrder())));
        return detail;
    }

    /** 当前筛选下将受影响的记录；用于“全部标为已读/未读”的二次确认 */
    public ReadStatePreviewVo previewReadState(NotificationRecordQuery query, boolean read, UserDetail user, Locale locale) {
        NotificationRecordQuery q = query == null ? new NotificationRecordQuery() : query;
        // 只统计本人可修改且状态需要变化的记录
        Document target = new Document(OWN, 1).append(OWN_UNREAD, read ? 1 : 0);
        GroupPage groups = groups(q, user, target, 0, MAX_BATCH);
        return new ReadStatePreviewVo((int) Math.min(groups.total, Integer.MAX_VALUE), groups.keys, groups.total > MAX_BATCH);
    }

    /**
     * 修改当前用户自己的阅读状态；管理员查看他人记录时不会改变他人状态。
     * @return 实际修改的消息数
     */
    public long updateReadState(Collection<String> ids, boolean read, UserDetail user, boolean batch) {
        if (CollectionUtils.isEmpty(ids)) {
            throw new BizException("IllegalArgument", "ids");
        }
        if (ids.size() > MAX_BATCH) {
            throw new BizException("IllegalArgument", "ids.size > " + MAX_BATCH);
        }
        List<Criteria> groups = ids.stream().distinct().map(NotificationRecordService::groupCriteria).collect(Collectors.toList());
        Query query = new Query(new Criteria().andOperator(ownScope(user), new Criteria().orOperator(groups.toArray(new Criteria[0]))));
        Update update = Update.update("read", read);
        update.set("readAt", read ? new Date() : null);
        long modified = mongoTemplate.updateMulti(query, update, MessageEntity.class).getModifiedCount();
        Operation operation = read ? (batch ? Operation.READ_ALL : Operation.READ) : Operation.UNREAD;
        String id = batch ? null : ids.iterator().next();
        try {
            userLogService.addUserLog(Modular.MESSAGE, operation, user, auditSourceId(id),
                    batch ? String.valueOf(ids.size()) : id);
        } catch (Exception e) {
            log.warn("Audit notification read state failed", e);
        }
        return modified;
    }

    /** 历史系统通知盘点（只读，仅统计数量） */
    public List<SystemNotificationInventoryVo> systemNotificationInventory() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("msg").ne(MsgTypeEnum.ALARM.getValue()).and("isDeleted").ne(true)),
                Aggregation.group("msg").count().as("count")
                        .sum(org.springframework.data.mongodb.core.aggregation.ConditionalOperators
                                .when(Criteria.where("read").is(true)).then(0).otherwise(1)).as("unread"));
        List<Document> rows = mongoTemplate.aggregate(aggregation, MessageEntity.class, Document.class).getMappedResults();
        return rows.stream().map(row -> {
            String msg = row.getString("_id");
            MsgTypeEnum type = msg == null ? null : MsgTypeEnum.getEnumByValue(msg);
            return new SystemNotificationInventoryVo(msg, SystemNotificationMapping.targetOf(type),
                    ((Number) row.getOrDefault("count", 0)).longValue(), ((Number) row.getOrDefault("unread", 0)).longValue());
        }).sorted(Comparator.comparingLong(SystemNotificationInventoryVo::getCount).reversed()).collect(Collectors.toList());
    }

    /** 审计来源须为 ObjectId：历史通知去掉前缀取消息 ID，非法值不记来源 */
    static String auditSourceId(String id) {
        String source = StringUtils.removeStart(id, LEGACY_PREFIX);
        return source != null && ObjectId.isValid(source) ? source : null;
    }

    /** 消息级筛选：可见范围、时间、告警事件、级别、关键字 */
    static Criteria messageCriteria(NotificationRecordQuery q, UserDetail user, boolean allData) {
        List<Criteria> and = new ArrayList<>();
        and.add(viewScope(user, allData));
        if (q.getStart() != null || q.getEnd() != null) {
            Criteria time = Criteria.where("createTime");
            if (q.getStart() != null) {
                time.gte(new Date(q.getStart()));
            }
            if (q.getEnd() != null) {
                time.lte(new Date(q.getEnd()));
            }
            and.add(time);
        }
        if (StringUtils.isNotBlank(q.getAlarmEventId())) {
            and.add(Criteria.where("alarmEventId").is(q.getAlarmEventId()));
        }
        if (CollectionUtils.isNotEmpty(q.getLevel())) {
            and.add(Criteria.where("level").in(q.getLevel().stream().map(Enum::name).collect(Collectors.toList())));
        }
        if (StringUtils.isNotBlank(q.getKeyword())) {
            String keyword = q.getKeyword().trim();
            Pattern pattern = Pattern.compile(Pattern.quote(keyword), Pattern.CASE_INSENSITIVE);
            and.add(new Criteria().orOperator(Criteria.where("messageMetadata.name").regex(pattern),
                    Criteria.where("messageMetadata.id").is(keyword), Criteria.where("title").regex(pattern)));
        }
        return new Criteria().andOperator(and.toArray(new Criteria[0]));
    }

    static class GroupPage {
        final long total;
        final List<String> keys;

        GroupPage(long total, List<String> keys) {
            this.total = total;
            this.keys = keys;
        }
    }

    /**
     * 在库内按告警聚合消息，完成阅读状态、告警类型、渠道、发送状态、收件人筛选后计数并分页。
     * @param extra 追加在分组结果上的筛选，可为空
     */
    GroupPage groups(NotificationRecordQuery q, UserDetail user, Document extra, long skip, int limit) {
        String userId = user == null ? null : user.getUserId();
        List<AggregationOperation> stages = new ArrayList<>();
        stages.add(Aggregation.match(messageCriteria(q, user, allData(user))));
        stages.add(Aggregation.sort(Sort.by(Sort.Direction.DESC, "createTime")));
        Document isOwn = new Document("$eq", Arrays.asList("$user_id", userId));
        stages.add(DeliveryAggregation.stage("$group", new Document("_id", GROUP_KEY)
                .append(LATEST, new Document("$max", "$createTime"))
                .append("alarmEventId", new Document("$first", "$alarmEventId"))
                .append("template", new Document("$first", "$template"))
                .append(MESSAGE_ID, new Document("$first", new Document("$toString", "$_id")))
                .append(OWN, new Document("$max", new Document("$cond", Arrays.asList(isOwn, 1, 0))))
                .append(OWN_UNREAD, new Document("$max", new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(isOwn, new Document("$ne", Arrays.asList("$read", true)))), 1, 0))))));
        stages.add(DeliveryAggregation.stage("$addFields", new Document(IS_ALARM,
                new Document("$ne", Arrays.asList(new Document("$substrCP", Arrays.asList("$_id", 0, LEGACY_PREFIX.length())), LEGACY_PREFIX)))));
        Document readFilter = readFilter(q);
        if (!readFilter.isEmpty()) {
            stages.add(DeliveryAggregation.stage("$match", readFilter));
        }
        if (extra != null && !extra.isEmpty()) {
            stages.add(DeliveryAggregation.stage("$match", extra));
        }
        if (CollectionUtils.isNotEmpty(q.getMetric())) {
            stages.addAll(metricStages(q.getMetric().stream().map(Enum::name).collect(Collectors.toList())));
        }
        Document deliveryFilter = deliveryFilter(q);
        if (!deliveryFilter.isEmpty()) {
            // 告警组按告警事件关联投递，历史消息组按消息 ID；不适用的一侧用占位值，避免空值匹配缺字段的投递
            String collection = mongoTemplate.getCollectionName(NotificationDelivery.class);
            stages.add(DeliveryAggregation.stage("$addFields", new Document("_byAlarm",
                    new Document("$cond", Arrays.asList("$" + IS_ALARM, "$_id", NO_MATCH)))
                    .append("_byMessage", new Document("$cond", Arrays.asList("$" + IS_ALARM, NO_MATCH, "$" + MESSAGE_ID)))));
            stages.add(DeliveryAggregation.lookup(collection, "_byAlarm", "alarmEventId", "_alarmDeliveries"));
            stages.add(DeliveryAggregation.lookup(collection, "_byMessage", "messageId", "_messageDeliveries"));
            stages.add(DeliveryAggregation.stage("$addFields", new Document(DELIVERIES,
                    new Document("$concatArrays", Arrays.asList("$_alarmDeliveries", "$_messageDeliveries")))));
            stages.addAll(DeliveryAggregation.summarize(DELIVERIES));
            stages.add(DeliveryAggregation.stage("$match", deliveryFilter));
        }
        stages.add(DeliveryAggregation.stage("$sort", new Document(LATEST, -1).append("_id", -1)));
        stages.add(DeliveryAggregation.stage("$facet", new Document("total", Collections.singletonList(new Document("$count", "count")))
                .append("keys", Arrays.asList(new Document("$skip", skip), new Document("$limit", limit),
                        new Document("$project", new Document("_id", 1))))));
        Aggregation aggregation = Aggregation.newAggregation(stages)
                .withOptions(AggregationOptions.builder().allowDiskUse(true).build());
        Document result = mongoTemplate.aggregate(aggregation, MessageEntity.class, Document.class).getUniqueMappedResult();
        if (result == null) {
            return new GroupPage(0, new ArrayList<>());
        }
        List<Document> total = result.getList("total", Document.class);
        List<String> keys = result.getList("keys", Document.class).stream().map(d -> d.getString("_id")).collect(Collectors.toList());
        return new GroupPage(total == null || total.isEmpty() ? 0 : ((Number) total.get(0).get("count")).longValue(), keys);
    }

    /** 未读页签及阅读状态筛选：未读指本人仍有未读消息 */
    static Document readFilter(NotificationRecordQuery q) {
        Document filter = new Document();
        if (NotificationRecordQuery.TAB_UNREAD.equals(q.getTab())) {
            filter.put(OWN_UNREAD, 1);
        }
        if (q.getRead() != null) {
            int unread = q.getRead() ? 0 : 1;
            // 未读页签再筛已读时无结果
            filter.put(OWN_UNREAD, filter.containsKey(OWN_UNREAD) && filter.getInteger(OWN_UNREAD) != unread ? -1 : unread);
        }
        return filter;
    }

    /** 告警类型取告警事件；告警事件已不存在时取消息模板，历史消息无告警类型 */
    private List<AggregationOperation> metricStages(List<String> metrics) {
        return Arrays.asList(
                DeliveryAggregation.stage("$addFields", new Document("_alarmOid", new Document("$convert",
                        new Document("input", new Document("$cond", Arrays.asList("$" + IS_ALARM, "$_id", null)))
                                .append("to", "objectId").append("onError", null).append("onNull", null)))),
                DeliveryAggregation.lookup(mongoTemplate.getCollectionName(AlarmInfo.class), "_alarmOid", "_id", "_alarm"),
                DeliveryAggregation.stage("$addFields", new Document("_metric", new Document("$cond", Arrays.asList(
                        new Document("$gt", Arrays.asList(new Document("$size", "$_alarm"), 0)),
                        new Document("$arrayElemAt", Arrays.asList("$_alarm.metric", 0)),
                        new Document("$cond", Arrays.asList("$" + IS_ALARM, "$template", null)))))),
                DeliveryAggregation.stage("$match", new Document("_metric", new Document("$in", metrics))),
                DeliveryAggregation.stage("$project", new Document("_alarm", 0)));
    }

    /** 发送失败页签、渠道、发送状态、收件人筛选，与投递汇总的判定一致 */
    static Document deliveryFilter(NotificationRecordQuery q) {
        List<Document> and = new ArrayList<>();
        if (NotificationRecordQuery.TAB_FAILED.equals(q.getTab())) {
            and.add(new Document(DeliveryAggregation.STATUS,
                    new Document("$in", Arrays.asList(DeliverySummaryVo.FAILED, DeliverySummaryVo.PARTIAL_FAILED))));
        }
        if (CollectionUtils.isNotEmpty(q.getDeliveryStatus())) {
            and.add(new Document(DeliveryAggregation.STATUS, new Document("$in", q.getDeliveryStatus())));
        }
        if (CollectionUtils.isNotEmpty(q.getChannel())) {
            and.add(new Document(DeliveryAggregation.CHANNELS,
                    new Document("$in", q.getChannel().stream().map(Enum::name).collect(Collectors.toList()))));
        }
        if (StringUtils.isNotBlank(q.getRecipient())) {
            String recipient = q.getRecipient().trim().toLowerCase(Locale.ROOT);
            Document contains = new Document("$gte", Arrays.asList(new Document("$indexOfCP", Arrays.asList(
                    new Document("$toLower", new Document("$ifNull", Arrays.asList("$$d.recipientAddress", ""))), recipient)), 0));
            and.add(new Document("$expr", new Document("$anyElementTrue", Collections.singletonList(
                    new Document("$map", new Document("input", "$" + DELIVERIES).append("as", "d").append("in", contains))))));
        }
        if (and.isEmpty()) {
            return new Document();
        }
        return and.size() == 1 ? and.get(0) : new Document("$and", and);
    }

    /** 加载当前页分组的消息并组装记录，顺序与分页结果一致 */
    List<NotificationRecordVo> records(List<String> keys, NotificationRecordQuery q, UserDetail user, Locale locale) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<String> alarmKeys = keys.stream().filter(k -> !k.startsWith(LEGACY_PREFIX)).collect(Collectors.toList());
        List<ObjectId> legacyIds = keys.stream().filter(k -> k.startsWith(LEGACY_PREFIX))
                .map(k -> k.substring(LEGACY_PREFIX.length())).filter(ObjectId::isValid).map(ObjectId::new)
                .collect(Collectors.toList());
        List<Criteria> or = new ArrayList<>();
        if (!alarmKeys.isEmpty()) {
            or.add(Criteria.where("alarmEventId").in(alarmKeys));
        }
        if (!legacyIds.isEmpty()) {
            or.add(Criteria.where("_id").in(legacyIds));
        }
        if (or.isEmpty()) {
            return new ArrayList<>();
        }
        Query query = new Query(new Criteria().andOperator(messageCriteria(q, user, allData(user)),
                new Criteria().orOperator(or.toArray(new Criteria[0]))))
                .with(Sort.by(Sort.Direction.DESC, "createTime")).limit(MAX_SCAN);
        Map<String, List<MessageEntity>> grouped = mongoTemplate.find(query, MessageEntity.class).stream()
                .collect(Collectors.groupingBy(NotificationRecordService::groupKey, LinkedHashMap::new, Collectors.toList()));
        Map<String, List<MessageEntity>> groups = new LinkedHashMap<>();
        keys.stream().filter(grouped::containsKey).forEach(k -> groups.put(k, grouped.get(k)));
        Map<String, AlarmInfo> alarms = alarms(new ArrayList<>(alarmKeys));
        Map<String, List<NotificationDelivery>> deliveries = deliveriesOf(groups);
        return groups.entrySet().stream()
                .map(e -> build(e.getKey(), e.getValue(), alarms, deliveries, locale, user))
                .collect(Collectors.toList());
    }

    NotificationRecordVo build(String key, List<MessageEntity> messages, Map<String, AlarmInfo> alarms,
                               Map<String, List<NotificationDelivery>> deliveries, Locale locale, UserDetail user) {
        MessageEntity latest = messages.get(0);
        NotificationRecordVo vo = new NotificationRecordVo();
        vo.setId(key);
        vo.setLegacy(StringUtils.isBlank(latest.getAlarmEventId()));
        vo.setAlarmEventId(latest.getAlarmEventId());
        vo.setMessageIds(messages.stream().map(m -> m.getId().toHexString()).collect(Collectors.toList()));
        vo.setTitle(title(latest, locale));
        vo.setLevel(latest.getLevel());
        vo.setNotificationCount(messages.size());
        // 阅读状态属于收件人本人：查看他人的通知不显示也不能修改其阅读状态
        List<MessageEntity> own = messages.stream().filter(m -> isOwn(m, user)).collect(Collectors.toList());
        vo.setReadable(!own.isEmpty());
        vo.setUnread(own.stream().anyMatch(m -> !Boolean.TRUE.equals(m.getRead())));
        vo.setLatestNotifyTime(latest.getCreateAt());
        MessageMetadata metadata = latest.getMessageMetadata();
        if (metadata != null) {
            vo.setTaskId(metadata.getId());
            vo.setTaskName(metadata.getName());
        }
        AlarmInfo alarm = latest.getAlarmEventId() == null ? null : alarms.get(latest.getAlarmEventId());
        if (alarm != null) {
            vo.setMetric(alarm.getMetric());
            vo.setAlarmStatus(alarm.getStatus());
            vo.setObjectType(AlarmObjectType.of(alarm));
            vo.setFirstOccurrenceTime(alarm.getFirstOccurrenceTime());
            vo.setLastOccurrenceTime(alarm.getLastOccurrenceTime());
            vo.setOccurrenceCount(alarm.occurrenceCount());
            if (StringUtils.isBlank(vo.getTaskId())) {
                vo.setTaskId(alarm.getTaskId());
            }
            if (StringUtils.isBlank(vo.getTaskName())) {
                vo.setTaskName(alarm.getName());
            }
        } else if (!vo.isLegacy()) {
            // 历史版本在任务重新运行时删除了告警事件：告警类型取消息模板，发生次数取最近投递快照，发生时间不以通知时间冒充
            vo.setAlarmEventMissing(true);
            vo.setMetric(metricOf(latest.getTemplate()));
            vo.setOccurrenceCount(latestOccurrenceSnapshot(deliveries.getOrDefault(key, Collections.emptyList())));
        }
        vo.setDeliverySummary(deliveryService.summarize(deliveries.getOrDefault(key, Collections.emptyList())));
        return vo;
    }

    private static AlarmKeyEnum metricOf(String template) {
        if (StringUtils.isBlank(template)) {
            return null;
        }
        try {
            return AlarmKeyEnum.valueOf(template);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Integer latestOccurrenceSnapshot(List<NotificationDelivery> deliveries) {
        return deliveries.stream().filter(d -> d.getOccurrenceCountSnapshot() != null)
                .max(Comparator.comparing((NotificationDelivery d) -> d.getSentAt() != null ? d.getSentAt() : d.getCreateAt(),
                        Comparator.nullsFirst(Comparator.naturalOrder())))
                .map(NotificationDelivery::getOccurrenceCountSnapshot).orElse(null);
    }

    private Map<String, AlarmInfo> alarms(List<String> alarmEventIds) {
        List<ObjectId> ids = alarmEventIds.stream().filter(Objects::nonNull).filter(ObjectId::isValid).distinct().map(ObjectId::new)
                .collect(Collectors.toList());
        if (ids.isEmpty()) {
            return Collections.emptyMap();
        }
        return mongoTemplate.find(Query.query(Criteria.where("_id").in(ids)), AlarmInfo.class).stream()
                .collect(Collectors.toMap(a -> a.getId().toHexString(), Function.identity(), (a, b) -> a));
    }

    /** 告警组取该告警的全部投递；历史消息组按消息 ID 取 */
    private Map<String, List<NotificationDelivery>> deliveriesOf(Map<String, List<MessageEntity>> groups) {
        Map<String, List<NotificationDelivery>> result = new LinkedHashMap<>();
        List<String> legacyMessageIds = new ArrayList<>();
        groups.forEach((key, messages) -> {
            if (key.startsWith(LEGACY_PREFIX)) {
                legacyMessageIds.add(key.substring(LEGACY_PREFIX.length()));
            }
        });
        List<String> alarmKeys = groups.keySet().stream().filter(k -> !k.startsWith(LEGACY_PREFIX)).collect(Collectors.toList());
        if (!alarmKeys.isEmpty()) {
            deliveryService.findByAlarmEventIds(alarmKeys)
                    .forEach(d -> result.computeIfAbsent(d.getAlarmEventId(), k -> new ArrayList<>()).add(d));
        }
        if (!legacyMessageIds.isEmpty()) {
            deliveryService.findByMessageIds(legacyMessageIds)
                    .forEach(d -> result.computeIfAbsent(LEGACY_PREFIX + d.getMessageId(), k -> new ArrayList<>()).add(d));
        }
        return result;
    }

    static String groupKey(MessageEntity m) {
        return StringUtils.isNotBlank(m.getAlarmEventId()) ? m.getAlarmEventId() : LEGACY_PREFIX + m.getId().toHexString();
    }

    static Criteria groupCriteria(String id) {
        if (StringUtils.startsWith(id, LEGACY_PREFIX)) {
            String messageId = id.substring(LEGACY_PREFIX.length());
            if (!ObjectId.isValid(messageId)) {
                throw new BizException("Notification.NotFound");
            }
            return Criteria.where("_id").is(new ObjectId(messageId));
        }
        return Criteria.where("alarmEventId").is(id);
    }

    /** 有通知模块权限可查看全部用户的记录，否则只查看发给自己的 */
    private boolean allData(UserDetail user) {
        return moduleAccess.allData(user, DataPermissionEnumsName.V2_NOTIFICATION_RECORD);
    }

    /** 可查看范围：告警通知且未隐藏 */
    static Criteria viewScope(UserDetail user, boolean allData) {
        Criteria base = Criteria.where("msg").is(MsgTypeEnum.ALARM.getValue()).and("isDeleted").ne(true);
        return allData ? base : base.and("user_id").is(user == null ? null : user.getUserId());
    }

    private static boolean isOwn(MessageEntity m, UserDetail user) {
        return user != null && Objects.equals(m.getUserId(), user.getUserId());
    }

    /** 阅读状态只作用于当前用户自己的消息 */
    static Criteria ownScope(UserDetail user) {
        return Criteria.where("msg").is(MsgTypeEnum.ALARM.getValue()).and("isDeleted").ne(true)
                .and("user_id").is(user == null ? null : user.getUserId());
    }

    private static String title(MessageEntity m, Locale locale) {
        if (m.getTemplate() != null && m.getParam() != null) {
            try {
                String template = MessageUtil.getAlarmMsg(locale, m.getTemplate());
                String content = new SpelExpressionParser().parseExpression(template, new TemplateParserContext())
                        .getValue(m.getParam(), String.class);
                if (StringUtils.isNotBlank(content)) {
                    return content;
                }
            } catch (Exception e) {
                log.debug("Render notification title failed, id: {}", m.getId(), e);
            }
        }
        return m.getTitle();
    }
}
