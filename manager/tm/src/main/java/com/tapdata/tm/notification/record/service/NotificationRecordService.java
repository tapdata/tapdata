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
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.notification.record.dto.NotificationRecordDetailVo;
import com.tapdata.tm.notification.record.dto.NotificationRecordQuery;
import com.tapdata.tm.notification.record.dto.NotificationRecordVo;
import com.tapdata.tm.notification.record.dto.ReadStatePreviewVo;
import com.tapdata.tm.notification.record.dto.SystemNotificationInventoryVo;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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

    private final MongoTemplate mongoTemplate;
    private final NotificationDeliveryService deliveryService;
    private final UserLogService userLogService;

    public NotificationRecordService(MongoTemplate mongoTemplate, NotificationDeliveryService deliveryService,
                                     UserLogService userLogService) {
        this.mongoTemplate = mongoTemplate;
        this.deliveryService = deliveryService;
        this.userLogService = userLogService;
    }

    public Page<NotificationRecordVo> list(NotificationRecordQuery q, UserDetail user, Locale locale) {
        NotificationRecordQuery query = q == null ? new NotificationRecordQuery() : q;
        List<NotificationRecordVo> records = records(query, user, locale);
        int page = Math.max(1, Objects.requireNonNullElse(query.getPage(), 1));
        int size = Math.min(500, Math.max(1, Objects.requireNonNullElse(query.getSize(), 20)));
        int from = Math.min((page - 1) * size, records.size());
        return Page.page(new ArrayList<>(records.subList(from, Math.min(from + size, records.size()))), records.size());
    }

    /** 铃铛角标：当前用户未读的站内告警通知（按告警聚合后的行数） */
    public long unreadCount(UserDetail user) {
        Query query = new Query(new Criteria().andOperator(ownScope(user), Criteria.where("read").ne(true)));
        query.fields().include("_id", "alarmEventId");
        return mongoTemplate.find(query.limit(MAX_SCAN), MessageEntity.class).stream()
                .map(NotificationRecordService::groupKey).distinct().count();
    }

    public NotificationRecordDetailVo detail(String id, UserDetail user, Locale locale) {
        List<MessageEntity> messages = mongoTemplate.find(new Query(new Criteria().andOperator(viewScope(user), groupCriteria(id)))
                .with(Sort.by(Sort.Direction.DESC, "createTime")).limit(MAX_SCAN), MessageEntity.class);
        if (messages.isEmpty()) {
            throw new BizException("Notification.NotFound");
        }
        Map<String, List<NotificationDelivery>> allDeliveries = deliveriesOf(Collections.singletonMap(id, messages));
        NotificationRecordVo base = build(id, messages, alarms(Collections.singletonList(messages.get(0).getAlarmEventId())),
                allDeliveries, locale);
        NotificationRecordDetailVo detail = new NotificationRecordDetailVo();
        org.springframework.beans.BeanUtils.copyProperties(base, detail);
        detail.setContent(base.getTitle());
        detail.setReadAt(messages.stream().map(MessageEntity::getReadAt).filter(Objects::nonNull)
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
        List<String> ids = records(q, user, locale).stream()
                .filter(r -> r.isUnread() == read)
                .map(NotificationRecordVo::getId).collect(Collectors.toList());
        boolean truncated = ids.size() > MAX_BATCH;
        return new ReadStatePreviewVo(ids.size(), truncated ? ids.subList(0, MAX_BATCH) : ids, truncated);
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
        String sourceId = batch ? null : ids.iterator().next();
        try {
            userLogService.addUserLog(Modular.MESSAGE, operation, user, sourceId,
                    batch ? String.valueOf(ids.size()) : sourceId);
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

    List<NotificationRecordVo> records(NotificationRecordQuery q, UserDetail user, Locale locale) {
        List<Criteria> and = new ArrayList<>();
        and.add(viewScope(user));
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
        Query query = new Query(new Criteria().andOperator(and.toArray(new Criteria[0])))
                .with(Sort.by(Sort.Direction.DESC, "createTime")).limit(MAX_SCAN);
        List<MessageEntity> messages = mongoTemplate.find(query, MessageEntity.class);

        Map<String, List<MessageEntity>> groups = messages.stream()
                .collect(Collectors.groupingBy(NotificationRecordService::groupKey, LinkedHashMap::new, Collectors.toList()));
        Map<String, AlarmInfo> alarms = alarms(messages.stream().map(MessageEntity::getAlarmEventId).collect(Collectors.toList()));
        Map<String, List<NotificationDelivery>> deliveries = deliveriesOf(groups);
        return groups.entrySet().stream()
                .map(e -> build(e.getKey(), e.getValue(), alarms, deliveries, locale))
                .filter(r -> matches(r, q, deliveries.getOrDefault(r.getId(), Collections.emptyList())))
                .collect(Collectors.toList());
    }

    static boolean matches(NotificationRecordVo r, NotificationRecordQuery q, List<NotificationDelivery> deliveries) {
        String status = r.getDeliverySummary() == null ? DeliverySummaryVo.NONE : r.getDeliverySummary().getStatus();
        if (NotificationRecordQuery.TAB_UNREAD.equals(q.getTab()) && !r.isUnread()) {
            return false;
        }
        if (NotificationRecordQuery.TAB_FAILED.equals(q.getTab())
                && !(DeliverySummaryVo.FAILED.equals(status) || DeliverySummaryVo.PARTIAL_FAILED.equals(status))) {
            return false;
        }
        if (q.getRead() != null && r.isUnread() == q.getRead()) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(q.getMetric()) && (r.getMetric() == null || !q.getMetric().contains(r.getMetric()))) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(q.getChannel()) && (r.getDeliverySummary() == null
                || q.getChannel().stream().noneMatch(c -> r.getDeliverySummary().getChannels().contains(c)))) {
            return false;
        }
        if (CollectionUtils.isNotEmpty(q.getDeliveryStatus()) && !q.getDeliveryStatus().contains(status)) {
            return false;
        }
        if (StringUtils.isNotBlank(q.getRecipient())) {
            String recipient = q.getRecipient().trim().toLowerCase(Locale.ROOT);
            return deliveries.stream().map(NotificationDelivery::getRecipientAddress).filter(Objects::nonNull)
                    .anyMatch(a -> a.toLowerCase(Locale.ROOT).contains(recipient));
        }
        return true;
    }

    NotificationRecordVo build(String key, List<MessageEntity> messages, Map<String, AlarmInfo> alarms,
                               Map<String, List<NotificationDelivery>> deliveries, Locale locale) {
        MessageEntity latest = messages.get(0);
        NotificationRecordVo vo = new NotificationRecordVo();
        vo.setId(key);
        vo.setLegacy(StringUtils.isBlank(latest.getAlarmEventId()));
        vo.setAlarmEventId(latest.getAlarmEventId());
        vo.setMessageIds(messages.stream().map(m -> m.getId().toHexString()).collect(Collectors.toList()));
        vo.setTitle(title(latest, locale));
        vo.setLevel(latest.getLevel());
        vo.setNotificationCount(messages.size());
        vo.setUnread(messages.stream().anyMatch(m -> !Boolean.TRUE.equals(m.getRead())));
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
        groups.keySet().stream().filter(k -> !k.startsWith(LEGACY_PREFIX))
                .forEach(k -> result.put(k, deliveryService.findByAlarmEventId(k)));
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

    /** 可查看范围：告警通知且未隐藏；root 可查看全部用户的记录 */
    static Criteria viewScope(UserDetail user) {
        Criteria base = Criteria.where("msg").is(MsgTypeEnum.ALARM.getValue()).and("isDeleted").ne(true);
        return user != null && user.isRoot() ? base : base.and("user_id").is(user == null ? null : user.getUserId());
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
