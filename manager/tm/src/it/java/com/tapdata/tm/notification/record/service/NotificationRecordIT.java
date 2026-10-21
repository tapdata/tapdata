package com.tapdata.tm.notification.record.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryServiceImpl;
import com.tapdata.tm.notification.record.dto.NotificationRecordDetailVo;
import com.tapdata.tm.notification.record.dto.NotificationRecordQuery;
import com.tapdata.tm.notification.record.dto.NotificationRecordVo;
import com.tapdata.tm.notification.record.dto.ReadStatePreviewVo;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.context.support.GenericApplicationContext;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2: real aggregation, filter, read-state and delivery summary logic; Mongo storage and audit log are boundaries. */
@Timeout(10)
class NotificationRecordIT {
    private static final String ALARM_A = "64c0000000000000000000a1";
    private GenericApplicationContext context;
    private NotificationRecordService service;
    private MongoTemplate mongo;
    private UserLogService audit;
    private PermissionService permissions;
    private SettingsService settings;
    private final List<MessageEntity> messages = new ArrayList<>();
    private final List<AlarmInfo> alarmData = new ArrayList<>();
    private final List<NotificationDelivery> deliveries = new ArrayList<>();
    private final AtomicReference<Query> lastMessageQuery = new AtomicReference<>();
    /** 分组聚合管道（JSON），由库内完成筛选、计数与分页 */
    private final List<String> pipelines = new ArrayList<>();
    private final List<String> deliveryQueries = new ArrayList<>();
    /** 存储边界：聚合返回的当前页分组键与总数；为空时按消息最近时间返回全部分组 */
    private List<String> pageKeys;
    private Long pageTotal;
    private UserDetail owner;
    private UserDetail root;

    @BeforeEach
    void setUp() {
        mongo = mock(MongoTemplate.class);
        audit = mock(UserLogService.class);
        permissions = mock(PermissionService.class);
        settings = mock(SettingsService.class);
        when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_NOTIFICATION_RECORD, "root")).thenReturn(true);
        when(mongo.find(any(Query.class), eq(MessageEntity.class))).thenAnswer(i -> {
            lastMessageQuery.set(i.getArgument(0));
            return new ArrayList<>(messages);
        });
        when(mongo.find(any(Query.class), eq(AlarmInfo.class))).thenAnswer(i -> new ArrayList<>(alarmData));
        when(mongo.getCollectionName(NotificationDelivery.class)).thenReturn("NotificationDelivery");
        when(mongo.getCollectionName(AlarmInfo.class)).thenReturn("AlarmInfo");
        when(mongo.aggregate(any(Aggregation.class), eq(MessageEntity.class), eq(Document.class))).thenAnswer(i -> {
            pipelines.add(new Document("p", i.<Aggregation>getArgument(0).toPipeline(Aggregation.DEFAULT_CONTEXT)).toJson());
            List<String> keys = pageKeys != null ? pageKeys : messages.stream()
                    .sorted(Comparator.comparing(MessageEntity::getCreateAt).reversed())
                    .map(NotificationRecordService::groupKey).distinct().collect(Collectors.toList());
            Document facet = new Document("total", List.of(new Document("count", pageTotal != null ? pageTotal : keys.size())))
                    .append("keys", keys.stream().map(k -> new Document("_id", k)).collect(Collectors.toList()));
            return new AggregationResults<>(List.of(facet), new Document());
        });
        when(mongo.find(any(Query.class), eq(NotificationDelivery.class))).thenAnswer(i -> {
            String json = i.<Query>getArgument(0).getQueryObject().toJson();
            deliveryQueries.add(json);
            return deliveries.stream().filter(d -> (d.getAlarmEventId() != null && json.contains(d.getAlarmEventId()))
                    || (d.getMessageId() != null && json.contains(d.getMessageId()))).collect(Collectors.toList());
        });
        owner = new UserDetail("owner", "customer", "owner", "unused", List.of());
        root = new UserDetail("root", "customer", "root", "unused", List.of(new SimpleGrantedAuthority("ADMIN")));

        context = new GenericApplicationContext();
        context.registerBean(NotificationDeliveryService.class, () -> new NotificationDeliveryServiceImpl(mongo));
        context.registerBean(ModuleAccess.class, () -> new ModuleAccess(permissions, settings));
        context.registerBean(NotificationRecordService.class, () -> new NotificationRecordService(mongo,
                context.getBean(NotificationDeliveryService.class), audit, context.getBean(ModuleAccess.class)));
        context.refresh();
        service = context.getBean(NotificationRecordService.class);
    }

    @AfterEach
    void tearDown() {
        context.close();
        messages.clear();
        alarmData.clear();
        deliveries.clear();
        pipelines.clear();
        deliveryQueries.clear();
        pageKeys = null;
        pageTotal = null;
    }

    private String lastPipeline() {
        return pipelines.get(pipelines.size() - 1);
    }

    @Test
    @DisplayName("AC3 同一告警的多次通知聚合为一行，追溯任务、发生时间与次数；无告警关联的历史消息单独成行")
    void should_aggregate_by_alarm_event() {
        givenAlarmWithThreeNotifications();
        MessageEntity legacy = message(null, "旧任务", true, new Date(500));
        messages.add(legacy);

        Page<NotificationRecordVo> page = service.list(new NotificationRecordQuery(), owner, Locale.CHINA);

        assertThat(page.getTotal()).isEqualTo(2);
        NotificationRecordVo record = page.getItems().get(0);
        assertThat(record.getId()).isEqualTo(ALARM_A);
        assertThat(record.getNotificationCount()).isEqualTo(3);
        assertThat(record.getTaskName()).isEqualTo("订单同步");
        assertThat(record.getOccurrenceCount()).isEqualTo(38);
        assertThat(record.getFirstOccurrenceTime()).isEqualTo(new Date(1000));
        assertThat(record.getMetric()).isEqualTo(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        assertThat(record.isUnread()).isTrue();
        assertThat(record.getDeliverySummary().getStatus()).isEqualTo(DeliverySummaryVo.PARTIAL_FAILED);
        assertThat(record.getDeliverySummary().getChannels()).containsExactlyInAnyOrder(DeliveryChannel.SYSTEM, DeliveryChannel.EMAIL);
        NotificationRecordVo old = page.getItems().get(1);
        assertThat(old.isLegacy()).isTrue();
        assertThat(old.getId()).isEqualTo(NotificationRecordService.LEGACY_PREFIX + legacy.getId().toHexString());
        assertThat(old.getDeliverySummary().getStatus()).isEqualTo(DeliverySummaryVo.NONE);
        assertThat(record.isAlarmEventMissing()).isFalse();
        assertThat(old.isAlarmEventMissing()).isFalse();
        // 投递记录按告警批量查询一次、历史消息批量查询一次，不按分组逐条查询
        assertThat(deliveryQueries).hasSize(2);
        assertThat(deliveryQueries.get(0)).contains("\"alarmEventId\": {\"$in\": [\"" + ALARM_A + "\"]}");
        // 分组、计数与分页在库内完成，当前页只加载对应分组的消息
        assertThat(lastPipeline()).contains("\"$group\"").contains("\"$facet\"").contains("\"$skip\": 0").contains("\"$limit\": 20")
                .doesNotContain("$lookup");
        assertThat(lastMessageQuery.get().getQueryObject().toJson()).contains("\"alarmEventId\": {\"$in\": [\"" + ALARM_A + "\"]}")
                .contains("{\"$oid\": \"" + legacy.getId().toHexString() + "\"}");

        NotificationRecordQuery far = new NotificationRecordQuery();
        far.setPage(10_000_000);
        far.setSize(500);
        pageKeys = List.of();
        assertThat(service.list(far, owner, Locale.CHINA).getItems()).isEmpty();
        assertThat(lastPipeline()).contains("\"$skip\": 4999999500");
    }

    @Test
    @DisplayName("AC3 告警事件已被历史版本删除时降级展示：类型取消息模板、次数取最近投递快照、发生时间不伪造")
    void should_degrade_when_alarm_event_missing() {
        givenAlarmWithThreeNotifications();
        alarmData.clear();
        messages.forEach(m -> m.setTemplate(AlarmKeyEnum.TASK_INCREMENT_DELAY.name()));

        NotificationRecordVo record = service.list(new NotificationRecordQuery(), owner, Locale.CHINA).getItems().get(0);
        assertThat(record.getId()).isEqualTo(ALARM_A);
        assertThat(record.isLegacy()).isFalse();
        assertThat(record.isAlarmEventMissing()).isTrue();
        assertThat(record.getMetric()).isEqualTo(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        assertThat(record.getOccurrenceCount()).isEqualTo(30);
        assertThat(record.getFirstOccurrenceTime()).isNull();
        assertThat(record.getLastOccurrenceTime()).isNull();
        assertThat(record.getAlarmStatus()).isNull();
        assertThat(record.getTaskName()).isEqualTo("订单同步");
        query(q -> q.setMetric(List.of(AlarmKeyEnum.TASK_INCREMENT_DELAY)));
        // 告警类型取告警事件，事件已不存在时取消息模板
        assertThat(lastPipeline()).contains("\"from\": \"AlarmInfo\"").contains("\"to\": \"objectId\"")
                .contains("\"$arrayElemAt\": [\"$_alarm.metric\", 0]").contains("\"$template\"")
                .contains("\"_metric\": {\"$in\": [\"TASK_INCREMENT_DELAY\"]}");

        NotificationRecordDetailVo detail = service.detail(ALARM_A, owner, Locale.CHINA);
        assertThat(detail.isAlarmEventMissing()).isTrue();
        assertThat(detail.getOccurrenceCount()).isEqualTo(30);
        assertThat(detail.getBatches()).hasSize(2);

        messages.forEach(m -> m.setTemplate("UNKNOWN_TEMPLATE"));
        assertThat(service.list(new NotificationRecordQuery(), owner, Locale.CHINA).getItems().get(0).getMetric()).isNull();
    }

    @Test
    @DisplayName("AC3 页签与筛选在库内完成：未读、发送失败、渠道、投递状态、收件人、告警类型，再计数分页")
    void should_apply_tabs_and_filters() {
        givenAlarmWithThreeNotifications();

        query(q -> q.setTab(NotificationRecordQuery.TAB_UNREAD));
        // 未读指本人仍有未读消息
        assertThat(lastPipeline()).contains("\"$eq\": [\"$user_id\", \"owner\"]").contains("\"$ne\": [\"$read\", true]")
                .contains("\"$match\": {\"_ownUnread\": 1}").doesNotContain("$lookup");
        query(q -> q.setRead(true));
        assertThat(lastPipeline()).contains("\"$match\": {\"_ownUnread\": 0}");
        query(q -> {
            q.setTab(NotificationRecordQuery.TAB_UNREAD);
            q.setRead(true);
        });
        assertThat(lastPipeline()).contains("\"$match\": {\"_ownUnread\": -1}");

        query(q -> q.setTab(NotificationRecordQuery.TAB_FAILED));
        assertThat(lastPipeline()).contains("\"_deliveryStatus\": {\"$in\": [\"FAILED\", \"PARTIAL_FAILED\"]}")
                // 告警组按告警事件、历史组按消息 ID 关联投递，不适用一侧用占位值避免空值误匹配
                .contains("\"_byAlarm\": {\"$cond\": [\"$_isAlarm\", \"$_id\", \"-\"]}")
                .contains("\"_byMessage\": {\"$cond\": [\"$_isAlarm\", \"-\", \"$_messageId\"]}")
                .contains("\"foreignField\": \"alarmEventId\"").contains("\"foreignField\": \"messageId\"");
        query(q -> q.setChannel(List.of(DeliveryChannel.EMAIL)));
        assertThat(lastPipeline()).contains("\"_deliveryChannels\": {\"$in\": [\"EMAIL\"]}");
        query(q -> q.setDeliveryStatus(List.of(DeliverySummaryVo.NONE)));
        assertThat(lastPipeline()).contains("\"_deliveryStatus\": {\"$in\": [\"NONE\"]}");
        query(q -> q.setRecipient(" C@X.COM "));
        assertThat(lastPipeline()).contains("\"$indexOfCP\"").contains("\"c@x.com\"").contains("\"$anyElementTrue\"");
        query(q -> {
            q.setTab(NotificationRecordQuery.TAB_FAILED);
            q.setChannel(List.of(DeliveryChannel.EMAIL));
        });
        assertThat(lastPipeline()).contains("\"$and\": [{\"_deliveryStatus\"");

        query(q -> {
            q.setKeyword("订单");
            q.setLevel(List.of(Level.WARNING));
            q.setStart(1L);
            q.setAlarmEventId(ALARM_A);
        });
        assertThat(lastPipeline()).contains("\"msg\": \"alarm\"").contains("\"user_id\": \"owner\"")
                .contains("messageMetadata.name").contains("\"level\": {\"$in\": [\"WARNING\"]}").contains("createTime")
                .contains("\"alarmEventId\": \"" + ALARM_A + "\"");
        assertThat(lastMessageQuery.get().getQueryObject().toJson()).contains("messageMetadata.name").contains("\"user_id\": \"owner\"");
    }

    @Test
    @DisplayName("AC5 只读取站内告警通知：许可证与系统通知不进入通知记录；有通知模块权限可查看全部用户")
    void should_only_read_alarm_messages() {
        service.list(new NotificationRecordQuery(), owner, Locale.CHINA);
        assertThat(lastPipeline()).contains("\"msg\": \"alarm\"").contains("\"isDeleted\": {\"$ne\": true}").contains("\"user_id\": \"owner\"")
                .doesNotContain(MsgTypeEnum.EXPIRING.getValue());

        service.list(new NotificationRecordQuery(), root, Locale.CHINA);
        assertThat(lastPipeline()).doesNotContain("\"user_id\": \"root\"").contains("\"$eq\": [\"$user_id\", \"root\"]");
    }

    @Test
    @DisplayName("AC5 铃铛角标：按告警聚合计数当前用户未读，且只计自己的告警通知")
    void should_count_unread_groups() {
        givenAlarmWithThreeNotifications();
        messages.add(message(null, "旧任务", false, new Date(500)));

        assertThat(service.unreadCount(root)).isEqualTo(2);
        String json = lastMessageQuery.get().getQueryObject().toJson();
        assertThat(json).contains("\"user_id\": \"root\"").contains("\"read\": {\"$ne\": true}").contains("\"msg\": \"alarm\"");
    }

    @Test
    @DisplayName("AC3 详情：按批次列出每个渠道、收件人的结果与发送时的发生次数")
    void should_list_batches_in_detail() {
        givenAlarmWithThreeNotifications();

        NotificationRecordDetailVo detail = service.detail(ALARM_A, owner, Locale.CHINA);

        assertThat(detail.getBatches()).hasSize(2);
        NotificationRecordDetailVo.Batch latest = detail.getBatches().get(0);
        assertThat(latest.getBatchId()).isEqualTo("b2");
        assertThat(latest.getOccurrenceCountSnapshot()).isEqualTo(30);
        assertThat(latest.getRecipients()).extracting(NotificationRecordDetailVo.Recipient::getAddress)
                .containsExactlyInAnyOrder("owner", "a@x.com", "b@x.com", "c@x.com");
        assertThat(latest.getRecipients()).filteredOn(r -> "c@x.com".equals(r.getAddress()))
                .singleElement().satisfies(r -> {
                    assertThat(r.getStatus()).isEqualTo(DeliveryStatus.FAILED);
                    assertThat(r.getFailureMessage()).isEqualTo("550 mailbox unavailable");
                });
        assertThat(detail.getBatches().get(1).getRecipients()).extracting(NotificationRecordDetailVo.Recipient::getSkipReason)
                .contains("BLACKLIST");
        messages.clear();
        assertThatThrownBy(() -> service.detail(ALARM_A, owner, Locale.CHINA))
                .isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Notification.NotFound"));
    }

    @Test
    @DisplayName("AC4 已读/未读：只改当前用户自己的记录，聚合行作用于该告警全部消息，并写审计")
    void should_update_own_read_state_with_audit() {
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        when(mongo.updateMulti(query.capture(), update.capture(), eq(MessageEntity.class))).thenReturn(UpdateResult.acknowledged(3, 3L, null));

        String legacyId = NotificationRecordService.LEGACY_PREFIX + new ObjectId().toHexString();
        long modified = service.updateReadState(List.of(ALARM_A, legacyId), false, root, true);

        assertThat(modified).isEqualTo(3);
        String json = query.getValue().getQueryObject().toJson();
        assertThat(json).contains("\"user_id\": \"root\"").contains("\"alarmEventId\": \"" + ALARM_A + "\"").contains("\"msg\": \"alarm\"");
        assertThat(update.getValue().getUpdateObject().toJson()).contains("\"read\": false").contains("\"readAt\": null");
        verify(audit).addUserLog(eq(Modular.MESSAGE), eq(Operation.UNREAD), eq(root), isNull(), eq("2"));

        service.updateReadState(List.of(ALARM_A), true, owner, false);
        verify(audit).addUserLog(eq(Modular.MESSAGE), eq(Operation.READ), eq(owner), eq(ALARM_A), eq(ALARM_A));
        // 历史通知审计来源取消息 ID，去掉前缀才能写入审计
        service.updateReadState(List.of(legacyId), true, owner, false);
        verify(audit).addUserLog(eq(Modular.MESSAGE), eq(Operation.READ), eq(owner),
                eq(legacyId.substring(NotificationRecordService.LEGACY_PREFIX.length())), eq(legacyId));
        assertThat(NotificationRecordService.auditSourceId("not-an-id")).isNull();
    }

    @Test
    @DisplayName("AC4 批量范围必须明确：空集合或超过上限拒绝，预览返回当前筛选下的数量与 ID")
    void should_require_explicit_batch_scope() {
        assertThatThrownBy(() -> service.updateReadState(List.of(), true, owner, true)).isInstanceOf(BizException.class);
        List<String> tooMany = IntStream.range(0, NotificationRecordService.MAX_BATCH + 1)
                .mapToObj(i -> new ObjectId().toHexString()).collect(Collectors.toList());
        assertThatThrownBy(() -> service.updateReadState(tooMany, true, owner, true)).isInstanceOf(BizException.class);
        verify(mongo, never()).updateMulti(any(Query.class), any(Update.class), eq(MessageEntity.class));

        // 预览只取本人可修改且状态需要变化的分组，数量在库内统计，ID 最多返回上限条
        pageKeys = List.of(ALARM_A);
        pageTotal = 1L;
        ReadStatePreviewVo preview = service.previewReadState(new NotificationRecordQuery(), true, owner, Locale.CHINA);
        assertThat(preview.getCount()).isEqualTo(1);
        assertThat(preview.getIds()).containsExactly(ALARM_A);
        assertThat(preview.isTruncated()).isFalse();
        assertThat(lastPipeline()).contains("\"$match\": {\"_own\": 1, \"_ownUnread\": 1}")
                .contains("\"$limit\": " + NotificationRecordService.MAX_BATCH);
        service.previewReadState(tab(NotificationRecordQuery.TAB_UNREAD), false, owner, Locale.CHINA);
        assertThat(lastPipeline()).contains("\"$match\": {\"_ownUnread\": 1}").contains("\"$match\": {\"_own\": 1, \"_ownUnread\": 0}");
        pageTotal = NotificationRecordService.MAX_BATCH + 1L;
        preview = service.previewReadState(new NotificationRecordQuery(), true, owner, Locale.CHINA);
        assertThat(preview.getCount()).isEqualTo(NotificationRecordService.MAX_BATCH + 1);
        assertThat(preview.isTruncated()).isTrue();
        verify(audit, never()).addUserLog(any(Modular.class), any(Operation.class), any(UserDetail.class), anyString(), anyString());
    }

    @Test
    @DisplayName("AC10 通知模块权限：可见全部用户记录但阅读状态只属于本人；无权限仅本人；云版仅本人")
    void should_scope_by_notification_module_permission() {
        givenAlarmWithThreeNotifications();

        // 有权限查看 owner 的记录：不显示对方未读，不可标记，也不进入未读页签和批量预览
        NotificationRecordVo record = service.list(new NotificationRecordQuery(), root, Locale.CHINA).getItems().get(0);
        assertThat(lastMessageQuery.get().getQueryObject().toJson()).doesNotContain("user_id");
        assertThat(record.getId()).isEqualTo(ALARM_A);
        assertThat(record.isReadable()).isFalse();
        assertThat(record.isUnread()).isFalse();
        // 未读页签与批量预览按当前用户本人的消息判定
        pageKeys = List.of();
        assertThat(ids(service.list(tab(NotificationRecordQuery.TAB_UNREAD), root, Locale.CHINA).getItems())).isEmpty();
        assertThat(lastPipeline()).contains("\"$eq\": [\"$user_id\", \"root\"]").contains("\"$match\": {\"_ownUnread\": 1}");
        assertThat(service.previewReadState(new NotificationRecordQuery(), true, root, Locale.CHINA).getCount()).isZero();
        pageKeys = null;
        NotificationRecordDetailVo detail = service.detail(ALARM_A, root, Locale.CHINA);
        assertThat(detail.isReadable()).isFalse();
        assertThat(detail.getReadAt()).isNull();

        // 收件人本人无模块权限：仅查看自己的，阅读状态正常
        record = service.list(new NotificationRecordQuery(), owner, Locale.CHINA).getItems().get(0);
        assertThat(lastMessageQuery.get().getQueryObject().toJson()).contains("\"user_id\": \"owner\"");
        assertThat(record.isReadable()).isTrue();
        assertThat(record.isUnread()).isTrue();

        // 云版为多租户，即使有权限也只查看自己的
        when(settings.isCloud()).thenReturn(true);
        service.list(new NotificationRecordQuery(), root, Locale.CHINA);
        assertThat(lastMessageQuery.get().getQueryObject().toJson()).contains("\"user_id\": \"root\"");
    }

    private static NotificationRecordQuery tab(String tab) {
        NotificationRecordQuery q = new NotificationRecordQuery();
        q.setTab(tab);
        return q;
    }

    private void givenAlarmWithThreeNotifications() {
        AlarmInfo alarm = new AlarmInfo();
        alarm.setId(new ObjectId(ALARM_A));
        alarm.setTaskId("64c000000000000000000001");
        alarm.setName("订单同步");
        alarm.setMetric(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        alarm.setStatus(AlarmStatusEnum.ING);
        alarm.setTally(37);
        alarm.setFirstOccurrenceTime(new Date(1000));
        alarm.setLastOccurrenceTime(new Date(9000));
        alarmData.add(alarm);
        messages.add(message(ALARM_A, "订单同步", false, new Date(3000)));
        messages.add(message(ALARM_A, "订单同步", true, new Date(2000)));
        messages.add(message(ALARM_A, "订单同步", true, new Date(1000)));
        deliveries.add(delivery("b1", DeliveryChannel.SYSTEM, "owner", DeliveryStatus.SUCCESS, null, 12, new Date(1000)));
        deliveries.add(delivery("b1", DeliveryChannel.EMAIL, "z@x.com", DeliveryStatus.SKIPPED, null, 12, null));
        deliveries.get(1).setSkipReason("BLACKLIST");
        deliveries.add(delivery("b2", DeliveryChannel.SYSTEM, "owner", DeliveryStatus.SUCCESS, null, 30, new Date(3000)));
        deliveries.add(delivery("b2", DeliveryChannel.EMAIL, "a@x.com", DeliveryStatus.SUCCESS, null, 30, new Date(3000)));
        deliveries.add(delivery("b2", DeliveryChannel.EMAIL, "b@x.com", DeliveryStatus.SUCCESS, null, 30, new Date(3000)));
        deliveries.add(delivery("b2", DeliveryChannel.EMAIL, "c@x.com", DeliveryStatus.FAILED, "550 mailbox unavailable", 30, new Date(3000)));
    }

    private List<NotificationRecordVo> query(java.util.function.Consumer<NotificationRecordQuery> customizer) {
        NotificationRecordQuery query = new NotificationRecordQuery();
        customizer.accept(query);
        return service.list(query, owner, Locale.CHINA).getItems();
    }

    private static List<String> ids(List<NotificationRecordVo> records) {
        return records.stream().map(NotificationRecordVo::getId).collect(Collectors.toList());
    }

    private static MessageEntity message(String alarmId, String taskName, boolean read, Date time) {
        MessageEntity m = new MessageEntity();
        m.setId(new ObjectId());
        m.setMsg(MsgTypeEnum.ALARM.getValue());
        m.setAlarmEventId(alarmId);
        m.setTitle(taskName + " 增量延迟");
        m.setLevel(Level.WARNING.name());
        m.setRead(read);
        m.setUserId("owner");
        m.setCreateAt(time);
        m.setMessageMetadata(new MessageMetadata(taskName, "64c000000000000000000001"));
        return m;
    }

    private static NotificationDelivery delivery(String batch, DeliveryChannel channel, String address, DeliveryStatus status,
                                                 String failure, Integer occurrence, Date sentAt) {
        NotificationDelivery d = new NotificationDelivery();
        d.setAlarmEventId(ALARM_A);
        d.setBatchId(batch);
        d.setChannel(channel);
        d.setRecipientAddress(address);
        d.setStatus(status);
        d.setFailureMessage(failure);
        d.setOccurrenceCountSnapshot(occurrence);
        d.setSentAt(sentAt);
        d.setCreateAt(sentAt == null ? new Date(1000) : sentAt);
        return d;
    }
}
