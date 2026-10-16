package com.tapdata.tm.message.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarmMail.service.AlarmMailService;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.events.service.EventsService;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemNotificationMapping;
import com.tapdata.tm.message.dto.MessageDto;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.repository.MessageRepository;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.notification.record.dto.SystemNotificationInventoryVo;
import com.tapdata.tm.notification.record.service.NotificationRecordService;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.SendStatus;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2: real MessageServiceImpl write decisions and system notification mapping; storage, mail and settings are boundaries. */
@Timeout(10)
class SystemNotificationRetireIT {
    private static final String SETTINGS = "{\"runNotification\":["
            + "{\"label\":\"jobStarted\",\"notice\":true,\"email\":false},"
            + "{\"label\":\"jobEncounterError\",\"notice\":true,\"email\":true},"
            + "{\"label\":\"jobPaused\",\"notice\":true,\"email\":false},"
            + "{\"label\":\"inspectError\",\"notice\":true,\"email\":true},"
            + "{\"label\":\"inspectValue\",\"notice\":true,\"email\":false}]}";

    private GenericApplicationContext context;
    private MessageServiceImpl service;
    private MessageRepository repository;
    private EventsService events;
    private MailUtils mail;
    private final List<MessageEntity> saved = new ArrayList<>();
    private UserDetail user;

    @BeforeEach
    void setUp() {
        repository = mock(MessageRepository.class);
        events = mock(EventsService.class);
        mail = mock(MailUtils.class);
        SettingsService settings = mock(SettingsService.class);
        Settings notification = new Settings();
        notification.setValue(SETTINGS);
        when(settings.getByCategoryAndKey(CategoryEnum.NOTIFICATION, KeyEnum.NOTIFICATION)).thenReturn(notification);
        when(settings.isCloud()).thenReturn(true);
        when(repository.save(any(MessageEntity.class), any(UserDetail.class))).thenAnswer(i -> {
            MessageEntity entity = i.getArgument(0);
            entity.setId(new ObjectId());
            saved.add(entity);
            return entity;
        });
        when(repository.count(any(Query.class))).thenReturn(0L);
        when(mail.sendHtmlMail(anyList(), anyString(), anyString(), any(), any(), anyString())).thenReturn(new SendStatus("true", ""));
        user = new UserDetail("u1", "customer", "user1", "unused", List.of());
        user.setEmail("u1@x.com");

        context = new GenericApplicationContext();
        context.registerBean(MessageServiceImpl.class, () -> {
            MessageServiceImpl impl = new MessageServiceImpl(repository);
            ReflectionTestUtils.setField(impl, "messageRepository", repository);
            ReflectionTestUtils.setField(impl, "settingsService", settings);
            ReflectionTestUtils.setField(impl, "eventsService", events);
            ReflectionTestUtils.setField(impl, "mailUtils", mail);
            ReflectionTestUtils.setField(impl, "alarmMailService", mock(AlarmMailService.class));
            return impl;
        });
        context.refresh();
        service = context.getBean(MessageServiceImpl.class);
    }

    @AfterEach
    void tearDown() {
        context.close();
        saved.clear();
    }

    @Test
    @DisplayName("AC6 未配置邮件的任务事件不再生成站内系统通知")
    void should_not_write_notice_only_events() {
        service.addSync("job", "task-1", MsgTypeEnum.STARTED, "", Level.INFO, user);
        service.addInspect("inspect", "inspect-1", MsgTypeEnum.INSPECT_VALUE, Level.WARN, user);
        service.addMigration("job", "task-1", MsgTypeEnum.PAUSED, Level.INFO, user);

        assertThat(saved).isEmpty();
        verify(events, never()).recordEvents(anyString(), anyString(), anyString(), anyString(), anyString(), any(SendStatus.class), anyInt(), anyString());
    }

    @Test
    @DisplayName("AC6 配置邮件的任务出错仍发送邮件并记录事件，消息仅作隐藏载体")
    void should_keep_email_with_hidden_carrier() {
        service.addSync("job", "task-1", MsgTypeEnum.STOPPED_BY_ERROR, "", Level.ERROR, user);

        assertThat(saved).singleElement().satisfies(m -> {
            assertThat(m.getIsDeleted()).isTrue();
            assertThat(m.getMsg()).isEqualTo(MsgTypeEnum.STOPPED_BY_ERROR.getValue());
        });
        verify(mail).sendHtmlMail(anyList(), eq("user1"), eq("job"), any(), eq(MsgTypeEnum.STOPPED_BY_ERROR), eq("task-1"));
        verify(events).recordEvents(anyString(), anyString(), eq("u1@x.com"), eq(saved.get(0).getId().toString()), eq("u1"),
                any(SendStatus.class), eq(0), anyString());
    }

    @Test
    @DisplayName("AC6 校验出错：邮件开启时保存隐藏载体并记录邮件事件")
    void should_keep_inspect_email() {
        service.addInspect("inspect", "inspect-1", MsgTypeEnum.INSPECT_ERROR, Level.ERROR, user);

        assertThat(saved).singleElement().satisfies(m -> assertThat(m.getIsDeleted()).isTrue());
        verify(events).recordEvents(anyString(), anyString(), eq("u1@x.com"), eq(saved.get(0).getId().toString()), eq("u1"),
                any(SendStatus.class), eq(0), anyString());
    }

    @Test
    @DisplayName("AC6 Agent 状态消息仍保存但对站内隐藏；告警通知保持可见")
    void should_hide_agent_message_but_keep_alarm() {
        MessageDto agent = new MessageDto();
        agent.setMsg(MsgTypeEnum.CONNECTED.getValue());
        agent.setMessageMetadata("{\"name\":\"agent-a\",\"id\":\"a1\"}");
        service.add(agent, user);
        MessageEntity alarm = new MessageEntity();
        alarm.setMsg(MsgTypeEnum.ALARM.getValue());
        service.addMessage(alarm, user);

        assertThat(saved).hasSize(2);
        assertThat(saved.get(0).getIsDeleted()).isTrue();
        assertThat(saved.get(1).getIsDeleted()).isNull();
    }

    @Test
    @DisplayName("AC6 §9.2 映射覆盖全部消息类型，只有告警通知保留在通知记录")
    void should_map_every_message_type() {
        assertThat(SystemNotificationMapping.targets().keySet()).containsExactlyInAnyOrder(MsgTypeEnum.values());
        assertThat(SystemNotificationMapping.targetOf(MsgTypeEnum.DELETED)).isEqualTo(SystemNotificationMapping.Target.AUDIT);
        assertThat(SystemNotificationMapping.targetOf(MsgTypeEnum.STOPPED_BY_ERROR)).isEqualTo(SystemNotificationMapping.Target.ALARM);
        assertThat(SystemNotificationMapping.targetOf(MsgTypeEnum.CONNECTION_INTERRUPTED)).isEqualTo(SystemNotificationMapping.Target.ALARM);
        assertThat(SystemNotificationMapping.targetOf(MsgTypeEnum.EXPIRING)).isEqualTo(SystemNotificationMapping.Target.PLATFORM_NOTICE);
        assertThat(Arrays.stream(MsgTypeEnum.values()).filter(SystemNotificationMapping::isRetiredSystemNotification))
                .doesNotContain(MsgTypeEnum.ALARM).hasSize(MsgTypeEnum.values().length - 1);
        assertThat(SystemNotificationMapping.licenseMsgValues()).containsExactlyInAnyOrder("expired", "expiring");
    }

    @Test
    @DisplayName("AC6 历史系统通知只读盘点：按类型统计数量、未读数与新归属")
    void should_inventory_history() {
        MongoTemplate mongo = mock(MongoTemplate.class);
        List<Document> rows = List.of(new Document("_id", "deleted").append("count", 12).append("unread", 3),
                new Document("_id", "expiring").append("count", 40).append("unread", 0));
        when(mongo.aggregate(any(Aggregation.class), eq(MessageEntity.class), eq(Document.class)))
                .thenReturn(new AggregationResults<>(rows, new Document()));
        NotificationRecordService records = new NotificationRecordService(mongo, mock(NotificationDeliveryService.class), mock(UserLogService.class));

        List<SystemNotificationInventoryVo> inventory = records.systemNotificationInventory();

        assertThat(inventory).extracting(SystemNotificationInventoryVo::getMsg).containsExactly("expiring", "deleted");
        assertThat(inventory.get(1).getTarget()).isEqualTo(SystemNotificationMapping.Target.AUDIT);
        assertThat(inventory.get(1).getUnread()).isEqualTo(3);
        assertThat(inventory.get(0).getTarget()).isEqualTo(SystemNotificationMapping.Target.PLATFORM_NOTICE);
    }
}
