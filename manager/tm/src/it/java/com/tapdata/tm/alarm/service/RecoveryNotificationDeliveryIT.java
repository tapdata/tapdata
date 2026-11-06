package com.tapdata.tm.alarm.service;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import org.springframework.test.util.ReflectionTestUtils;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryServiceImpl;
import com.tapdata.tm.user.service.UserService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2：真实扫描和投递记录组件组装，Mongo与消息发送作为外部边界。 */
@Timeout(10)
class RecoveryNotificationDeliveryIT {
    @Test
    @DisplayName("恢复投递：RECOVER事件进入扫描、写入投递记录且游标推进后不重发")
    void should_deliver_recovery_once() throws Exception {
        Class<?> implementation;
        try {
            implementation = Class.forName("com.tapdata.tm.alarm.service.impl.AlarmServiceImpl");
        } catch (ClassNotFoundException missingImplementation) {
            Assumptions.assumeTrue(false, "恢复投递IT需要可用的告警服务实现");
            return;
        }
        // 默认构建加载 OSS 实现，扫描与投递字段只由企业实仓提供；此时如实跳过，不记通过。
        Assumptions.assumeTrue(isEnterpriseImplementation(implementation),
                "恢复投递IT需要 enterprise profile");
        MongoTemplate mongo = mock(MongoTemplate.class);
        UserService users = mock(UserService.class);
        MessageService messages = mock(MessageService.class);
        AlarmSettingService settings = mock(AlarmSettingService.class);
        AlarmService service = (AlarmService) implementation.getDeclaredConstructor().newInstance();
        ReflectionTestUtils.setField(service, "mongoTemplate", mongo);
        ReflectionTestUtils.setField(service, "taskService", mock(com.tapdata.tm.task.service.TaskService.class));
        ReflectionTestUtils.setField(service, "userService", users);
        ReflectionTestUtils.setField(service, "messageService", messages);
        ReflectionTestUtils.setField(service, "alarmSettingService", settings);
        ReflectionTestUtils.setField(service, "notificationDeliveryService", new NotificationDeliveryServiceImpl(mongo));
        UserDetail admin = new UserDetail("64b000000000000000000001", "customer", "operator", "pwd",
                List.of(new com.tapdata.tm.config.security.SimpleGrantedAuthority("ADMIN")));
        when(users.loadAllUser()).thenReturn(List.of(admin));
        when(users.getUserByIdList(anyList())).thenReturn(List.of());
        AlarmSettingDto enabled = new AlarmSettingDto();
        enabled.setKey(AlarmKeyEnum.ENGINE_ONLINE);
        enabled.setOpen(true);
        enabled.setNotify(List.of(NotifyEnum.SYSTEM));
        when(settings.findAllAlarmSetting(admin)).thenReturn(List.of(enabled));

        AlarmInfo online = new AlarmInfo();
        online.setId(new ObjectId());
        online.setMetric(AlarmKeyEnum.ENGINE_ONLINE);
        online.setStatus(AlarmStatusEnum.RECOVER);
        online.setLevel(Level.RECOVERY);
        online.setType(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM);
        online.setAgentId("agent-1");
        online.setName("engine-1");
        online.setSummary("ENGINE_ONLINE");
        online.setParam(Map.of("agentName", "engine-1", "alarmDate", "2026-11-06 10:00:00"));
        online.setLastOccurrenceTime(new Date(System.currentTimeMillis() - 1000));
        online.setLastNotifyTime(online.getLastOccurrenceTime());
        Settings cursor = new Settings();
        cursor.setValue(new Date(System.currentTimeMillis() - 30000));
        when(mongo.findOne(any(Query.class), eq(Settings.class))).thenReturn(cursor);
        doAnswer(invocation -> {
            Update update = invocation.getArgument(1);
            cursor.setValue(((Document) update.getUpdateObject().get("$set")).get("value"));
            return null;
        }).when(mongo).updateFirst(any(Query.class), any(Update.class), eq(Settings.class));
        when(mongo.find(any(Query.class), eq(AlarmInfo.class))).thenAnswer(invocation -> {
            Document query = invocation.<Query>getArgument(0).getQueryObject();
            List<Document> statuses = query.getList("$or", Document.class);
            boolean acceptsRecovery = statuses != null && statuses.stream().anyMatch(condition ->
                    AlarmStatusEnum.RECOVER.equals(condition.get("status"))
                            && "ENGINE_ONLINE".equals(condition.get("metric")));
            Document window = (Document) query.get("lastNotifyTime");
            Date from = window.getDate("$gte");
            Date until = window.getDate("$lt");
            return acceptsRecovery && !online.getLastNotifyTime().before(from) && online.getLastNotifyTime().before(until)
                    ? List.of(online) : List.of();
        });
        List<NotificationDelivery> persisted = new ArrayList<>();
        doAnswer(invocation -> {
            persisted.addAll(invocation.getArgument(0));
            return List.of();
        }).when(mongo).insertAll(any(java.util.Collection.class));

        java.lang.reflect.Method notifyOnce = implementation.getMethod("notifyOnce");
        notifyOnce.invoke(service);
        notifyOnce.invoke(service);

        verify(messages, times(1)).addMessage(any(MessageEntity.class), eq(admin));
        assertEquals(1, persisted.size());
        assertEquals(online.getId().toHexString(), persisted.get(0).getAlarmEventId());
        assertTrue(((Date) cursor.getValue()).after(online.getLastNotifyTime()));
    }

    private static boolean isEnterpriseImplementation(Class<?> implementation) {
        CodeSource source = implementation.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            return false;
        }
        String location = source.getLocation().toString();
        return location.contains("tm-enterprise") || location.contains("tapdata-enterprise");
    }
}
