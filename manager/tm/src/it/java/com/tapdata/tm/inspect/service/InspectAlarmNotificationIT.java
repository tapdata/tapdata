package com.tapdata.tm.inspect.service;

import cn.hutool.json.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.dto.TestResponseDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.inspect.entity.InspectResultEntity;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.inspect.repository.InspectResultRepository;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryServiceImpl;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MailUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2：校验状态与结果告警跨模块协作（InspectServiceImpl + InspectResultServiceImpl + AlarmServiceImpl + NotificationDeliveryServiceImpl），
 * MongoDB 与邮件发送网关作为外部边界。
 */
@Timeout(15)
class InspectAlarmNotificationIT {

    @Test
    @DisplayName("混合校验结果与首次报错同时触发站内/邮件告警及投递记录；运行期和重报不重发，用户停止不误报")
    void should_trigger_both_error_and_value_alarms_without_duplicates_or_user_stop_false_positives() throws Exception {
        Class<?> alarmImplClass;
        Class<?> saverClass;
        Class<?> inspectImplClass;
        Class<?> inspectResultImplClass;
        try {
            alarmImplClass = Class.forName("com.tapdata.tm.alarm.service.impl.AlarmServiceImpl");
            saverClass = Class.forName("com.tapdata.tm.alarm.AlarmInfoSaver");
            inspectImplClass = Class.forName("com.tapdata.tm.inspect.service.InspectServiceImpl");
            inspectResultImplClass = Class.forName("com.tapdata.tm.inspect.service.InspectResultServiceImpl");
        } catch (ClassNotFoundException missingImplementation) {
            Assumptions.assumeTrue(false, "校验告警L2 IT需要可用的 enterprise 实现");
            return;
        }
        Assumptions.assumeTrue(isEnterpriseImplementation(alarmImplClass)
                        && isEnterpriseImplementation(inspectImplClass)
                        && isEnterpriseImplementation(inspectResultImplClass),
                "校验告警L2 IT需要 enterprise profile");

        String userId = "64b000000000000000000011";
        ObjectId inspectId = new ObjectId("6afeb16c7c374625f0b7aec5");
        ObjectId inspectResultId = new ObjectId("6afeb2097c374625f0b7b8d4");
        List<String> expectedRecipients = List.of("a@example.com", "b@example.com", "c@example.com");

        UserDetail owner = new UserDetail(userId, "customer", "martin", "pwd",
                List.of(new SimpleGrantedAuthority("ADMIN")));
        AlarmSettingVO taskErrorSetting = settingVO(AlarmKeyEnum.INSPECT_TASK_ERROR, Map.of());
        AlarmSettingVO valueErrorSetting = settingVO(AlarmKeyEnum.INSPECT_VALUE_ERROR, Map.of("maxDifferentialValues", 0));

        InspectDto inspectDto = new InspectDto();
        inspectDto.setId(inspectId);
        inspectDto.setName("martin-1119");
        inspectDto.setAgentId("agent-184");
        inspectDto.setUserId(userId);
        inspectDto.setInspectMethod("field");
        inspectDto.setErrorMsg(null);
        inspectDto.setAlarmSettings(List.of(taskErrorSetting, valueErrorSetting));
        inspectDto.setAlarmReceivers(List.of(
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "a@example.com"),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "b@example.com"),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "c@example.com")
        ));
        inspectDto.setEmailReceivers(expectedRecipients);

        InspectEntity storedInspectEntity = new InspectEntity();
        storedInspectEntity.setId(inspectId);
        storedInspectEntity.setName("martin-1119");
        storedInspectEntity.setAgentId("agent-184");
        storedInspectEntity.setUserId(userId);
        storedInspectEntity.setInspectMethod("field");
        storedInspectEntity.setErrorMsg(null);
        storedInspectEntity.setAlarmSettings(inspectDto.getAlarmSettings());
        storedInspectEntity.setAlarmReceivers(inspectDto.getAlarmReceivers());
        storedInspectEntity.setEmailReceivers(expectedRecipients);

        AtomicReference<InspectResultEntity> storedResult = new AtomicReference<>();
        List<AlarmInfo> alarmStore = new ArrayList<>();
        List<MessageEntity> sentMessages = new ArrayList<>();
        List<NotificationDelivery> deliveries = new ArrayList<>();
        List<List<String>> mailedRecipients = new ArrayList<>();

        MongoTemplate mongo = mock(MongoTemplate.class);
        Settings cursor = new Settings();
        cursor.setValue(new Date(System.currentTimeMillis() - 60000));
        when(mongo.findOne(any(Query.class), eq(Settings.class))).thenReturn(cursor);
        doAnswer(inv -> {
            Update update = inv.getArgument(1);
            Document set = (Document) update.getUpdateObject().get("$set");
            if (set != null && set.containsKey("value")) {
                cursor.setValue(set.get("value"));
            }
            return UpdateResult.acknowledged(1, 1L, null);
        }).when(mongo).updateFirst(any(Query.class), any(Update.class), eq(Settings.class));

        when(mongo.findOne(any(Query.class), eq(AlarmInfo.class))).thenAnswer(inv -> {
            Document query = inv.<Query>getArgument(0).getQueryObject();
            String metric = query.getString("metric");
            return alarmStore.stream()
                    .filter(a -> metric.equals(a.getMetric().name()))
                    .findFirst()
                    .orElse(null);
        });
        doAnswer(inv -> {
            AlarmInfo info = inv.getArgument(0);
            if (info.getId() == null) {
                info.setId(new ObjectId());
            }
            alarmStore.add(info);
            return info;
        }).when(mongo).insert(any(AlarmInfo.class));
        doAnswer(inv -> {
            AlarmInfo info = inv.getArgument(0);
            alarmStore.removeIf(a -> a.getId().equals(info.getId()));
            alarmStore.add(info);
            return info;
        }).when(mongo).save(any(AlarmInfo.class));
        when(mongo.find(any(Query.class), eq(AlarmInfo.class))).thenAnswer(inv -> {
            Document query = inv.<Query>getArgument(0).getQueryObject();
            Document window = (Document) query.get("lastNotifyTime");
            Date from = window.getDate("$gte");
            Date until = window.getDate("$lt");
            List<AlarmInfo> matched = new ArrayList<>();
            for (AlarmInfo info : alarmStore) {
                if (info.getLastNotifyTime() != null
                        && !info.getLastNotifyTime().before(from)
                        && info.getLastNotifyTime().before(until)) {
                    matched.add(info);
                }
            }
            return matched;
        });
        doAnswer(inv -> {
            deliveries.addAll(inv.getArgument(0, Collection.class));
            return List.of();
        }).when(mongo).insertAll(any(Collection.class));

        UserService userService = mock(UserService.class);
        when(userService.getUserByIdList(anyList())).thenReturn(List.of(owner));
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(owner);

        AlarmSettingService alarmSettingService = mock(AlarmSettingService.class);
        AlarmSettingDto sysTaskError = settingDto(AlarmKeyEnum.INSPECT_TASK_ERROR);
        AlarmSettingDto sysValueError = settingDto(AlarmKeyEnum.INSPECT_VALUE_ERROR);
        when(alarmSettingService.findAllAlarmSetting(owner)).thenReturn(List.of(sysTaskError, sysValueError));
        when(alarmSettingService.findByKey(AlarmKeyEnum.INSPECT_TASK_ERROR, owner)).thenReturn(sysTaskError);
        when(alarmSettingService.findByKey(AlarmKeyEnum.INSPECT_VALUE_ERROR, owner)).thenReturn(sysValueError);

        MessageService messageService = mock(MessageService.class);
        when(messageService.checkMessageLimit(owner)).thenReturn(0L);
        doAnswer(inv -> {
            MessageEntity entity = new MessageEntity();
            entity.setId(new ObjectId());
            sentMessages.add(entity);
            return entity;
        }).when(messageService).addMessage(any(MessageEntity.class), eq(owner));

        SettingsService settingsService = mock(SettingsService.class);
        when(settingsService.isCloud()).thenReturn(true);
        when(settingsService.getMailAccount(userId)).thenReturn(new MailAccountDto(
                "127.0.0.1", 465, "noreply@example.com", "noreply@example.com",
                "secret", List.of("default@example.com"), "SSL", "", 0));

        InspectRepository inspectRepository = mock(InspectRepository.class);
        when(inspectRepository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        when(inspectRepository.findById(any(ObjectId.class), any(Field.class)))
                .thenAnswer(inv -> Optional.of(copyEntity(storedInspectEntity)));
        when(inspectRepository.findAll(any(JSONObject.class))).thenAnswer(inv -> List.of(copyEntity(storedInspectEntity)));
        when(inspectRepository.findAll(any(Query.class))).thenAnswer(inv -> List.of(copyEntity(storedInspectEntity)));
        when(inspectRepository.findOne(any(Query.class))).thenAnswer(inv -> Optional.of(copyEntity(storedInspectEntity)));
        when(inspectRepository.updateByWhere(any(Query.class), any(InspectEntity.class), eq(owner))).thenAnswer(inv -> {
            InspectEntity update = inv.getArgument(1);
            storedInspectEntity.setStatus(update.getStatus());
            storedInspectEntity.setErrorMsg(update.getErrorMsg());
            return UpdateResult.acknowledged(1, 1L, null);
        });
        when(inspectRepository.updateFirst(any(Query.class), any(Update.class), eq(owner)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(inspectRepository.update(any(Query.class), any(Update.class), eq(owner)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        InspectResultRepository resultRepository = mock(InspectResultRepository.class);
        when(resultRepository.filterToQuery(any(Filter.class))).thenReturn(new Query());
        when(resultRepository.findOne(any(Where.class), eq(owner)))
                .thenAnswer(inv -> Optional.ofNullable(storedResult.get()));
        when(resultRepository.findById(any(ObjectId.class), any(Field.class)))
                .thenAnswer(inv -> Optional.ofNullable(storedResult.get()));
        when(resultRepository.upsert(any(Query.class), any(InspectResultEntity.class), eq(owner)))
                .thenAnswer(inv -> {
                    InspectResultEntity entity = inv.getArgument(1);
                    if (entity.getId() == null) {
                        entity.setId(inspectResultId);
                    }
                    storedResult.set(entity);
                    return 1L;
                });

        InspectService inspectService = (InspectService) inspectImplClass
                .getDeclaredConstructor(InspectRepository.class).newInstance(inspectRepository);
        AlarmService alarmService = (AlarmService) alarmImplClass.getDeclaredConstructor().newInstance();
        Object saver = saverClass.getDeclaredConstructor().newInstance();
        ReflectionTestUtils.setField(saver, "userService", userService);
        ReflectionTestUtils.setField(saver, "mongoTemplate", mongo);
        ReflectionTestUtils.setField(saver, "alarmSettingService", alarmSettingService);

        ReflectionTestUtils.setField(alarmService, "mongoTemplate", mongo);
        ReflectionTestUtils.setField(alarmService, "alarmInfoSaver", saver);
        ReflectionTestUtils.setField(alarmService, "alarmSettingService", alarmSettingService);
        ReflectionTestUtils.setField(alarmService, "userService", userService);
        ReflectionTestUtils.setField(alarmService, "messageService", messageService);
        ReflectionTestUtils.setField(alarmService, "settingsService", settingsService);
        ReflectionTestUtils.setField(alarmService, "inspectService", inspectService);
        ReflectionTestUtils.setField(alarmService, "notificationDeliveryService", new NotificationDeliveryServiceImpl(mongo));
        ReflectionTestUtils.setField(alarmService, "taskAlterWebHookSender",
                mock(Class.forName("com.tapdata.tm.webhook.impl.convert.TaskAlterWebHookSender")));

        ReflectionTestUtils.setField(inspectService, "alarmService", alarmService);
        InspectResultService resultService = (InspectResultService) inspectResultImplClass
                .getDeclaredConstructor(InspectResultRepository.class).newInstance(resultRepository);
        ReflectionTestUtils.setField(resultService, "inspectService", inspectService);
        ReflectionTestUtils.setField(resultService, "alarmService", alarmService);

        // 1. 运行期周期上报（end == null）不触发差异告警
        SaveInspectResultParam periodic = mixedResultParam(inspectId, null);
        resultService.upsertInspectResultByWhere(new Where(), periodic, owner);
        assertThat(alarmStore).isEmpty();

        // 2. 终态上报混合结果 + 首轮报错状态上报，触发两类告警及邮件投递
        long terminalEnd = System.currentTimeMillis();
        resultService.upsertInspectResultByWhere(new Where(), mixedResultParam(inspectId, terminalEnd), owner);

        InspectDto errorStatusUpdate = new InspectDto();
        errorStatusUpdate.setStatus("error");
        errorStatusUpdate.setErrorMsg("Command failed with error 292 (QueryExceededMemoryLimitNoDiskUseAllowed)");
        Where inspectWhere = new Where();
        inspectWhere.put("id", inspectId.toHexString());
        inspectService.updateByWhere(inspectWhere, errorStatusUpdate, owner);

        assertThat(alarmStore).extracting(AlarmInfo::getMetric)
                .containsExactlyInAnyOrder(AlarmKeyEnum.INSPECT_VALUE_ERROR, AlarmKeyEnum.INSPECT_TASK_ERROR);

        try (MockedStatic<MailUtils> mail = Mockito.mockStatic(MailUtils.class)) {
            mail.when(() -> MailUtils.sendHtmlEmail(any(), anyList(), anyString(), anyString())).thenAnswer(inv -> {
                List<String> recipients = new ArrayList<>(inv.getArgument(1));
                mailedRecipients.add(recipients);
                return new TestResponseDto(true, null, recipients, List.of());
            });

            alarmImplClass.getMethod("notifyOnce").invoke(alarmService);
            assertThat(sentMessages).hasSize(2);
            assertThat(mailedRecipients).hasSize(2);
            assertThat(mailedRecipients.get(0)).containsExactlyInAnyOrderElementsOf(expectedRecipients);
            assertThat(mailedRecipients.get(1)).containsExactlyInAnyOrderElementsOf(expectedRecipients);
            assertThat(deliveries).filteredOn(d -> d.getChannel() == DeliveryChannel.EMAIL && d.getStatus() == DeliveryStatus.SUCCESS)
                    .hasSize(6);

            // 3. 同一终态结果重报 & 用户主动停止，不产生新通知与投递
            deliveries.clear();
            mailedRecipients.clear();
            sentMessages.clear();

            resultService.upsertInspectResultByWhere(new Where(), mixedResultParam(inspectId, terminalEnd), owner);
            InspectDto stopUpdate = new InspectDto();
            stopUpdate.setStatus("error");
            stopUpdate.setErrorMsg("Exit of user stop");
            inspectService.updateByWhere(inspectWhere, stopUpdate, owner);

            alarmImplClass.getMethod("notifyOnce").invoke(alarmService);
            assertThat(sentMessages).isEmpty();
            assertThat(mailedRecipients).isEmpty();
            assertThat(deliveries).isEmpty();
        }
    }

    private static SaveInspectResultParam mixedResultParam(ObjectId inspectId, Long endMillis) {
        SaveInspectResultParam param = new SaveInspectResultParam();
        param.setInspect_id(inspectId.toHexString());
        param.setStatus(InspectResultDto.STATUS_ERROR);
        param.setStart(1700000000000L);
        param.setEnd(endMillis);
        param.setSpendMilli(100);
        param.setJudgeAutomaticCheck(false);
        Stats err = new Stats();
        err.setTaskId("patient");
        err.setStatus(InspectStatusEnum.ERROR.getValue());
        err.setErrorMsg("error 292");
        Stats diff = new Stats();
        diff.setTaskId("pmi_case");
        diff.setStatus(InspectStatusEnum.DONE.getValue());
        diff.setResult(InspectResultEnum.FAILED.getValue());
        diff.setSourceOnly(699597L);
        diff.setTargetOnly(0L);
        diff.setRowFailed(0L);
        param.setStats(List.of(err, diff));
        return param;
    }

    private static AlarmSettingVO settingVO(AlarmKeyEnum key, Map<String, Object> params) {
        AlarmSettingVO vo = new AlarmSettingVO();
        vo.setKey(key);
        vo.setOpen(true);
        vo.setNotify(List.of(NotifyEnum.SYSTEM, NotifyEnum.EMAIL));
        vo.setParams(params);
        return vo;
    }

    private static AlarmSettingDto settingDto(AlarmKeyEnum key) {
        AlarmSettingDto dto = new AlarmSettingDto();
        dto.setKey(key);
        dto.setOpen(true);
        dto.setNotify(List.of(NotifyEnum.SYSTEM, NotifyEnum.EMAIL));
        return dto;
    }

    private static InspectEntity copyEntity(InspectEntity src) {
        InspectEntity copy = new InspectEntity();
        copy.setId(src.getId());
        copy.setName(src.getName());
        copy.setAgentId(src.getAgentId());
        copy.setUserId(src.getUserId());
        copy.setInspectMethod(src.getInspectMethod());
        copy.setErrorMsg(src.getErrorMsg());
        copy.setStatus(src.getStatus());
        copy.setAlarmSettings(src.getAlarmSettings());
        copy.setAlarmReceivers(src.getAlarmReceivers());
        copy.setEmailReceivers(src.getEmailReceivers());
        return copy;
    }

    private static boolean isEnterpriseImplementation(Class<?> type) {
        CodeSource source = type.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null) {
            return false;
        }
        String location = source.getLocation().toString();
        return location.contains("tm-enterprise") || location.contains("tapdata-enterprise");
    }
}
