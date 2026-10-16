package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.alarm.dto.AlarmEventQuery;
import com.tapdata.tm.alarm.dto.AlarmEventStatsVo;
import com.tapdata.tm.alarm.dto.AlarmEventVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.notification.delivery.constant.DeliveryChannel;
import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryServiceImpl;
import com.tapdata.tm.task.service.TaskService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2: real query, visibility and delivery summary services; Mongo storage, task and inspect lookups are boundaries. */
@Timeout(10)
class AlarmEventQueryIT {
    private GenericApplicationContext context;
    private AlarmEventQueryService service;
    private MongoTemplate mongo;
    private TaskService tasks;
    private InspectService inspects;
    private AlarmService alarms;
    private final List<AlarmInfo> alarmData = new ArrayList<>();
    private final List<NotificationDelivery> deliveryData = new ArrayList<>();
    private final AtomicReference<Query> lastAlarmQuery = new AtomicReference<>();
    private static final String T1 = "64b000000000000000000001";
    private static final String T2 = "64b000000000000000000002";
    private static final String T3 = "64b000000000000000000003";
    private static final String I1 = "64b0000000000000000000a1";
    private UserDetail admin;
    private UserDetail member;

    @BeforeEach
    void setUp() {
        mongo = mock(MongoTemplate.class);
        tasks = mock(TaskService.class);
        inspects = mock(InspectService.class);
        alarms = mock(AlarmService.class);
        when(alarms.renderSummary(any(), any())).thenAnswer(i -> "rendered:" + i.<AlarmInfo>getArgument(0).getSummary());
        when(mongo.find(any(Query.class), eq(AlarmInfo.class))).thenAnswer(i -> {
            lastAlarmQuery.set(i.getArgument(0));
            return new ArrayList<>(alarmData);
        });
        when(mongo.count(any(Query.class), eq(AlarmInfo.class))).thenAnswer(i -> {
            lastAlarmQuery.set(i.getArgument(0));
            return (long) alarmData.size();
        });
        when(mongo.find(any(Query.class), eq(NotificationDelivery.class))).thenAnswer(i -> new ArrayList<>(deliveryData));
        admin = new UserDetail("admin", "customer", "admin", "unused", List.of(new SimpleGrantedAuthority("ADMIN")));
        member = new UserDetail("member", "customer", "member", "unused", List.of());

        context = new GenericApplicationContext();
        context.registerBean(NotificationDeliveryService.class, () -> new NotificationDeliveryServiceImpl(mongo));
        context.registerBean(AlarmEventQueryService.class, () -> new AlarmEventQueryService(mongo, tasks, inspects, alarms,
                context.getBean(NotificationDeliveryService.class)));
        context.refresh();
        service = context.getBean(AlarmEventQueryService.class);
    }

    @AfterEach
    void tearDown() {
        context.close();
        alarmData.clear();
        deliveryData.clear();
    }

    @Test
    @DisplayName("AC1 列表返回发生次数、对象类型、恢复/关闭、当前值/阈值与投递概览")
    void should_return_new_fields() {
        AlarmInfo alarm = taskAlarm(T1, "订单同步", null);
        alarm.setTally(37);
        alarm.setStatus(AlarmStatusEnum.CLOESE);
        alarm.setCloseTime(new Date(2000));
        alarm.setCloseBy("admin");
        alarm.setRecoveryTime(new Date(1500));
        alarm.setCurrentValue(120);
        alarm.setThreshold(60);
        alarmData.add(alarm);
        TaskDto task = task(T1, "订单同步");
        when(tasks.findAllTasksByIds(List.of(T1))).thenReturn(List.of(task));
        String id = alarm.getId().toHexString();
        deliveryData.add(delivery(id, DeliveryChannel.SYSTEM, "admin", DeliveryStatus.SUCCESS));
        deliveryData.add(delivery(id, DeliveryChannel.EMAIL, "a@x.com", DeliveryStatus.SUCCESS));
        deliveryData.add(delivery(id, DeliveryChannel.EMAIL, "b@x.com", DeliveryStatus.SUCCESS));
        deliveryData.add(delivery(id, DeliveryChannel.EMAIL, "c@x.com", DeliveryStatus.FAILED));

        Page<AlarmEventVo> page = service.list(new AlarmEventQuery(), admin, Locale.CHINA);

        assertThat(page.getTotal()).isEqualTo(1);
        AlarmEventVo vo = page.getItems().get(0);
        assertThat(vo.getOccurrenceCount()).isEqualTo(37);
        assertThat(vo.getObjectType()).isEqualTo(AlarmObjectType.TASK);
        assertThat(vo.getObjectName()).isEqualTo("订单同步");
        assertThat(vo.isObjectDeleted()).isFalse();
        assertThat(vo.getSyncType()).isEqualTo("migrate");
        assertThat(vo.getCloseBy()).isEqualTo("admin");
        assertThat(vo.getRecoveryTime()).isEqualTo(new Date(1500));
        assertThat(vo.getCurrentValue()).isEqualTo(120);
        assertThat(vo.getThreshold()).isEqualTo(60);
        assertThat(vo.getSummary()).isEqualTo("rendered:TASK_INCREMENT_DELAY_ALTER");
        DeliverySummaryVo summary = vo.getDeliverySummary();
        assertThat(summary.getStatus()).isEqualTo(DeliverySummaryVo.PARTIAL_FAILED);
        assertThat(summary.getByChannel().get(DeliveryChannel.EMAIL).getSuccess()).isEqualTo(2);
        assertThat(summary.getByChannel().get(DeliveryChannel.EMAIL).getTotal()).isEqualTo(3);
        assertThat(summary.getByChannel().get(DeliveryChannel.SYSTEM).getSuccess()).isEqualTo(1);
    }

    @Test
    @DisplayName("AC1 非任务告警（校验、引擎、API Server）同样返回并识别对象类型")
    void should_include_non_task_alarms() {
        AlarmInfo inspect = alarm();
        inspect.setInspectId(I1);
        inspect.setName("校验A");
        AlarmInfo engine = alarm();
        engine.setAgentId("agent-1");
        engine.setMetric(AlarmKeyEnum.ENGINE_OFFLINE);
        AlarmInfo api = alarm();
        api.setType(AlarmTypeEnum.API_SERVER_ALARM);
        api.setServerId("server-1");
        alarmData.addAll(List.of(inspect, engine, api));
        when(inspects.findAllByIds(List.of(I1))).thenReturn(List.of());

        List<AlarmEventVo> items = service.list(new AlarmEventQuery(), admin, Locale.CHINA).getItems();

        assertThat(items).extracting(AlarmEventVo::getObjectType)
                .containsExactly(AlarmObjectType.INSPECT, AlarmObjectType.AGENT, AlarmObjectType.API_SERVER);
        assertThat(items.get(0).isObjectDeleted()).isTrue();
        assertThat(items.get(1).getObjectName()).isEqualTo("agent-1");
        assertThat(items.get(2).getObjectId()).isEqualTo("server-1");
    }

    @Test
    @DisplayName("AC1 筛选条件转换为告警查询，包括对象类型与 URL 中的时间范围")
    void should_translate_filters() {
        AlarmEventQuery query = new AlarmEventQuery();
        query.setStatus(List.of(AlarmStatusEnum.ING));
        query.setLevel(List.of(Level.CRITICAL, Level.EMERGENCY));
        query.setObjectType(List.of(AlarmObjectType.NODE));
        query.setMetric(List.of(AlarmKeyEnum.TASK_INCREMENT_DELAY));
        query.setAgentId("agent-9");
        query.setKeyword("a.b");
        query.setStart(1000L);
        query.setEnd(2000L);

        service.list(query, admin, Locale.CHINA);

        String json = lastAlarmQuery.get().getQueryObject().toJson();
        assertThat(json).contains("\"status\": {\"$in\": [\"ING\"]}")
                .contains("\"level\": {\"$in\": [\"CRITICAL\", \"EMERGENCY\"]}")
                .contains("\"metric\": {\"$in\": [\"TASK_INCREMENT_DELAY\"]}")
                .contains("\"agentId\": \"agent-9\"")
                .contains("\"nodeId\": {\"$nin\": [null, \"\"]}")
                .contains("\"lastOccurrenceTime\": {\"$gte\"")
                .contains("\\\\Qa.b\\\\E");
    }

    @Test
    @DisplayName("可见范围：普通用户只看有权限任务和校验的告警；没有任何权限时不查询告警")
    void should_scope_member_visibility() {
        when(tasks.findAllDto(any(Query.class), eq(member))).thenReturn(List.of(task(T1, "t")));
        when(inspects.findAllDto(any(Query.class), eq(member))).thenReturn(List.of(inspect(I1)));
        service.list(new AlarmEventQuery(), member, Locale.CHINA);
        Document scope = lastAlarmQuery.get().getQueryObject();
        assertThat(scope.toJson()).contains("\"taskId\": {\"$in\": [\"" + T1 + "\"]}")
                .contains("\"inspectId\": {\"$in\": [\"" + I1 + "\"]}");

        UserDetail stranger = new UserDetail("x", "customer", "x", "unused", List.of());
        when(tasks.findAllDto(any(Query.class), eq(stranger))).thenReturn(List.of());
        when(inspects.findAllDto(any(Query.class), eq(stranger))).thenReturn(List.of());
        lastAlarmQuery.set(null);
        assertThat(service.list(new AlarmEventQuery(), stranger, Locale.CHINA).getTotal()).isZero();
        assertThat(lastAlarmQuery.get()).isNull();
    }

    @Test
    @DisplayName("AC2 详情：任务删除后保留名称快照并标记已删除，摘要渲染异常时回退原始摘要")
    void should_keep_snapshot_after_task_deleted() {
        AlarmInfo alarm = taskAlarm(T3, "已删除任务", "node-1");
        alarm.setNode("源节点");
        when(mongo.findById(alarm.getId(), AlarmInfo.class)).thenReturn(alarm);
        when(tasks.findAllTasksByIds(List.of(T3))).thenReturn(List.of());
        doThrow(new IllegalStateException("template")).when(alarms).renderSummary(any(), any());

        AlarmEventVo vo = service.detail(alarm.getId().toHexString(), admin, Locale.CHINA);

        assertThat(vo.getObjectType()).isEqualTo(AlarmObjectType.NODE);
        assertThat(vo.getObjectName()).isEqualTo("源节点");
        assertThat(vo.getTaskName()).isEqualTo("已删除任务");
        assertThat(vo.isObjectDeleted()).isTrue();
        assertThat(vo.getSyncType()).isNull();
        assertThat(vo.getSummary()).isEqualTo("TASK_INCREMENT_DELAY_ALTER");
    }

    @Test
    @DisplayName("AC2 详情：无权限或不存在时返回 Alarm.NotFound")
    void should_hide_invisible_detail() {
        AlarmInfo alarm = taskAlarm(T2, "t2", null);
        when(mongo.findById(alarm.getId(), AlarmInfo.class)).thenReturn(alarm);
        when(tasks.findAllDto(any(Query.class), eq(member))).thenReturn(List.of(task(T1, "t")));
        when(inspects.findAllDto(any(Query.class), eq(member))).thenReturn(List.of());
        when(mongo.exists(any(Query.class), eq(AlarmInfo.class))).thenReturn(false);

        assertThatThrownBy(() -> service.detail(alarm.getId().toHexString(), member, Locale.CHINA))
                .isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("Alarm.NotFound"));
        assertThatThrownBy(() -> service.detail("bad-id", admin, Locale.CHINA)).isInstanceOf(BizException.class);
    }

    @Test
    @DisplayName("AC1 统计卡：告警中、严重/紧急、今日新增、今日已恢复分别计数")
    void should_count_stats() {
        List<String> queries = new ArrayList<>();
        when(mongo.count(any(Query.class), eq(AlarmInfo.class))).thenAnswer(i -> {
            String json = i.<Query>getArgument(0).getQueryObject().toJson();
            queries.add(json);
            if (json.contains("recoveryTime")) return 1L;
            if (json.contains("firstOccurrenceTime")) return 4L;
            if (json.contains("CRITICAL")) return 2L;
            return 7L;
        });

        AlarmEventStatsVo stats = service.stats(admin);

        assertThat(stats).isEqualTo(new AlarmEventStatsVo(7, 2, 4, 1));
        assertThat(queries.get(1)).contains("\"status\": \"ING\"").contains("EMERGENCY");
        assertThat(queries.get(3)).contains("\"status\": \"RECOVER\"");
    }

    @Test
    @DisplayName("AC1 是否已通知筛选：按投递汇总过滤后再分页")
    void should_filter_by_notified() {
        AlarmInfo failed = taskAlarm(T1, "t", null);
        AlarmInfo quiet = taskAlarm(T1, "t", null);
        alarmData.addAll(List.of(failed, quiet));
        when(tasks.findAllTasksByIds(any())).thenReturn(List.of(task(T1, "t")));
        deliveryData.add(delivery(failed.getId().toHexString(), DeliveryChannel.EMAIL, "a@x.com", DeliveryStatus.FAILED));
        when(mongo.find(any(Query.class), eq(AlarmInfo.class))).thenAnswer(i -> {
            Query q = i.getArgument(0);
            if (q.getQueryObject().containsKey("_id")) {
                return List.of(failed);
            }
            return List.of(failed, quiet);
        });

        AlarmEventQuery query = new AlarmEventQuery();
        query.setNotified(DeliverySummaryVo.FAILED);
        Page<AlarmEventVo> page = service.list(query, admin, Locale.CHINA);
        assertThat(page.getTotal()).isEqualTo(1);
        assertThat(page.getItems().get(0).getId()).isEqualTo(failed.getId().toHexString());

        query.setNotified(AlarmEventQueryService.NOTIFIED_NONE);
        assertThat(service.list(query, admin, Locale.CHINA).getTotal()).isEqualTo(1);
        verify(mongo, never()).count(any(Query.class), eq(NotificationDelivery.class));
    }

    private static AlarmInfo alarm() {
        AlarmInfo info = new AlarmInfo();
        info.setId(new ObjectId());
        info.setStatus(AlarmStatusEnum.ING);
        info.setLevel(Level.WARNING);
        info.setType(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM);
        info.setMetric(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        info.setSummary("TASK_INCREMENT_DELAY_ALTER");
        info.setFirstOccurrenceTime(new Date(1000));
        info.setLastOccurrenceTime(new Date(1200));
        return info;
    }

    private static AlarmInfo taskAlarm(String taskId, String name, String nodeId) {
        AlarmInfo info = alarm();
        info.setTaskId(taskId);
        info.setName(name);
        info.setNodeId(nodeId);
        return info;
    }

    private static TaskDto task(String id, String name) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId(id));
        task.setName(name);
        task.setSyncType("migrate");
        return task;
    }

    private static InspectDto inspect(String id) {
        InspectDto dto = new InspectDto();
        dto.setId(new ObjectId(id));
        return dto;
    }

    private static NotificationDelivery delivery(String alarmId, DeliveryChannel channel, String address, DeliveryStatus status) {
        NotificationDelivery d = new NotificationDelivery();
        d.setAlarmEventId(alarmId);
        d.setChannel(channel);
        d.setRecipientAddress(address);
        d.setStatus(status);
        d.setSentAt(new Date());
        return d;
    }
}
