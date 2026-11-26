package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmEventQueryService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.task.service.TaskService;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * L2: legacy {@code POST /api/alarm/close} through the real controller, alarm event visibility and module access;
 * alarm storage, task/inspect lookups, permission storage and audit log are boundaries.
 */
@Timeout(10)
class AlarmLegacyCloseIT {
    private static final String T1 = "64c000000000000000000001";
    private static final String T2 = "64c000000000000000000002";

    private GenericApplicationContext context;
    private AlarmController controller;
    private AlarmService alarms;
    private TaskService tasks;
    private InspectService inspects;
    private PermissionService permissions;
    private UserLogService audit;
    private final List<AlarmInfo> alarmData = new ArrayList<>();
    private UserDetail user;

    @BeforeEach
    void setUp() {
        alarms = mock(AlarmService.class);
        tasks = mock(TaskService.class);
        inspects = mock(InspectService.class);
        permissions = mock(PermissionService.class);
        audit = mock(UserLogService.class);
        SettingsService settings = mock(SettingsService.class);
        when(alarms.query(any(Query.class))).thenAnswer(i -> {
            Map<String, Object> filter = i.<Query>getArgument(0).getQueryObject();
            return alarmData.stream().filter(a -> matches(filter, a)).collect(Collectors.toList());
        });
        user = new UserDetail("member", "customer", "member", "unused", List.of());

        context = new GenericApplicationContext();
        context.registerBean(ModuleAccess.class, () -> new ModuleAccess(permissions, settings));
        context.registerBean(AlarmEventQueryService.class, () -> new AlarmEventQueryService(mock(MongoTemplate.class),
                tasks, inspects, alarms, mock(NotificationDeliveryService.class), context.getBean(ModuleAccess.class), audit));
        context.registerBean(AlarmController.class, () -> {
            AlarmController c = spy(new AlarmController());
            c.setAlarmService(alarms);
            c.setUserLogService(audit);
            c.setModuleAccess(context.getBean(ModuleAccess.class));
            c.setAlarmEventQueryService(context.getBean(AlarmEventQueryService.class));
            return c;
        });
        context.refresh();
        controller = context.getBean(AlarmController.class);
        doReturn(user).when(controller).getLoginUser();
    }

    @AfterEach
    void tearDown() {
        context.close();
        alarmData.clear();
    }

    @Test
    @DisplayName("TAP-13349 旧关闭接口：无告警关闭权限时拒绝 NotAuthorized，不关闭告警也不写审计")
    void should_reject_without_close_permission() {
        AlarmInfo alarm = alarm(T1);
        when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ALARM_EVENT, "member")).thenReturn(true);

        assertThatThrownBy(() -> controller.close(new String[]{alarm.getId().toHexString()}))
                .isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("NotAuthorized"));

        verify(alarms, never()).query(any(Query.class));
        verify(alarms, never()).close(any(), any());
        verifyNoInteractions(audit);
    }

    @Test
    @DisplayName("TAP-13349 旧关闭接口：有关闭权限的成员只关闭可见任务的告警，不可见与非法 ID 忽略；全量告警权限可关闭全部")
    void should_close_only_visible_alarms() {
        AlarmInfo visible = alarm(T1);
        AlarmInfo hidden = alarm(T2);
        when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE, "member")).thenReturn(true);
        when(tasks.findAllDto(any(Query.class), eq(user))).thenReturn(List.of(task(T1)));
        when(inspects.findAllDto(any(Query.class), eq(user))).thenReturn(List.of());
        String[] ids = {visible.getId().toHexString(), hidden.getId().toHexString(), "not-an-id"};

        controller.close(ids);

        assertThat(closed()).containsExactly(visible.getId().toHexString());
        verify(audit).addUserLog(Modular.ALARM, Operation.CLOSE, user, visible.getId().toHexString(), visible.getId().toHexString());
        verify(audit, never()).addUserLog(any(), any(), any(UserDetail.class), eq(hidden.getId().toHexString()), anyString());

        when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ALARM_EVENT, "member")).thenReturn(true);
        controller.close(ids);

        assertThat(closed()).containsExactlyInAnyOrder(visible.getId().toHexString(), hidden.getId().toHexString());
    }

    private List<String> closed() {
        ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
        verify(alarms, org.mockito.Mockito.atLeastOnce()).close(captor.capture(), eq(user));
        return List.of(captor.getValue());
    }

    private AlarmInfo alarm(String taskId) {
        AlarmInfo info = new AlarmInfo();
        info.setId(new ObjectId());
        info.setTaskId(taskId);
        alarmData.add(info);
        return info;
    }

    private static TaskDto task(String id) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId(id));
        return task;
    }

    /** Minimal Mongo filter evaluation for the operators used by the close query. */
    @SuppressWarnings("unchecked")
    private static boolean matches(Map<String, Object> filter, AlarmInfo alarm) {
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            Object cond = e.getValue();
            boolean ok = switch (e.getKey()) {
                case "$and" -> ((Collection<Map<String, Object>>) cond).stream().allMatch(c -> matches(c, alarm));
                case "$or" -> ((Collection<Map<String, Object>>) cond).stream().anyMatch(c -> matches(c, alarm));
                default -> {
                    Object value = switch (e.getKey()) {
                        case "_id" -> alarm.getId();
                        case "taskId" -> alarm.getTaskId();
                        case "inspectId" -> alarm.getInspectId();
                        default -> throw new IllegalArgumentException("unexpected alarm field " + e.getKey());
                    };
                    if (cond instanceof Map<?, ?> ops && ops.containsKey("$in")) {
                        yield ((Collection<?>) ops.get("$in")).stream().anyMatch(v -> Objects.equals(v, value));
                    }
                    yield Objects.equals(cond, value);
                }
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }
}
