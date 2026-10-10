package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.task.service.TaskService;
import jakarta.servlet.http.HttpServletRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlarmControllerReceiverScopeTest {
    private AlarmController controller;
    private AlarmService alarmService;
    private TaskService taskService;
    private AlarmReceiverAccess access;
    private UserDetail user;
    private HttpServletRequest request;
    private final ObjectId taskId = new ObjectId();

    @BeforeEach
    void setUp() {
        controller = spy(new AlarmController());
        alarmService = mock(AlarmService.class);
        taskService = mock(TaskService.class);
        access = mock(AlarmReceiverAccess.class);
        controller.setAlarmService(alarmService);
        controller.setTaskService(taskService);
        controller.setAlarmReceiverAccess(access);
        user = mock(UserDetail.class);
        request = mock(HttpServletRequest.class);
        doReturn(user).when(controller).getLoginUser();
    }

    @Test
    void newReferencesAreCheckedAgainstTaskCurrentReceivers() {
        List<AlarmReceiver> current = List.of(group("finance"));
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(current);
        when(taskService.findByTaskId(taskId, "alarmReceivers", "emailReceivers")).thenReturn(task);
        AlarmVO alarm = alarm(List.of(group("finance"), group("own")));

        runAuthorized(() -> controller.updateTaskAlarm(request, alarm));

        verify(access).checkNewReferences(user, alarm.getAlarmReceivers(), current);
        verify(alarmService).updateTaskAlarm(alarm, user);
    }

    @Test
    void nodeLevelAndSystemDefaultRequestsAreCheckedToo() {
        // 不再复刻 service 的写入判定：节点级 / 切回系统默认也按新增引用校验（未改动的引用零开销放行）
        AlarmVO alarm = alarm(List.of(group("own")));
        alarm.setNodeId("node-1");
        alarm.setUseSystemDefaultReceivers(true);

        runAuthorized(() -> controller.updateTaskAlarm(request, alarm));

        verify(access).checkNewReferences(user, alarm.getAlarmReceivers(), (List<AlarmReceiver>) null);
    }

    @Test
    void outOfScopeRejectsWithoutWriting() {
        AlarmVO alarm = alarm(List.of(group("finance")));
        doThrow(new BizException("AlarmReceiver.OutOfScope")).when(access)
                .checkNewReferences(any(), any(), (List<AlarmReceiver>) any());

        assertThrows(BizException.class, () -> runAuthorized(() -> controller.updateTaskAlarm(request, alarm)));

        verify(alarmService, never()).updateTaskAlarm(any(), any());
    }

    private AlarmVO alarm(List<AlarmReceiver> receivers) {
        AlarmVO alarm = new AlarmVO();
        alarm.setTaskId(taskId.toHexString());
        alarm.setAlarmReceivers(receivers);
        return alarm;
    }

    private void runAuthorized(Runnable action) {
        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
            action.run();
        }
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }
}
