package com.tapdata.tm.task.controller;

import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.commons.task.dto.alarm.BatchAlarmDetail;
import com.tapdata.tm.commons.task.dto.alarm.BatchAlarmResult;
import com.tapdata.tm.commons.task.dto.alarm.BatchUpdateAlarmParam;
import com.tapdata.tm.commons.task.dto.alarm.ReceiverBatchMode;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.task.service.TaskService;
import jakarta.servlet.http.HttpServletRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskControllerBatchAlarmScopeTest {
    private TaskController controller;
    private AlarmService alarmService;
    private TaskService taskService;
    private AlarmReceiverAccess access;
    private UserDetail user;
    private HttpServletRequest request;
    private final ObjectId own = new ObjectId();
    private final ObjectId other = new ObjectId();

    @BeforeEach
    void setUp() {
        controller = spy(new TaskController());
        alarmService = mock(AlarmService.class);
        taskService = mock(TaskService.class);
        access = mock(AlarmReceiverAccess.class);
        controller.setAlarmService(alarmService);
        controller.setTaskService(taskService);
        controller.setAlarmReceiverAccess(access);
        user = mock(UserDetail.class);
        request = mock(HttpServletRequest.class);
        doReturn(user).when(controller).getLoginUser();
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(alarmService).runWithReceiverCache(any());
        TaskDto ownTask = new TaskDto();
        ownTask.setId(own);
        ownTask.setAlarmReceivers(List.of());
        TaskDto otherTask = new TaskDto();
        otherTask.setId(other);
        otherTask.setAlarmReceivers(List.of(group("finance")));
        when(taskService.findAll(any(Query.class))).thenReturn(List.of(otherTask, ownTask));
        when(alarmService.applyAuthorizedTaskAlarm(anyString(), any(), any()))
                .thenAnswer(invocation -> BatchAlarmDetail.of(invocation.getArgument(0), null, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void eachTaskIsDiffedAgainstItsOwnReceivers() {
        BatchUpdateAlarmParam param = param(ReceiverBatchMode.APPEND, List.of(email("a@example.com")), own, other);

        BatchAlarmResult result = runAuthorized(() -> controller.batchUpdateTaskAlarm(request, param));

        ArgumentCaptor<Collection<List<AlarmReceiver>>> currents = ArgumentCaptor.forClass(Collection.class);
        verify(access).checkNewReferences(same(user), eq(param.getAlarmReceivers()), currents.capture());
        // 按请求顺序，每个任务一份自己的当前列表，不合并、不借用
        assertEquals(List.of(List.of(), List.of(group("finance"))), new ArrayList<>(currents.getValue()));
        verify(alarmService, times(2)).applyAuthorizedTaskAlarm(anyString(), same(param), same(user));
        assertEquals(2, result.getDetails().size());
    }

    @Test
    void removeModeSkipsScopeCheck() {
        BatchUpdateAlarmParam param = param(ReceiverBatchMode.REMOVE, List.of(group("finance")), own);

        runAuthorized(() -> controller.batchUpdateTaskAlarm(request, param));

        verify(access, never()).checkNewReferences(any(), any(), any(Collection.class));
        verify(alarmService).applyAuthorizedTaskAlarm(own.toHexString(), param, user);
    }

    @Test
    void outOfScopeRejectsWholeBatchWithoutAnyWrite() {
        BatchUpdateAlarmParam param = param(ReceiverBatchMode.REPLACE, List.of(group("finance")), own, other);
        doThrow(new BizException("AlarmReceiver.OutOfScope")).when(access)
                .checkNewReferences(any(), any(), any(Collection.class));

        assertThrows(BizException.class, () -> runAuthorized(() -> controller.batchUpdateTaskAlarm(request, param)));

        verify(alarmService, never()).applyAuthorizedTaskAlarm(any(), any(), any());
    }

    @Test
    void permissionIsCheckedOncePerTaskInsideWrite() {
        BatchUpdateAlarmParam param = param(ReceiverBatchMode.APPEND, List.of(group("own")), own, other);
        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(6)).get());

            BatchAlarmResult result = controller.batchUpdateTaskAlarm(request, param).getData();

            helper.verify(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()), times(2));
            verify(alarmService, never()).applyAuthorizedTaskAlarm(any(), any(), any());
            assertEquals(2, result.getDetails().size());
        }
    }

    private BatchAlarmResult runAuthorized(Supplier<ResponseMessage<BatchAlarmResult>> action) {
        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
            return action.get().getData();
        }
    }

    private static BatchUpdateAlarmParam param(ReceiverBatchMode mode, List<AlarmReceiver> receivers, ObjectId... ids) {
        BatchUpdateAlarmParam param = new BatchUpdateAlarmParam();
        List<String> taskIds = new ArrayList<>();
        for (ObjectId id : ids) {
            taskIds.add(id.toHexString());
        }
        param.setTaskIds(taskIds);
        param.setReceiverMode(mode);
        param.setAlarmReceivers(receivers);
        return param;
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }

    private static AlarmReceiver email(String email) {
        return new AlarmReceiver(AlarmReceiverType.EMAIL, null, email);
    }
}
