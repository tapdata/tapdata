package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.dto.InspectReceiverPreviewRequest;
import com.tapdata.tm.alarm.service.AlarmReceiverAccess;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverPreview;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.commons.task.dto.alarm.AlarmVO;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.task.service.TaskService;
import jakarta.servlet.http.HttpServletRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Collections;
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
import static org.junit.jupiter.api.Assertions.assertSame;

class AlarmControllerReceiverScopeTest {
    private AlarmController controller;
    private AlarmService alarmService;
    private TaskService taskService;
    private InspectService inspectService;
    private AlarmReceiverAccess access;
    private PermissionService permissionService;
    private UserDetail user;
    private HttpServletRequest request;
    private final ObjectId taskId = new ObjectId();

    @BeforeEach
    void setUp() {
        controller = spy(new AlarmController());
        alarmService = mock(AlarmService.class);
        taskService = mock(TaskService.class);
        inspectService = mock(InspectService.class);
        access = mock(AlarmReceiverAccess.class);
        permissionService = mock(PermissionService.class);
        controller.setAlarmService(alarmService);
        controller.setTaskService(taskService);
        controller.setInspectService(inspectService);
        controller.setAlarmReceiverAccess(access);
        controller.setPermissionService(permissionService);
        user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn("current-user");
        when(permissionService.checkCurrentUserHasPermission(any(), any())).thenReturn(true);
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

    @Test
    void arbitraryTableInspectUsesOnlyCurrentUserDirectoryScope() {
        when(user.getUserId()).thenReturn("current-user");
        AlarmReceiverCandidates candidates = new AlarmReceiverCandidates();
        when(alarmService.receiverCandidates("current-user", Collections.emptyList())).thenReturn(candidates);

        assertSame(candidates, controller.receiverCandidates(request, null, null, true).getData());
    }

    @Test
    void arbitraryTableInspectCandidateListRequiresInspectPermission() {
        when(permissionService.checkCurrentUserHasPermission(any(), any())).thenReturn(false);
        assertThrows(BizException.class, () -> controller.receiverCandidates(request, null, null, true));
        verify(alarmService, never()).receiverCandidates(any(), any());
    }

    @Test
    void inspectDraftPreviewRequiresEditPermissionAndUsesCurrentDraft() {
        when(user.getUserId()).thenReturn("current-user");
        when(access.canViewUserDirectory(user)).thenReturn(true);
        AlarmReceiver receiver = new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com");
        InspectReceiverPreviewRequest previewRequest = new InspectReceiverPreviewRequest();
        previewRequest.setAlarmReceivers(List.of(receiver));
        AlarmReceiverPreview preview = new AlarmReceiverPreview();
        when(alarmService.previewInspectReceivers(List.of(receiver), null, "current-user")).thenReturn(preview);

        assertSame(preview, controller.inspectReceiverPreview(request, previewRequest).getData());

        verify(access).checkNewReferences(user, List.of(receiver), (List<AlarmReceiver>) null);
        verify(alarmService).previewInspectReceivers(List.of(receiver), null, "current-user");
    }

    @Test
    void inspectDraftPreviewRejectsUserWithoutInspectEditPermission() {
        InspectReceiverPreviewRequest previewRequest = new InspectReceiverPreviewRequest();
        when(permissionService.checkCurrentUserHasPermission(any(), any())).thenReturn(false);
        assertThrows(BizException.class, () -> controller.inspectReceiverPreview(request, previewRequest));
        verify(alarmService, never()).previewInspectReceivers(any(), any(), any());
    }

    @Test
    void inspectViewerPreviewUsesSavedReceiverValues() {
        ObjectId inspectId = new ObjectId();
        AlarmReceiver saved = new AlarmReceiver(AlarmReceiverType.EMAIL, null, "saved@example.com");
        AlarmReceiver forged = new AlarmReceiver(AlarmReceiverType.EMAIL, null, "forged@example.com");
        InspectDto inspect = new InspectDto();
        inspect.setId(inspectId);
        inspect.setUserId("inspect-owner");
        inspect.setAlarmReceivers(List.of(saved));
        when(inspectService.findById(any(ObjectId.class), any(Field.class))).thenReturn(inspect);
        when(inspectService.dataPermissionFindById(any(ObjectId.class), any(Field.class))).thenReturn(() -> inspect);
        when(access.canViewUserDirectory(user)).thenReturn(false);
        AlarmReceiverPreview preview = new AlarmReceiverPreview();
        when(alarmService.previewInspectReceivers(List.of(saved), null, "inspect-owner")).thenReturn(preview);
        InspectReceiverPreviewRequest previewRequest = new InspectReceiverPreviewRequest();
        previewRequest.setInspectId(inspectId.toHexString());
        previewRequest.setAlarmReceivers(List.of(forged));

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> {
                        if (invocation.getArgument(2) == DataPermissionActionEnums.Edit) {
                            return ((Supplier<?>) invocation.getArgument(6)).get();
                        }
                        return ((Supplier<?>) invocation.getArgument(5)).get();
                    });
            assertSame(preview, controller.inspectReceiverPreview(request, previewRequest).getData());
        }

        verify(alarmService).previewInspectReceivers(List.of(saved), null, "inspect-owner");
        verify(access, never()).checkNewReferences(any(), any(), (List<AlarmReceiver>) any());
    }

    @Test
    void inspectEditorPreviewChecksNewReferencesAgainstSavedReceiverList() {
        ObjectId inspectId = new ObjectId();
        AlarmReceiver saved = group("saved-group");
        AlarmReceiver added = group("new-group");
        InspectDto inspect = new InspectDto();
        inspect.setId(inspectId);
        inspect.setUserId("inspect-owner");
        inspect.setAlarmReceivers(List.of(saved));
        when(inspectService.findById(any(ObjectId.class), any(Field.class))).thenReturn(inspect);
        when(inspectService.dataPermissionFindById(any(ObjectId.class), any(Field.class))).thenReturn(() -> inspect);
        when(access.canViewUserDirectory(user)).thenReturn(true);
        AlarmReceiverPreview preview = new AlarmReceiverPreview();
        when(alarmService.previewInspectReceivers(List.of(saved, added), null, "inspect-owner")).thenReturn(preview);
        InspectReceiverPreviewRequest previewRequest = new InspectReceiverPreviewRequest();
        previewRequest.setInspectId(inspectId.toHexString());
        previewRequest.setAlarmReceivers(List.of(saved, added));

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
            assertSame(preview, controller.inspectReceiverPreview(request, previewRequest).getData());
        }

        verify(access).checkNewReferences(user, List.of(saved, added), List.of(saved));
        verify(alarmService).previewInspectReceivers(List.of(saved, added), null, "inspect-owner");
    }

    @Test
    void inspectPreviewChecksSavedInspectPermissionAndPreservesExistingReceivers() {
        ObjectId inspectId = new ObjectId();
        AlarmReceiver existing = group("existing-group");
        AlarmReceiver added = new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com");
        InspectDto inspect = new InspectDto();
        inspect.setUserId("inspect-owner");
        inspect.setAlarmReceivers(List.of(existing));
        when(inspectService.findById(eq(inspectId), any(Field.class))).thenReturn(inspect);
        when(access.canViewUserDirectory(user)).thenReturn(true);
        AlarmReceiverPreview preview = new AlarmReceiverPreview();
        when(alarmService.previewInspectReceivers(List.of(existing, added), null, "inspect-owner"))
                .thenReturn(preview);
        InspectReceiverPreviewRequest previewRequest = new InspectReceiverPreviewRequest();
        previewRequest.setInspectId(inspectId.toHexString());
        previewRequest.setAlarmReceivers(List.of(existing, added));

        try (MockedStatic<DataPermissionHelper> helper = mockStatic(DataPermissionHelper.class)) {
            helper.when(() -> DataPermissionHelper.checkOfQuery(same(user), any(), any(), any(), any(), any(), any()))
                    .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(5)).get());
            assertSame(preview, controller.inspectReceiverPreview(request, previewRequest).getData());
        }

        verify(access).checkNewReferences(user, List.of(existing, added), (List<AlarmReceiver>) List.of(existing));
        verify(alarmService).previewInspectReceivers(List.of(existing, added), null, "inspect-owner");
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
