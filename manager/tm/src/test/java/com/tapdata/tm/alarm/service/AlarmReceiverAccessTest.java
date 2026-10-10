package com.tapdata.tm.alarm.service;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlarmReceiverAccessTest {
    private AlarmService alarmService;
    private PermissionService permissionService;
    private SettingsService settingsService;
    private AlarmReceiverAccess access;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        alarmService = mock(AlarmService.class);
        permissionService = mock(PermissionService.class);
        settingsService = mock(SettingsService.class);
        access = new AlarmReceiverAccess();
        access.setAlarmService(alarmService);
        access.setPermissionService(permissionService);
        access.setSettingsService(settingsService);
        user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn("me");
        AlarmReceiverCandidates candidates = new AlarmReceiverCandidates();
        AlarmReceiverCandidates.CandidateGroup own = new AlarmReceiverCandidates.CandidateGroup();
        own.setId("own");
        candidates.getGroups().add(own);
        when(alarmService.receiverCandidates("me", List.of())).thenReturn(candidates);
    }

    @Test
    void cloudNeverGetsFullDirectory() {
        when(settingsService.isCloud()).thenReturn(true);
        assertFalse(access.canViewUserDirectory(user));
        assertFalse(access.canViewUserDirectory(null));
        verify(permissionService, never()).checkCurrentUserHasPermission(anyString(), anyString());
    }

    @Test
    void cloudUserIsCheckedAgainstOwnScope() {
        when(settingsService.isCloud()).thenReturn(true);
        assertThrows(BizException.class, () -> access.checkNewReferences(user, List.of(group("finance")), (List<AlarmReceiver>) null));
        assertDoesNotThrow(() -> access.checkNewReferences(user, List.of(group("own")), (List<AlarmReceiver>) null));
    }

    @Test
    void userManagementViewerSkipsScopeCheck() {
        when(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, "me")).thenReturn(true);
        assertTrue(access.canViewUserDirectory(user));

        assertDoesNotThrow(() -> access.checkNewReferences(user, List.of(group("any")), (List<AlarmReceiver>) null));
        verify(alarmService, never()).receiverCandidates(any(), any());
    }

    @Test
    void unchangedAndStaleReferencesPassWithoutCandidates() {
        // 任务上已有的条目（包括已软删的用户）一律放行，也不计算候选
        List<AlarmReceiver> current = List.of(group("finance"), user("deleted"));
        assertDoesNotThrow(() -> access.checkNewReferences(user,
                List.of(group("finance"), user("deleted"), email("new@example.com")), current));
        assertDoesNotThrow(() -> access.checkNewReferences(user,
                List.of(email("ops@example.com")), (List<AlarmReceiver>) null));
        verify(alarmService, never()).receiverCandidates(any(), any());
    }

    @Test
    void newGroupOutsideScopeIsRejected() {
        assertDoesNotThrow(() -> access.checkNewReferences(user, List.of(group("own")), (List<AlarmReceiver>) null));
        BizException exception = assertThrows(BizException.class,
                () -> access.checkNewReferences(user, List.of(group("own"), group("finance")), List.of(group("own"))));
        assertEquals("AlarmReceiver.OutOfScope", exception.getErrorCode());
    }

    @Test
    void referencesOfAnotherTaskAreNotBorrowed() {
        // 批量：finance 只在 V 上，对 B 来说仍是新增，必须校验，整批拒绝
        List<List<AlarmReceiver>> currents = Arrays.asList(List.of(), List.of(group("finance")));
        assertThrows(BizException.class, () -> access.checkNewReferences(user, List.of(group("finance")), currents));
        // finance 在所有目标任务上都已存在，不算新增
        assertDoesNotThrow(() -> access.checkNewReferences(user, List.of(group("finance")),
                Arrays.asList(List.of(group("finance")), List.of(group("finance")))));
        verify(alarmService, times(1)).receiverCandidates("me", List.of());
    }

    @Test
    void anonymousUserCannotAddDirectoryReferences() {
        assertThrows(BizException.class, () -> access.checkNewReferences(null, List.of(group("own")), (List<AlarmReceiver>) null));
        assertDoesNotThrow(() -> access.checkNewReferences(null, List.of(group("own")), List.of(group("own"))));
    }

    @Test
    void retainInScopeDropsOutOfScopeReferencesOnNewTask() {
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(new ArrayList<>(List.of(group("own"), group("finance"), email("a@example.com"))));

        List<AlarmReceiver> dropped = access.retainInScope(user, task, null);

        assertEquals(List.of(group("finance")), dropped);
        assertEquals(List.of(group("own"), email("a@example.com")), task.getAlarmReceivers());
    }

    @Test
    void retainInScopeKeepsReferencesAlreadyOnTask() {
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(List.of(group("finance")));
        assertTrue(access.retainInScope(user, task, List.of(group("finance"))).isEmpty());
        assertEquals(List.of(group("finance")), task.getAlarmReceivers());
    }

    @Test
    void retainInScopeDropsDirectoryReferencesWhenCandidatesUnavailable() {
        when(alarmService.receiverCandidates("me", List.of())).thenThrow(new BizException("TapOssNonSupportFunctionException"));
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(List.of(group("own")));
        assertEquals(List.of(group("own")), access.retainInScope(user, task, null));
        assertTrue(task.getAlarmReceivers().isEmpty());
        // 显式更新接口不吞异常
        assertThrows(BizException.class, () -> access.checkNewReferences(user, List.of(group("own")), (List<AlarmReceiver>) null));
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }

    private static AlarmReceiver user(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER, id, null);
    }

    private static AlarmReceiver email(String email) {
        return new AlarmReceiver(AlarmReceiverType.EMAIL, null, email);
    }
}
