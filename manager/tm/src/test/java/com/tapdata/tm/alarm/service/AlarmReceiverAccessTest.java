package com.tapdata.tm.alarm.service;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    }

    @Test
    void cloudNeverGetsFullDirectory() {
        when(settingsService.isCloud()).thenReturn(true);
        assertFalse(access.canViewUserDirectory(user));
        assertFalse(access.canViewUserDirectory(null));
        verify(permissionService, never()).checkCurrentUserHasPermission(anyString(), anyString());
    }

    @Test
    void userManagementViewerSkipsScopeCheck() {
        when(permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, "me")).thenReturn(true);
        assertTrue(access.canViewUserDirectory(user));

        assertDoesNotThrow(() -> access.checkScope(user, List.of(group("any")), List.of("t1")));
        verify(alarmService, never()).receiverCandidates(any(), any());
    }

    @Test
    void emailOnlyReceiversSkipScopeCheck() {
        assertDoesNotThrow(() -> access.checkScope(user,
                List.of(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com")), List.of("t1")));
        verify(alarmService, never()).receiverCandidates(any(), any());
    }

    @Test
    void groupOutsideCandidatesIsRejected() {
        AlarmReceiverCandidates candidates = new AlarmReceiverCandidates();
        AlarmReceiverCandidates.CandidateGroup own = new AlarmReceiverCandidates.CandidateGroup();
        own.setId("own");
        candidates.getGroups().add(own);
        when(alarmService.receiverCandidates("me", List.of("t1"))).thenReturn(candidates);

        assertDoesNotThrow(() -> access.checkScope(user, List.of(group("own")), List.of("t1")));
        BizException exception = assertThrows(BizException.class,
                () -> access.checkScope(user, List.of(group("own"), group("finance")), List.of("t1")));
        assertEquals("AlarmReceiver.OutOfScope", exception.getErrorCode());
    }

    private static AlarmReceiver group(String id) {
        return new AlarmReceiver(AlarmReceiverType.USER_GROUP, id, null);
    }
}
