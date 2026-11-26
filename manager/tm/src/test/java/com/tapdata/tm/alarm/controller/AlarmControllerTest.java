package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarm.dto.SystemNotificationStatusDto;
import com.tapdata.tm.alarm.dto.SystemNotificationStatusItemDto;
import com.tapdata.tm.alarm.service.AlarmEventQueryService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlarmControllerTest {
    private AlarmController controller;
    private AlarmSettingService alarmSettingService;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        controller = spy(new AlarmController());
        alarmSettingService = mock(AlarmSettingService.class);
        user = new UserDetail("owner-id", "customer-id", "owner", "password", Collections.emptyList());
        ReflectionTestUtils.setField(controller, "alarmSettingService", alarmSettingService);
        doReturn(user).when(controller).getLoginUser();
    }

    @Test
    void systemNotificationStatusReturnsOnlyStatusFields() {
        AlarmSettingDto setting = new AlarmSettingDto();
        setting.setKey(AlarmKeyEnum.TASK_STATUS_ERROR);
        setting.setOpen(true);
        setting.setNotify(List.of(NotifyEnum.EMAIL));
        when(alarmSettingService.isSystemNotificationStatusSupported()).thenReturn(true);
        when(alarmSettingService.findAllAlarmSetting(user)).thenReturn(List.of(setting));

        SystemNotificationStatusDto result = controller
                .systemNotificationStatus(null, null, null)
                .getData();

        assertTrue(result.isSupported());
        assertEquals(1, result.getItems().size());
        SystemNotificationStatusItemDto item = result.getItems().get(0);
        assertEquals("TASK_STATUS_ERROR", item.getKey());
        assertTrue(item.isOpen());
        assertEquals(List.of(NotifyEnum.EMAIL), item.getChannels());
    }

    @Test
    void systemNotificationStatusReportsUnsupportedWithoutReadingSettings() {
        when(alarmSettingService.isSystemNotificationStatusSupported()).thenReturn(false);

        SystemNotificationStatusDto result = controller
                .systemNotificationStatus(null, null, null)
                .getData();

        assertFalse(result.isSupported());
        assertTrue(result.getItems().isEmpty());
        verify(alarmSettingService, never()).findAllAlarmSetting(user);
    }

    @Test
    void legacyCloseRequiresClosePermissionBeforeQuery() {
        AlarmService alarmService = mock(AlarmService.class);
        ModuleAccess moduleAccess = mock(ModuleAccess.class);
        ReflectionTestUtils.setField(controller, "alarmService", alarmService);
        ReflectionTestUtils.setField(controller, "moduleAccess", moduleAccess);
        doThrow(new BizException("NotAuthorized")).when(moduleAccess).require(user, DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE);

        assertThrows(BizException.class, () -> controller.close(new String[]{new ObjectId().toHexString()}));

        verify(alarmService, never()).query(any(Query.class));
        verify(alarmService, never()).close(any(), any());
    }

    @Test
    void legacyCloseSkipsWhenNothingIsVisible() {
        AlarmService alarmService = mock(AlarmService.class);
        AlarmEventQueryService queryService = mock(AlarmEventQueryService.class);
        ReflectionTestUtils.setField(controller, "alarmService", alarmService);
        ReflectionTestUtils.setField(controller, "moduleAccess", mock(ModuleAccess.class));
        ReflectionTestUtils.setField(controller, "alarmEventQueryService", queryService);
        when(queryService.visibility(user)).thenReturn(null);

        controller.close(new String[]{new ObjectId().toHexString()});

        verify(alarmService, never()).query(any(Query.class));
        verify(alarmService, never()).close(any(), any());
    }

    @Test
    void systemNotificationStatusRejectsBothEntityIds() {
        assertThrows(RuntimeException.class,
                () -> controller.systemNotificationStatus(null, "task-id", "inspect-id"));
    }
}
