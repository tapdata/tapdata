package com.tapdata.tm.schedule;

import com.tapdata.tm.alarm.service.AlarmNotifyService;
import com.tapdata.tm.alarm.service.ApiServerAlarmConfig;
import com.tapdata.tm.alarm.service.ApiServerAlarmService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;

class AlarmScheduleTest {
    AlarmSchedule alarmSchedule;
    ApiServerAlarmService apiServerAlarmService;
    ApiServerAlarmConfig apiServerAlarmConfig;
    AlarmNotifyService alarmNotifyService;
    AtomicBoolean init = new AtomicBoolean(false);

    @BeforeEach
    void init() {
        alarmSchedule = Mockito.mock(AlarmSchedule.class);
        apiServerAlarmService = Mockito.mock(ApiServerAlarmService.class);
        apiServerAlarmConfig = Mockito.mock(ApiServerAlarmConfig.class);
        alarmNotifyService = Mockito.mock(AlarmNotifyService.class);
        ReflectionTestUtils.setField(alarmSchedule, "apiServerAlarmService", apiServerAlarmService);
        ReflectionTestUtils.setField(alarmSchedule, "apiServerAlarmConfig", apiServerAlarmConfig);
        ReflectionTestUtils.setField(alarmSchedule, "alarmNotifyService", alarmNotifyService);
        ReflectionTestUtils.setField(alarmSchedule, "init", init);
        doNothing().when(alarmNotifyService).notifyAlarm();
        doNothing().when(apiServerAlarmService).scanMetricData();
        doNothing().when(apiServerAlarmConfig).updateConfig();
        doCallRealMethod().when(alarmSchedule).schedule();
        doCallRealMethod().when(alarmSchedule).apiServer();
        doCallRealMethod().when(alarmSchedule).taskRetryAlarm();
        doCallRealMethod().when(alarmSchedule).afterPropertiesSet();
    }

    @Test
    void testSchedule() {
        Assertions.assertDoesNotThrow(alarmSchedule::schedule);
    }

    @Test
    void testApiServer() {
        Assertions.assertDoesNotThrow(alarmSchedule::apiServer);
    }

    @Test
    void testTaskRetryAlarm() {
        Assertions.assertDoesNotThrow(alarmSchedule::taskRetryAlarm);
    }

    @Test
    void testAfterPropertiesSet() {
        Assertions.assertDoesNotThrow(alarmSchedule::afterPropertiesSet);
    }
}