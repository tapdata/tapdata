package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskIncrementStuckScheduleTest {
    private static final String USER_ID = new ObjectId().toHexString();
    private static final String UNIT_KEY = JsonUtil.toJsonUseJackson(Arrays.asList("src-node", "tgt-node"));
    private static final long TWENTY_MINUTES = 20 * 60 * 1000L;
    private static final long TWO_HOURS = 2 * 60 * 60 * 1000L;

    private TaskIncrementStuckSchedule schedule;
    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private StateMachineService stateMachineService;
    private UserService userService;
    private MonitoringLogsService monitoringLogsService;
    private AlarmService alarmService;
    private UserDetail userDetail;

    @BeforeEach
    void setUp() {
        // SettingsEnum resolves through the static SettingUtil; initialize it so the built-in
        // defaults (timeout/cooldown/grace/limit) apply instead of an NPE.
        new SettingUtil(mock(SettingsService.class));

        schedule = new TaskIncrementStuckSchedule();
        taskService = mock(TaskService.class);
        taskScheduleService = mock(TaskScheduleService.class);
        stateMachineService = mock(StateMachineService.class);
        TransformSchemaService transformSchemaService = mock(TransformSchemaService.class);
        userService = mock(UserService.class);
        monitoringLogsService = mock(MonitoringLogsService.class);
        alarmService = mock(AlarmService.class);
        schedule.setTaskService(taskService);
        schedule.setTaskScheduleService(taskScheduleService);
        schedule.setStateMachineService(stateMachineService);
        schedule.setTransformSchemaService(transformSchemaService);
        schedule.setUserService(userService);
        schedule.setMonitoringLogsService(monitoringLogsService);
        schedule.setAlarmService(alarmService);

        userDetail = mock(UserDetail.class);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);
        when(stateMachineService.executeAboutTask(any(TaskDto.class), any(), any())).thenReturn(StateMachineResult.ok());
    }

    private Map<String, Object> unit(String stage, Object eventTime, String streamOffset) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("syncStage", stage);
        p.put("streamOffset", streamOffset);
        p.put("offset", "off-1");
        p.put("eventTime", eventTime);
        p.put("sourceTime", eventTime);
        return p;
    }

    private String signature(Map<String, Object> p) {
        // Mirror the schedule exactly: it reads the engine value back through Gson (numbers become
        // Double), so round-trip here or the last-seen signature would not match.
        Map<String, Object> parsed = JsonUtil.parseJson(JsonUtil.toJsonUseJackson(p), LinkedHashMap.class);
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("syncStage", parsed.get("syncStage"));
        s.put("streamOffset", parsed.get("streamOffset"));
        s.put("offset", parsed.get("offset"));
        s.put("eventTime", parsed.get("eventTime"));
        s.put("sourceTime", parsed.get("sourceTime"));
        return JsonUtil.toJsonUseJackson(s);
    }

    /**
     * Builds a running task whose {@code attrs.syncProgress} entry is the JSON string the engine
     * actually persists, with an optional pre-existing incrementStuck state.
     */
    private TaskDto task(String syncType, Map<String, Object> unitProgress, Map<String, Object> state) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setName("task-1");
        task.setUserId(USER_ID);
        task.setAgentId("agent-1");
        task.setStatus(TaskDto.STATUS_RUNNING);
        task.setSyncType(syncType);
        task.setStartTime(new Date(System.currentTimeMillis() - 3_600_000L));

        Map<String, Object> attrs = new HashMap<>();
        Map<String, Object> syncProgress = new HashMap<>();
        syncProgress.put(UNIT_KEY, JsonUtil.toJsonUseJackson(unitProgress));
        attrs.put("syncProgress", syncProgress);
        if (state != null) {
            Map<String, Object> stuck = new HashMap<>();
            stuck.put(UNIT_KEY, state);
            attrs.put("incrementStuck", stuck);
        }
        task.setAttrs(attrs);
        return task;
    }

    private Map<String, Object> stalledState(Map<String, Object> unitProgress) {
        Map<String, Object> state = new HashMap<>();
        state.put("lastProgressAt", System.currentTimeMillis() - TWENTY_MINUTES);
        state.put("lastSignature", signature(unitProgress));
        state.put("restartCount", 0);
        return state;
    }

    private void run(TaskDto... tasks) {
        when(taskService.findAll(any(Query.class))).thenReturn(Arrays.asList(tasks));
        schedule.detectIncrementStuck();
    }

    @Test
    void seedsFirstSightingWithoutRestarting() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, null);

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        // seeded state is persisted exactly once (no restart budget consumed)
        verify(taskService, times(1)).update(any(Query.class), any(Update.class));
        verify(stateMachineService, never()).executeAboutTask(any(TaskDto.class), any(), any());
    }

    @Test
    void restartsWhenCdcUnitStalledPastTimeout() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, stalledState(p));

        run(task);

        verify(taskScheduleService, times(1)).scheduling(task, userDetail, true);
        verify(stateMachineService, times(1)).executeAboutTask(eq(task), any(), eq(userDetail));
        // restart counter persisted after the successful restart
        verify(taskService, times(1)).update(any(Query.class), any(Update.class));
    }

    @Test
    void skipsUnitsNotInCdcStage() {
        Map<String, Object> p = unit("INITIAL_SYNC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, stalledState(p));

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        verify(taskService, never()).update(any(Query.class), any(Update.class));
    }

    @Test
    void detectsLogCollectorUnitBySourceTime() {
        // logCollector units may not carry syncStage=CDC; they must still be monitored.
        Map<String, Object> p = unit(null, 1L, null);
        TaskDto task = task(TaskDto.SYNC_TYPE_LOG_COLLECTOR, p, stalledState(p));

        run(task);

        verify(taskScheduleService, times(1)).scheduling(task, userDetail, true);
    }

    @Test
    void suppressesRestartWithinCooldown() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        Map<String, Object> state = stalledState(p);
        state.put("lastRestartAt", System.currentTimeMillis() - TWENTY_MINUTES);
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, state);

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        verify(monitoringLogsService, times(1)).startTaskErrorLog(eq(task), eq(userDetail), contains("suppressed"), any());
    }

    @Test
    void allowsRestartAfterTwoHourCooldown() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        Map<String, Object> state = stalledState(p);
        state.put("lastRestartAt", System.currentTimeMillis() - TWO_HOURS - 1L);
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, state);

        run(task);

        verify(taskScheduleService, times(1)).scheduling(task, userDetail, true);
    }

    @Test
    void suppressesRestartAtMaxLimit() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        Map<String, Object> state = stalledState(p);
        state.put("restartCount", 3);
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, state);

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
    }

    @Test
    void doesNotResetRestartBudgetWhenProgressChanges() {
        Map<String, Object> previousProgress = unit("CDC", 1L, "100");
        Map<String, Object> currentProgress = unit("CDC", 2L, "200");
        Map<String, Object> state = stalledState(previousProgress);
        state.put("restartCount", 3);
        state.put("lastRestartAt", System.currentTimeMillis() - TWENTY_MINUTES);
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, currentProgress, state);

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
        verify(taskService).update(any(Query.class), captor.capture());
        Object setValue = captor.getValue().getUpdateObject().get("$set");
        assertTrue(setValue instanceof Map, "expected a $set payload, got: " + setValue);
        Map<?, ?> set = (Map<?, ?>) setValue;
        Object persistedState = set.get("attrs.incrementStuck." + UNIT_KEY);
        assertTrue(persistedState instanceof Map, "expected persisted unit state, got: " + persistedState);
        assertEquals(3L, ((Number) ((Map<?, ?>) persistedState).get("restartCount")).longValue());
        assertTrue(((Map<?, ?>) persistedState).containsKey("lastRestartAt"));
    }

    @Test
    void doesNotConsumeRestartBudgetWhenStateMachineRejects() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, stalledState(p));
        when(stateMachineService.executeAboutTask(any(TaskDto.class), any(), any()))
                .thenReturn(StateMachineResult.fail("rejected"));

        run(task);

        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        // A rejected restart must neither persist the counter nor log success.
        verify(taskService, never()).update(any(Query.class), any(Update.class));
        verify(monitoringLogsService, times(1)).startTaskErrorLog(eq(task), eq(userDetail), contains("not restarted"), any());
    }

    @Test
    void skipsRestartWhenOwnerCannotBeLoaded() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, stalledState(p));
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(null);

        run(task);

        // Owner resolution happens before any counter/logging, so a missing owner must not cause a
        // restart, a persisted counter, or a misleading "restarting" log.
        verify(taskScheduleService, never()).scheduling(any(), any(), anyBoolean());
        verify(taskService, never()).update(any(Query.class), any(Update.class));
        verify(monitoringLogsService, never()).startTaskErrorLog(any(), any(), any(), any());
    }

    @Test
    void persistsOnlyTheTouchedUnitAtItsOwnPath() {
        Map<String, Object> p = unit("CDC", 1L, "100");
        TaskDto task = task(TaskDto.SYNC_TYPE_SYNC, p, null);

        run(task);

        ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
        verify(taskService).update(any(Query.class), captor.capture());
        Object setValue = captor.getValue().getUpdateObject().get("$set");
        assertTrue(setValue instanceof Map, "expected a $set payload, got: " + setValue);
        Map<?, ?> set = (Map<?, ?>) setValue;
        assertEquals(1, set.size());
        assertTrue(set.containsKey("attrs.incrementStuck." + UNIT_KEY),
                "update must target only the touched unit, was: " + set.keySet());
    }
}
