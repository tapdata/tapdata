package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class TaskAlarmSchedulerTest {
    TaskAlarmScheduler taskAlarmScheduler;
    TaskService taskService;
    AlarmService alarmService;
    WorkerService workerService;
    UserService userService;
    SettingsService settingsService;
    AgentGroupService agentGroupService;

    UserDetail userDetail;
    TaskDto taskDto;

    @BeforeEach
    void init() {
        taskAlarmScheduler = mock(TaskAlarmScheduler.class);
        taskService = mock(TaskService.class);
        alarmService = mock(AlarmService.class);
        workerService = mock(WorkerService.class);
        userService = mock(UserService.class);
        settingsService = mock(SettingsService.class);
        agentGroupService = mock(AgentGroupService.class);

        userDetail = mock(UserDetail.class);
        taskDto = mock(TaskDto.class);
    }
    @Nested
    class FindWorkerListTest {
        List<Worker> workerList;
        Worker worker;

        List<String> processIdList;
        String processId;
        @BeforeEach
        void init() {
            processId = "id";
            processIdList = new ArrayList<>();
            processIdList.add(processId);

            workerList = new ArrayList<>();
            worker = mock(Worker.class);
            when(worker.getProcessId()).thenReturn(processId);
            workerList.add(worker);
            when(workerService.findAvailableAgentBySystem(userDetail)).thenReturn(workerList);
        }
        void assertVerify(String accessType,
                          WorkerService w, AgentGroupService a, TaskDto d,
                          int findAvailableAgent,
                          int getAccessNodeType,
                          int typeTimes) {
            ReflectionTestUtils.setField(taskAlarmScheduler, "workerService", w);
            ReflectionTestUtils.setField(taskAlarmScheduler, "agentGroupService", a);
            when(taskDto.getAccessNodeType()).thenReturn(accessType);
            when(taskAlarmScheduler.findWorkerList(d,userDetail)).thenCallRealMethod();
            List<Worker> list = taskAlarmScheduler.findWorkerList(d, userDetail);
            Assertions.assertNotNull(list);
            verify(workerService, times(findAvailableAgent)).findAvailableAgentBySystem(userDetail);
            verify(taskDto, times(getAccessNodeType)).getAccessNodeType();
            verify(agentGroupService, times(typeTimes)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(worker, times(typeTimes*workerList.size())).getProcessId();
        }

        @Test
        void testWorkerServiceIsNull() {
            assertVerify("", null, agentGroupService, taskDto,
                    0, 0, 0);
        }
        @Test
        void testAgentGroupServiceIsNull() {
            assertVerify("", workerService, null, taskDto,
                    1, 0, 0);
        }
        @Test
        void testTaskDtoIsNull() {
            assertVerify("", workerService, agentGroupService, null,
                    1, 0, 0);
        }
        @Test
        void testNotIsManually() {
            assertVerify(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), workerService, agentGroupService, taskDto,
                    1, 1, 0);
        }
        @Test
        void testNormal() {
            assertVerify(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), workerService, agentGroupService, taskDto,
                    1, 1, 1);
        }
    }

    @Nested
    class EngineHeartbeatAlarmTest {
        TaskAlarmScheduler scheduler;
        TaskService taskSvc;
        WorkerService workerSvc;
        SettingsService settingsSvc;
        MongoTemplate mongoTemplate;

        @BeforeEach
        void init() {
            scheduler = new TaskAlarmScheduler();
            taskSvc = mock(TaskService.class);
            workerSvc = mock(WorkerService.class);
            settingsSvc = mock(SettingsService.class);
            mongoTemplate = mock(MongoTemplate.class);
            scheduler.setTaskService(taskSvc);
            scheduler.setWorkerService(workerSvc);
            scheduler.setSettingsService(settingsSvc);
            scheduler.setMongoTemplate(mongoTemplate);
            when(settingsSvc.getValueByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT))
                    .thenReturn("60");
        }

        private WorkerDto buildWorker(String processId, String hostname, long pingTime, Boolean isDeleted, Boolean stopping) {
            WorkerDto w = new WorkerDto();
            w.setProcessId(processId);
            w.setHostname(hostname);
            w.setPingTime(pingTime);
            w.setIsDeleted(isDeleted);
            w.setStopping(stopping);
            return w;
        }

        @Test
        @DisplayName("returns early when no workers exist")
        void testNoWorkers() {
            when(workerSvc.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            scheduler.engineHeartbeatAlarm();
            verify(mongoTemplate, never()).find(any(Query.class), eq(AlarmInfo.class));
            verify(mongoTemplate, never()).insert(any(AlarmInfo.class));
        }

        @Test
        @DisplayName("creates ENGINE_OFFLINE alarm when worker ping expired")
        void testOfflineDetected() {
            long now = System.currentTimeMillis();
            WorkerDto offlineWorker = buildWorker("agent-1", "host-1", now - 1_000_000L, false, false);
            when(workerSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(offlineWorker));
            when(mongoTemplate.find(any(Query.class), eq(AlarmInfo.class))).thenReturn(new ArrayList<>());
            TaskDto t = new TaskDto();
            t.setAgentId("agent-1");
            when(taskSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(t, t, t));

            scheduler.engineHeartbeatAlarm();

            ArgumentCaptor<AlarmInfo> captor = ArgumentCaptor.forClass(AlarmInfo.class);
            verify(mongoTemplate, times(1)).insert(captor.capture());
            AlarmInfo saved = captor.getValue();
            Assertions.assertEquals(AlarmKeyEnum.ENGINE_OFFLINE, saved.getMetric());
            Assertions.assertEquals(AlarmStatusEnum.ING, saved.getStatus());
            Assertions.assertEquals("agent-1", saved.getAgentId());
            Assertions.assertEquals("host-1", saved.getName());
            Assertions.assertEquals("host-1", saved.getParam().get("agentName"));
            Assertions.assertEquals(3L, saved.getParam().get("taskCount"));
        }

        @Test
        @DisplayName("skips ENGINE_OFFLINE when an ING record already exists for the agent")
        void testOfflineDeduplicated() {
            long now = System.currentTimeMillis();
            WorkerDto offlineWorker = buildWorker("agent-1", "host-1", now - 1_000_000L, false, false);
            when(workerSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(offlineWorker));
            AlarmInfo existing = AlarmInfo.builder()
                    .agentId("agent-1").metric(AlarmKeyEnum.ENGINE_OFFLINE).status(AlarmStatusEnum.ING).build();
            when(mongoTemplate.find(any(Query.class), eq(AlarmInfo.class))).thenReturn(Arrays.asList(existing));
            when(taskSvc.findAll(any(Query.class))).thenReturn(new ArrayList<>());

            scheduler.engineHeartbeatAlarm();

            verify(mongoTemplate, never()).insert(any(AlarmInfo.class));
            verify(mongoTemplate, never()).updateMulti(any(Query.class), any(Update.class), eq(AlarmInfo.class));
        }

        @Test
        @DisplayName("flips OFFLINE to RECOVER and inserts ENGINE_ONLINE when agent comes back")
        void testRecoveryDetected() {
            long now = System.currentTimeMillis();
            WorkerDto liveWorker = buildWorker("agent-1", "host-1-new", now, false, false);
            when(workerSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(liveWorker));
            AlarmInfo offline = AlarmInfo.builder()
                    .agentId("agent-1").name("host-1-old")
                    .metric(AlarmKeyEnum.ENGINE_OFFLINE).status(AlarmStatusEnum.ING).build();
            when(mongoTemplate.find(any(Query.class), eq(AlarmInfo.class))).thenReturn(Arrays.asList(offline));

            scheduler.engineHeartbeatAlarm();

            verify(mongoTemplate, times(1)).updateMulti(any(Query.class), any(Update.class), eq(AlarmInfo.class));
            ArgumentCaptor<AlarmInfo> captor = ArgumentCaptor.forClass(AlarmInfo.class);
            verify(mongoTemplate, times(1)).insert(captor.capture());
            AlarmInfo saved = captor.getValue();
            Assertions.assertEquals(AlarmKeyEnum.ENGINE_ONLINE, saved.getMetric());
            Assertions.assertEquals(AlarmStatusEnum.ING, saved.getStatus());
            Assertions.assertEquals("agent-1", saved.getAgentId());
            Assertions.assertEquals("host-1-new", saved.getName());
            Assertions.assertEquals("host-1-new", saved.getParam().get("agentName"));
        }

        @Test
        @DisplayName("recovery deduplicates by agentId when offline records contain duplicates")
        void testRecoveryDeduplicatesByAgentId() {
            long now = System.currentTimeMillis();
            WorkerDto liveWorker = buildWorker("agent-1", "host-1", now, false, false);
            when(workerSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(liveWorker));
            AlarmInfo dup1 = AlarmInfo.builder()
                    .agentId("agent-1").name("host-1")
                    .metric(AlarmKeyEnum.ENGINE_OFFLINE).status(AlarmStatusEnum.ING).build();
            AlarmInfo dup2 = AlarmInfo.builder()
                    .agentId("agent-1").name("host-1")
                    .metric(AlarmKeyEnum.ENGINE_OFFLINE).status(AlarmStatusEnum.ING).build();
            when(mongoTemplate.find(any(Query.class), eq(AlarmInfo.class))).thenReturn(Arrays.asList(dup1, dup2));

            scheduler.engineHeartbeatAlarm();

            verify(mongoTemplate, times(1)).updateMulti(any(Query.class), any(Update.class), eq(AlarmInfo.class));
            verify(mongoTemplate, times(1)).insert(any(AlarmInfo.class));
        }

        @Test
        @DisplayName("no recovery when no offline alarm exists for an online worker")
        void testNoRecoveryWhenNoOfflineAlarm() {
            long now = System.currentTimeMillis();
            WorkerDto liveWorker = buildWorker("agent-1", "host-1", now, false, false);
            when(workerSvc.findAll(any(Query.class))).thenReturn(Arrays.asList(liveWorker));
            when(mongoTemplate.find(any(Query.class), eq(AlarmInfo.class))).thenReturn(new ArrayList<>());

            scheduler.engineHeartbeatAlarm();

            verify(mongoTemplate, never()).updateMulti(any(Query.class), any(Update.class), eq(AlarmInfo.class));
            verify(mongoTemplate, never()).insert(any(AlarmInfo.class));
        }
    }
}
