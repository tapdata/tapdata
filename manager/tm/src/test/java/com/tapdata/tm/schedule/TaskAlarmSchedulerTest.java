package com.tapdata.tm.schedule;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
    class TaskAgentAlarmTest {
        @BeforeEach
        void init() {
            taskAlarmScheduler = new TaskAlarmScheduler();
            ReflectionTestUtils.setField(taskAlarmScheduler, "taskService", taskService);
            ReflectionTestUtils.setField(taskAlarmScheduler, "alarmService", alarmService);
            ReflectionTestUtils.setField(taskAlarmScheduler, "workerService", workerService);
            ReflectionTestUtils.setField(taskAlarmScheduler, "userService", userService);
            ReflectionTestUtils.setField(taskAlarmScheduler, "settingsService", settingsService);
            ReflectionTestUtils.setField(taskAlarmScheduler, "agentGroupService", agentGroupService);
        }

        @Test
        void testDownAgentAlarmKeepsOriginalAgentIdWhenTaskIsRescheduled() {
            ObjectId taskObjectId = new ObjectId();
            TaskDto runningTask = new TaskDto();
            runningTask.setId(taskObjectId);
            runningTask.setStatus(TaskDto.STATUS_RUNNING);
            runningTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            runningTask.setAgentId("old-agent");
            runningTask.setUserId("user-id");
            runningTask.setName("task-name");

            WorkerDto stoppedWorker = new WorkerDto();
            stoppedWorker.setProcessId("old-agent");
            stoppedWorker.setPingTime(0L);

            Worker availableWorker = new Worker();
            availableWorker.setProcessId("new-agent");

            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setProcessId("new-agent");

            AlarmInfo existingAlarm = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING)
                    .metric(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN)
                    .agentId("old-agent")
                    .taskId(taskObjectId.toHexString())
                    .build();
            ObjectId alarmId = new ObjectId();
            existingAlarm.setId(alarmId);
            Date firstOccurrenceTime = new Date(System.currentTimeMillis() - 60000L);
            existingAlarm.setFirstOccurrenceTime(firstOccurrenceTime);
            existingAlarm.setTally(3);

            when(settingsService.getValueByCategoryAndKey(any(), any())).thenReturn("DAAS", null);
            when(taskService.findAll(any(Query.class))).thenReturn(Collections.singletonList(runningTask));
            when(workerService.findAll(any(Query.class))).thenReturn(Collections.singletonList(stoppedWorker));
            when(userService.getUserByIdList(Collections.singletonList("user-id"))).thenReturn(Collections.singletonList(userDetail));
            when(userDetail.getUserId()).thenReturn("user-id");
            when(alarmService.checkOpen(eq(runningTask), eq(null), eq(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN), eq(null), eq(userDetail))).thenReturn(true);
            when(workerService.findAvailableAgentBySystem(userDetail)).thenReturn(Collections.singletonList(availableWorker));
            when(workerService.scheduleTaskToEngine(any(TaskDto.class), eq(userDetail), eq("task"), eq("task-name"))).thenReturn(calculationEngineVo);
            when(alarmService.query(any(Query.class))).thenReturn(Collections.singletonList(existingAlarm));

            taskAlarmScheduler.taskAgentAlarm();

            ArgumentCaptor<AlarmInfo> captor = ArgumentCaptor.forClass(AlarmInfo.class);
            verify(alarmService).save(captor.capture());
            AlarmInfo savedAlarm = captor.getValue();
            Assertions.assertEquals(alarmId, savedAlarm.getId());
            Assertions.assertEquals("old-agent", savedAlarm.getAgentId());
            Assertions.assertEquals("new-agent", savedAlarm.getParam().get("otherAgentName"));
            Assertions.assertEquals(firstOccurrenceTime, savedAlarm.getFirstOccurrenceTime());
            Assertions.assertEquals(4, savedAlarm.getTally());
            Assertions.assertNotNull(savedAlarm.getLastOccurrenceTime());
        }
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
}
