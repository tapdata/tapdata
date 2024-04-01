package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
            when(taskDto.getAccessNodeType()).thenReturn(accessType);
            when(taskAlarmScheduler.findWorkerList(w,a,d,userDetail)).thenCallRealMethod();
            List<Worker> list = taskAlarmScheduler.findWorkerList(w, a, d, userDetail);
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