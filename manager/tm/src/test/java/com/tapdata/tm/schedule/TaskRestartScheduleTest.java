package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.constant.SyncStatus;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/2/19 17:52 Create
 */
public class TaskRestartScheduleTest {

    @Nested
    class WaitRunTaskTest {
        @Test
        void testWaitRunTaskPositive() {
            String userId = "test-user-id";
            TaskDto taskDto = new TaskDto();
            taskDto.setUserId(userId);
            taskDto.setStatus(TaskDto.STATUS_WAIT_RUN);
            taskDto.setSyncStatus(SyncStatus.NORMAL);
            taskDto.setScheduledTime(new Date(System.currentTimeMillis() - 40000));
            List<TaskDto> all = new ArrayList<>();
            all.add(taskDto);

            TaskRestartSchedule taskRestartSchedule = new TaskRestartSchedule();

            TaskService taskService = mock(TaskService.class);
            when(taskService.findAll(any(Query.class))).thenReturn(all);
            taskRestartSchedule.setTaskService(taskService);

            MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
            taskRestartSchedule.setMonitoringLogsService(monitoringLogsService);

            StateMachineService stateMachineService = mock(StateMachineService.class);
            taskRestartSchedule.setStateMachineService(stateMachineService);
            when(stateMachineService.executeAboutTask(any(TaskDto.class), any(), any())).thenReturn(StateMachineResult.ok());

            TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
            taskRestartSchedule.setTaskScheduleService(taskScheduleService);

            UserService userService = mock(UserService.class);
            List<UserDetail> userDetails = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn(userId);
            userDetails.add(userDetail);
            when(userService.getUserByIdList(anyList())).thenReturn(userDetails);
            taskRestartSchedule.setUserService(userService);

            // Test positive case
            // Verify that the taskScheduleService methods are called
            // Verify that the stateMachineService method is executed
            // Assert the correctness of the code logic
            taskRestartSchedule.waitRunTask();

            verify(stateMachineService, times(1)).executeAboutTask(any(TaskDto.class), any(DataFlowEvent.class), any(UserDetail.class));
            verify(taskScheduleService, times(1)).scheduling(any(), any());
        }

        @Test
        void testWaitRunTaskNegative() {
            List<TaskDto> all = new ArrayList<>();

            TaskRestartSchedule taskRestartSchedule = new TaskRestartSchedule();

            TaskService taskService = mock(TaskService.class);
            when(taskService.findAll(any(Query.class))).thenReturn(all);
            taskRestartSchedule.setTaskService(taskService);

            MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
            taskRestartSchedule.setMonitoringLogsService(monitoringLogsService);

            StateMachineService stateMachineService = mock(StateMachineService.class);
            taskRestartSchedule.setStateMachineService(stateMachineService);

            TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
            taskRestartSchedule.setTaskScheduleService(taskScheduleService);

            // Test negative case where the list is empty
            // Ensure that no methods are called
            taskRestartSchedule.waitRunTask();

            verifyNoInteractions(monitoringLogsService);
            verifyNoInteractions(stateMachineService);
            verifyNoInteractions(taskScheduleService);
        }
    }

}
