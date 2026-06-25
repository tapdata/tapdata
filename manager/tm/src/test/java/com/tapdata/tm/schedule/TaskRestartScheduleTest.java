package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.constant.SyncStatus;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/2/19 17:52 Create
 */
public class TaskRestartScheduleTest {

    TaskRestartSchedule taskRestartSchedule;

    @BeforeEach
    void setUp() {
        taskRestartSchedule = new TaskRestartSchedule();
    }

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

            SettingsService settingsService = mock(SettingsService.class);
            taskRestartSchedule.setSettingsService(settingsService);
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
            verify(taskScheduleService, times(1)).scheduling(any(), any(),any());
        }

        @Test
        void testWaitRunTaskReschedulesWhenAgentAlive() {
            // A wait_run task that exceeds the task heartbeat timeout should be rolled back
            // and rescheduled instead of resending START directly.
            String userId = "test-user-id";
            String agentId = "test-agent-id";

            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setUserId(userId);
            taskDto.setAgentId(agentId);
            taskDto.setStatus(TaskDto.STATUS_WAIT_RUN);
            taskDto.setSyncStatus(SyncStatus.NORMAL);
            taskDto.setScheduledTime(new Date(System.currentTimeMillis() - 40000));
            List<TaskDto> all = new ArrayList<>();
            all.add(taskDto);

            SettingsService settingsService = mock(SettingsService.class);
            taskRestartSchedule.setSettingsService(settingsService);
            TaskService taskService = mock(TaskService.class);
            when(taskService.findAll(any(Query.class))).thenReturn(all);
            taskRestartSchedule.setTaskService(taskService);

            MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
            taskRestartSchedule.setMonitoringLogsService(monitoringLogsService);

            StateMachineService stateMachineService = mock(StateMachineService.class);
            taskRestartSchedule.setStateMachineService(stateMachineService);
            when(stateMachineService.executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.OVERTIME), any(UserDetail.class))).thenReturn(StateMachineResult.ok());

            TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
            taskRestartSchedule.setTaskScheduleService(taskScheduleService);

            UserService userService = mock(UserService.class);
            List<UserDetail> userDetails = new ArrayList<>();
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn(userId);
            userDetails.add(userDetail);
            when(userService.getUserByIdList(anyList())).thenReturn(userDetails);
            taskRestartSchedule.setUserService(userService);

            WorkerService workerService = mock(WorkerService.class);
            WorkerDto worker = new WorkerDto();
            worker.setProcessId(agentId);
            worker.setPingTime(System.currentTimeMillis() - 1000L);
            when(workerService.findByProcessId(eq(agentId), any(UserDetail.class), any(), any())).thenReturn(worker);
            when(workerService.isAgentTimeout(anyLong())).thenReturn(false);
            taskRestartSchedule.setWorkerService(workerService);

            taskRestartSchedule.waitRunTask();

            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
            verify(taskScheduleService, times(1)).scheduling(taskDto, userDetail, true);
            verify(taskScheduleService, never()).sendStartMsg(anyString(), anyString(), any(UserDetail.class));
        }

        @Test
        void testWaitRunTaskNegative() {
            List<TaskDto> all = new ArrayList<>();

            // waitRunTask reads getHeartExpire() at the top before checking the result list,
            // so settingsService must be present even when the list is empty.
            SettingsService settingsService = mock(SettingsService.class);
            taskRestartSchedule.setSettingsService(settingsService);

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

    @Nested
    class skipCloudEngineOfflineTest {
        WorkerService workerService;

        @BeforeEach
        void setUp() {
            taskRestartSchedule = new TaskRestartSchedule();
            workerService = mock(WorkerService.class);
            taskRestartSchedule.setWorkerService(workerService);
        }

        @Test
        void testEngineOnline() {
            // 模拟数据
            String agentId = "test-agent-id";
            UserDetail user = mock(UserDetail.class);
            doReturn("online").when(workerService).checkUsedAgent(eq(agentId), eq(user));

            assertFalse(taskRestartSchedule.skipCloudEngineOffline(agentId, user));
        }

        @Test
        void testEngineOffline() {
            // 模拟数据
            String agentId = "test-agent-id";
            UserDetail user = mock(UserDetail.class);
            doReturn("offline").when(workerService).checkUsedAgent(eq(agentId), eq(user));

            assertTrue(taskRestartSchedule.skipCloudEngineOffline(agentId, user));
        }
    }

    @Nested
    class SchedulingTaskTest {
        String userId = "test-user-id";
        String agentId = "test-agent-id";
        UserDetail userDetail;
        TaskService taskService;
        TaskScheduleService taskScheduleService;
        StateMachineService stateMachineService;

        private TaskDto schedulingTask() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setUserId(userId);
            taskDto.setAgentId(agentId);
            taskDto.setStatus(TaskDto.STATUS_SCHEDULING);
            taskDto.setSyncStatus(SyncStatus.NORMAL);
            taskDto.setSchedulingTime(new Date(System.currentTimeMillis() - 40000));
            return taskDto;
        }

        // taskActiveCount: result of taskService.count(...) probing task.pingTime freshness.
        // 0 => engine is NOT executing the task; >0 => engine is actively pinging it.
        private void wire(TaskDto taskDto, long taskActiveCount) {
            List<TaskDto> all = new ArrayList<>();
            all.add(taskDto);

            // settingsService unstubbed: isCloud()->null->DAAS(non-cloud); getHeartExpire->default
            SettingsService settingsService = mock(SettingsService.class);
            taskRestartSchedule.setSettingsService(settingsService);

            taskService = mock(TaskService.class);
            when(taskService.findAll(any(Query.class))).thenReturn(all);
            when(taskService.count(any(Query.class))).thenReturn(taskActiveCount);
            taskRestartSchedule.setTaskService(taskService);

            MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
            taskRestartSchedule.setMonitoringLogsService(monitoringLogsService);

            stateMachineService = mock(StateMachineService.class);
            when(stateMachineService.executeAboutTask(any(TaskDto.class), any(), any())).thenReturn(StateMachineResult.ok());
            taskRestartSchedule.setStateMachineService(stateMachineService);

            taskScheduleService = mock(TaskScheduleService.class);
            taskRestartSchedule.setTaskScheduleService(taskScheduleService);

            TransformSchemaService transformSchema = mock(TransformSchemaService.class);
            taskRestartSchedule.setTransformSchema(transformSchema);

            UserService userService = mock(UserService.class);
            userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn(userId);
            List<UserDetail> userDetails = new ArrayList<>();
            userDetails.add(userDetail);
            when(userService.getUserByIdList(anyList())).thenReturn(userDetails);
            taskRestartSchedule.setUserService(userService);

            // The engine PROCESS is alive (this is the stuck scenario): worker heartbeat is fresh.
            WorkerService workerService = mock(WorkerService.class);
            WorkerDto worker = new WorkerDto();
            worker.setProcessId(agentId);
            worker.setPingTime(System.currentTimeMillis() - 1000L);
            when(workerService.findByProcessId(eq(agentId), any(UserDetail.class), any(), any())).thenReturn(worker);
            when(workerService.isAgentTimeout(anyLong())).thenReturn(false);
            taskRestartSchedule.setWorkerService(workerService);
        }

        @Test
        void testReschedulesWhenAgentAliveButNotRunningTask() {
            // Engine process alive but NOT executing the task (task.pingTime stale -> count 0):
            // TM must re-dispatch the task and must NOT mark it schedule_failed.
            TaskDto taskDto = schedulingTask();
            wire(taskDto, 0L);

            taskRestartSchedule.schedulingTask();

            verify(taskScheduleService, times(1)).scheduling(taskDto, userDetail, true);
            verify(stateMachineService, never()).executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.SCHEDULE_FAILED), any(UserDetail.class));
        }

        @Test
        void testSkipsWhenAgentRunningTask() {
            // Engine IS executing the task (fresh task.pingTime -> count > 0): do not re-dispatch.
            TaskDto taskDto = schedulingTask();
            wire(taskDto, 1L);

            taskRestartSchedule.schedulingTask();

            verify(taskScheduleService, never()).scheduling(any(), any(), any());
            verify(stateMachineService, never()).executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.SCHEDULE_FAILED), any(UserDetail.class));
        }
    }

    @Nested
    class EngineRestartNeedStartTaskTest {
        String userId = "test-user-id";
        String agentId = "test-agent-id";
        UserDetail userDetail;
        TaskService taskService;
        TaskScheduleService taskScheduleService;
        StateMachineService stateMachineService;

        private TaskDto runningTask() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setUserId(userId);
            taskDto.setAgentId(agentId);
            taskDto.setStatus(TaskDto.STATUS_RUNNING);
            taskDto.setSyncStatus(SyncStatus.NORMAL);
            return taskDto;
        }

        private void wire(TaskDto taskDto, long taskActiveCount) {
            List<TaskDto> all = new ArrayList<>();
            all.add(taskDto);

            SettingsService settingsService = mock(SettingsService.class);
            taskRestartSchedule.setSettingsService(settingsService);

            taskService = mock(TaskService.class);
            when(taskService.findAll(any(Query.class))).thenReturn(all);
            when(taskService.count(any(Query.class))).thenReturn(taskActiveCount);
            taskRestartSchedule.setTaskService(taskService);

            MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
            taskRestartSchedule.setMonitoringLogsService(monitoringLogsService);

            stateMachineService = mock(StateMachineService.class);
            when(stateMachineService.executeAboutTask(any(TaskDto.class), any(), any())).thenReturn(StateMachineResult.ok());
            taskRestartSchedule.setStateMachineService(stateMachineService);

            taskScheduleService = mock(TaskScheduleService.class);
            taskRestartSchedule.setTaskScheduleService(taskScheduleService);

            TransformSchemaService transformSchema = mock(TransformSchemaService.class);
            taskRestartSchedule.setTransformSchema(transformSchema);

            UserService userService = mock(UserService.class);
            userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn(userId);
            List<UserDetail> userDetails = new ArrayList<>();
            userDetails.add(userDetail);
            when(userService.getUserByIdList(anyList())).thenReturn(userDetails);
            taskRestartSchedule.setUserService(userService);

            // worker process alive AND available in the system (so a reschedule target exists)
            WorkerService workerService = mock(WorkerService.class);
            WorkerDto worker = new WorkerDto();
            worker.setProcessId(agentId);
            worker.setPingTime(System.currentTimeMillis() - 1000L);
            when(workerService.findByProcessId(eq(agentId), any(UserDetail.class), any(), any())).thenReturn(worker);
            when(workerService.isAgentTimeout(anyLong())).thenReturn(false);
            Worker availWorker = new Worker();
            availWorker.setUserId(userId);
            availWorker.setProcessId(agentId);
            List<Worker> avail = new ArrayList<>();
            avail.add(availWorker);
            when(workerService.findAvailableAgentBySystem(anyList())).thenReturn(avail);
            taskRestartSchedule.setWorkerService(workerService);

            MetadataDefinitionService metadataDefinitionService = mock(MetadataDefinitionService.class);
            when(metadataDefinitionService.orderTaskByTagPriority(anyList())).thenReturn(all);
            taskRestartSchedule.setMetadataDefinitionService(metadataDefinitionService);
        }

        @Test
        void testReschedulesRunningTaskWhenAgentNotPingingTask() {
            // running task whose pingTime is stale (in the result set) and the engine is NOT
            // pinging it (count 0) -> must OVERTIME + reschedule even though the worker is alive.
            TaskDto taskDto = runningTask();
            wire(taskDto, 0L);

            taskRestartSchedule.engineRestartNeedStartTask();

            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
            verify(taskScheduleService, times(1)).scheduling(taskDto, userDetail, true);
        }

        @Test
        void testSkipsRunningTaskWhenAgentPingingTask() {
            // engine IS pinging this task (count > 0) -> genuinely running -> do not demote.
            TaskDto taskDto = runningTask();
            wire(taskDto, 1L);

            taskRestartSchedule.engineRestartNeedStartTask();

            verify(stateMachineService, never()).executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.OVERTIME), any(UserDetail.class));
            verify(taskScheduleService, never()).scheduling(any(), any(), any());
        }
    }
}
