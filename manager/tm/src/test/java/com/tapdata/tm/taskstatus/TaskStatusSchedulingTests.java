package com.tapdata.tm.taskstatus;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.schedule.TaskRestartSchedule;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskStatusSchedulingTests extends TaskStatusTests {

    @Autowired
    private TaskScheduleService taskScheduleService;
    @Autowired
    private TaskService taskService;

    @MockBean
    private WorkerService workerService;

    @Autowired
    private TaskRestartSchedule taskRestartSchedule;

    @Test
    public void testScheduleSucc() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setAvailable(1);
        calculationEngineVo.setProcessId("zed_flowEngin");
        Mockito.when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName()))
                .then((Answer<CalculationEngineVo>) invocation -> {
                    SchedulableDto taskDto1 = invocation.getArgument(0);
                    taskDto1.setAgentId("zed_flowEngin");
                    return calculationEngineVo;
                });
        taskScheduleService.scheduling(taskDto, user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_WAIT_RUN, taskDto.getStatus());
    }

    @Test
    public void testScheduleFail() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setAvailable(1);
        calculationEngineVo.setProcessId("zed_flowEngin");
        Mockito.when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
       try {

        taskScheduleService.scheduling(taskDto, user);
       } catch (BizException e) {
           assertEquals("Task.AgentNotFound", e.getErrorCode());
       }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }


    @Test
    public void testScheduleOverTime() throws InterruptedException {
        //让定时任务先跑
        Thread.sleep(5000);
        initTask(TaskDto.STATUS_SCHEDULING, ImmutablePair.of("lastUpdAt", new Date(System.currentTimeMillis() - 500000))
                , ImmutablePair.of("scheduleDate", System.currentTimeMillis() - 500000));
        taskRestartSchedule.schedulingTask();
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }

    @Test
    public void testScheduleStop() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, false, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }
}
