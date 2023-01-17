package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.schedule.TaskRestartSchedule;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class TaskStatusRunningTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;
    @MockBean
    private TaskScheduleService taskScheduleService;

    @Autowired
    private TaskRestartSchedule taskRestartSchedule;

    @Test
    public void testComplete() {
        initTask(TaskDto.STATUS_RUNNING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.complete(taskId, user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_COMPLETE, taskDto.getStatus());
    }


    @Test
    public void testStop() {
        initTask(TaskDto.STATUS_RUNNING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, false, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOPPING, taskDto.getStatus());
    }

    @Test
    public void testForceStop() {
        initTask(TaskDto.STATUS_RUNNING, ImmutablePair.of("lastUpdAt", new Date()));
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, true, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }

    @Test
    public void testError() {
        initTask(TaskDto.STATUS_RUNNING);
        taskService.runError(taskId, user, null, null);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_ERROR, taskDto.getStatus());
    }

    @Test
    public void testOverTime() {
        initTask(TaskDto.STATUS_RUNNING, ImmutablePair.of("pingTime", System.currentTimeMillis() - 500000));
        Mockito.doNothing().when(taskScheduleService).scheduling(any(TaskDto.class), any(UserDetail.class));
        taskRestartSchedule.engineRestartNeedStartTask();
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }


    @Test
    public void testRunning() {
        initTask(TaskDto.STATUS_RUNNING);
        taskService.running(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RUNNING, taskDto.getStatus());
    }
}
