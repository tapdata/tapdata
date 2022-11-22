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

public class TaskStatusWaitRunTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;
    @MockBean
    private TaskScheduleService taskScheduleService;


    @Autowired
    private TaskRestartSchedule taskRestartSchedule;

    @Test
    public void testSucc() {
        initTask(TaskDto.STATUS_WAIT_RUN);
        taskService.running(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RUNNING, taskDto.getStatus());
    }


    @Test
    public void testStop() {
        initTask(TaskDto.STATUS_WAIT_RUN);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, false, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOPPING, taskDto.getStatus());
    }

    @Test
    public void testForceStop() {
        initTask(TaskDto.STATUS_WAIT_RUN, ImmutablePair.of("lastUpdAt", new Date()));
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, true, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }
    @Test
    public void tesOverTime() throws InterruptedException {
        Thread.sleep(5000);
        initTask(TaskDto.STATUS_WAIT_RUN);
        Mockito.doNothing().when(taskScheduleService).scheduling(any(TaskDto.class), any(UserDetail.class));
        taskRestartSchedule.waitRunTask();

        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }
}
