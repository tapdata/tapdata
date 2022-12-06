package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.schedule.TaskRestartSchedule;
import com.tapdata.tm.task.service.TaskService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskStatusStoppingTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRestartSchedule taskRestartSchedule;




    @Test
    public void testStopped() {
        initTask(TaskDto.STATUS_STOPPING);
        taskService.stopped(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }

    @Test
    public void testForceStop() {
        initTask(TaskDto.STATUS_STOPPING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.pause(taskDto, user, true, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }


    @Test
    public void testOverTime() {
        initTask(TaskDto.STATUS_STOPPING, ImmutablePair.of("lastUpdAt", new Date(System.currentTimeMillis() - 500000)));
        taskRestartSchedule.stoppingTask();
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }

    @Test
    public void testComplete() {
        initTask(TaskDto.STATUS_STOPPING);
        taskService.complete(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_COMPLETE, taskDto.getStatus());
    }



    @Test
    public void testError() {
        initTask(TaskDto.STATUS_STOPPING);
        taskService.runError(taskId, user, null, null);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_ERROR, taskDto.getStatus());
    }
}
