package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskStatusRenewFailedTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @Test
    public void testRenew() {
        initTask(TaskDto.STATUS_RENEW_FAILED);
        taskService.renew(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RENEWING, taskDto.getStatus());
    }


    @Test
    public void testDel() {
        initTask(TaskDto.STATUS_RENEW_FAILED);

        taskService.remove(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETING, taskDto.getStatus());
    }
}
