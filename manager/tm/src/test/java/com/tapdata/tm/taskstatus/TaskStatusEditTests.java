package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskStatusEditTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @Test
    public void testConfirm() {
        initTask(TaskDto.STATUS_EDIT);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.confirmById(taskDto, user, false, false);
        taskDto = taskService.findById(taskId);
        assertEquals(taskDto.getStatus(), TaskDto.STATUS_WAIT_START);
    }

}
