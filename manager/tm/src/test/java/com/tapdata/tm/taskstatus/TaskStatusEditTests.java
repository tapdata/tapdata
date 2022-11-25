package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;


public class TaskStatusEditTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @MockBean
    private MessageQueueService messageQueueService;

    @Test
    public void testConfirm() {
        initTask(TaskDto.STATUS_EDIT);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.confirmById(taskDto, user, false, false);
        taskDto = taskService.findById(taskId);
        assertEquals(taskDto.getStatus(), TaskDto.STATUS_WAIT_START);
    }


    @Test
    public void testDelete() {
        initTask(TaskDto.STATUS_EDIT);
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        taskService.remove(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETING, taskDto.getStatus());
    }

}
