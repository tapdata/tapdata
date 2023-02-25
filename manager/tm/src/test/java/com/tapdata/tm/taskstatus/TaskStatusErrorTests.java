package com.tapdata.tm.taskstatus;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class TaskStatusErrorTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;
    @MockBean
    private TaskScheduleService taskScheduleService;


    @MockBean
    private MessageQueueService messageQueueService;


    @Test
    public void testRun() {
        initTask(TaskDto.STATUS_ERROR);
        TaskDto taskDto = taskService.findById(taskId);
        Mockito.doNothing().when(taskScheduleService).scheduling(any(TaskDto.class), any(UserDetail.class));
        taskService.run(taskDto, user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }


    @Test
    public void testRenew() {
        initTask(TaskDto.STATUS_ERROR);
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        taskService.renew(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RENEWING, taskDto.getStatus());
    }

    @Test
    public void testEdit() {
        initTask(TaskDto.STATUS_ERROR);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.confirmById(taskDto, user, true, false);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_ERROR, taskDto.getStatus());
    }
    @Test
    public void testDelete() {
        initTask(TaskDto.STATUS_ERROR);
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        taskService.remove(taskId, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETING, taskDto.getStatus());
    }
}
