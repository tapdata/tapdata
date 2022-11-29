package com.tapdata.tm.taskstatus.exception;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskstatus.TaskStatusTests;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Update;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TaskExceptionEditTest extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @Test
    public void testWaitStart() {
        initTask(TaskDto.STATUS_WAIT_START);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_WAIT_START, taskDto.getStatus());
    }

    @Test
    public void testScheduling() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testScheduleFailed() {
        initTask(TaskDto.STATUS_SCHEDULE_FAILED);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }

    @Test
    public void testWaitRun() {
        initTask(TaskDto.STATUS_WAIT_RUN);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_WAIT_RUN, taskDto.getStatus());
    }

    @Test
    public void testRunning() {
        initTask(TaskDto.STATUS_RUNNING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RUNNING, taskDto.getStatus());
    }

    @Test
    public void testComplete() {
        initTask(TaskDto.STATUS_COMPLETE);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_COMPLETE, taskDto.getStatus());
    }

    @Test
    public void testError() {
        initTask(TaskDto.STATUS_ERROR);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_ERROR, taskDto.getStatus());
    }

    @Test
    public void testStopping() {
        initTask(TaskDto.STATUS_STOPPING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOPPING, taskDto.getStatus());
    }

    @Test
    public void testStop() {
        initTask(TaskDto.STATUS_STOP);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_STOP, taskDto.getStatus());
    }

    @Test
    public void testRenewing() {
        initTask(TaskDto.STATUS_RENEWING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RENEWING, taskDto.getStatus());
    }

    @Test
    public void testRenewFailed() {
        initTask(TaskDto.STATUS_RENEW_FAILED);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_RENEW_FAILED, taskDto.getStatus());
    }

    @Test
    public void testDeleting() {
        initTask(TaskDto.STATUS_DELETING);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETING, taskDto.getStatus());
    }

    @Test
    public void testDeleteFailed() {
        initTask(TaskDto.STATUS_DELETE_FAILED);
        TaskDto taskDto = taskService.findById(taskId);
        taskService.updateById(taskId, Update.update("status", TaskDto.STATUS_EDIT), user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETE_FAILED, taskDto.getStatus());
    }
}
