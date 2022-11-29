package com.tapdata.tm.taskstatus.exception;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.taskstatus.TaskStatusTests;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TaskSchedulingTest extends TaskStatusTests {

    @Autowired
    private TaskService taskService;

    @Autowired
    private StateMachineService stateMachineService;

    @Test
    public void testStart() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.START, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }




    @Test
    public void testRunning() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RUNNING, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }


    @Test
    public void testComplete() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.COMPLETED, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testStopped() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOPPED, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testError() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.ERROR, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testConfirm() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.CONFIRM, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testRenew() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testDelete() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.DELETE, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testRenewDelFailed() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_FAILED, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }

    @Test
    public void testRenewDelSuccess() {
        initTask(TaskDto.STATUS_SCHEDULING);
        TaskDto taskDto = taskService.findById(taskId);
        try {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_SUCCESS, user);
        } catch (BizException e) {
            assertEquals("Transition.Not.Supported", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULING, taskDto.getStatus());
    }
}
