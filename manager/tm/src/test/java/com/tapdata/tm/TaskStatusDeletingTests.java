package com.tapdata.tm;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.schedule.TaskResetSchedule;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.service.TaskService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

import static com.mongodb.internal.connection.tlschannel.util.Util.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class TaskStatusDeletingTests extends TaskStatusTests {

    @Autowired
    private TaskService taskService;
    @Autowired
    private TaskResetLogService taskResetLogService;

    @Autowired
    private TaskResetSchedule taskResetSchedule;

    @Test
    public void testSucc() {
        initTask(TaskDto.STATUS_DELETING);

        TaskResetEventDto eventDto = new TaskResetEventDto();
        eventDto.setTaskId(taskId.toHexString());
        eventDto.setStatus(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED);
        taskResetLogService.save(eventDto, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertTrue(taskDto.is_deleted());
    }


    @Test
    public void testStatusFailed() {
        initTask(TaskDto.STATUS_DELETING);

        TaskResetEventDto eventDto = new TaskResetEventDto();
        eventDto.setTaskId(taskId.toHexString());
        eventDto.setStatus(TaskResetEventDto.ResetStatusEnum.TASK_FAILED);
        taskResetLogService.save(eventDto, user);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETE_FAILED, taskDto.getStatus());
    }

    @Test
    public void testStatusOverTime() throws InterruptedException {
        Thread.sleep(5000);
        initTask(TaskDto.STATUS_DELETING,  ImmutablePair.of("lastUpdAt", new Date(System.currentTimeMillis() - 500000)));
        taskResetSchedule.checkNoResponseOp();
        //Mockito.when(taskService.checkPdkTask(any(TaskDto.class), user)).thenReturn(true);
        TaskDto taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_DELETE_FAILED, taskDto.getStatus());
    }
}
