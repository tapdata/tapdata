package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class UpdateRecordStatusEventHandlerTest {
    private UpdateRecordStatusEventHandler handler;
    private TaskService taskService;
    @BeforeEach
    void buildHandler(){
        handler = new UpdateRecordStatusEventHandler();
        taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(handler,"taskService",taskService);
    }
    @Nested
    class TaskAlarmTest{
        @Test
        @DisplayName("test task alarm for task alarm")
        void test1(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("stop");
                TaskDto stopTaskDto = mock(TaskDto.class);
                when(taskService.findById(any())).thenReturn(stopTaskDto);
                when(alarmService.checkOpen(stopTaskDto,null, AlarmKeyEnum.TASK_STATUS_STOP, null, data.getUserDetail())).thenReturn(true);
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).save(any());
            }
        }
        @Test
        @DisplayName("test task alarm for task running")
        void test2(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("running");
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).closeWhenTaskRunning(any());
            }
        }
        @Test
        @DisplayName("test task alarm for task error")
        void test3(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("error");
                TaskDto taskDto = mock(TaskDto.class);
                when(taskService.findById(any())).thenReturn(taskDto);
                when(alarmService.checkOpen(taskDto,null, AlarmKeyEnum.TASK_STATUS_ERROR, null, data.getUserDetail())).thenReturn(true);
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).save(any());
            }
        }
    }
}
