package com.tapdata.tm.base.aop;

import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class MeasureAOPTest {
    @Nested
    class TaskIncrementDelayAlarmTest{
        private TaskService taskService;
        private AlarmService alarmService;
        private UserService userService;
        Map<String, Map<String, AtomicInteger>> obsMap = Maps.newConcurrentMap();
        String taskId = "66837fb973828322f83f7074";
        List<AlarmInfo> alarmInfos=new ArrayList<>();

        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            alarmService = mock(AlarmService.class);
            userService = mock(UserService.class);
            Map<String,AtomicInteger> infoMap=new HashMap<>();
            infoMap.put(taskId+"-replicateLag",new AtomicInteger(50000));
            obsMap.put(taskId,infoMap);
            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE).status(AlarmStatusEnum.ING).type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY).firstOccurrenceTime(new Date(System.currentTimeMillis()-50000)).build();
            alarmInfos.add(alarmInfo);
            when(alarmService.find(anyString(), any(), eq(AlarmKeyEnum.TASK_INCREMENT_DELAY))).thenReturn(alarmInfos);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag not greater alarmRule ms then taskReplicateLagCount is less than alarmRule point")
        @Test
        void test1(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.RECOVERY,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.RECOVER,alarmInfo.getStatus());
                assertEquals("TASK_INCREMENT_DELAY_RECOVER",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto,taskId,10,alarmRuleDto);

        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and taskReplicateLagCount is greater than alarmRule point")
        @Test
        void test2(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.GREATER,flag);
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 500000, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and and info map is null ,so taskReplicateLagCount is less than alarmRule point")
        @Test
        void test3(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.RECOVERY,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.RECOVER,alarmInfo.getStatus());
                assertEquals("TASK_INCREMENT_DELAY_RECOVER",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 500000, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is less and replicateLag less alarmRule ms and and info map is not null ,so taskReplicateLagCount is greater than alarmRule point")
        @Test
        void test4(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            alarmRuleDto.setEqualsFlag(-1);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.LESS,flag);
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 3, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is less and replicateLag less alarmRule ms and and info map is not null ,so taskReplicateLagCount is greater than alarmRule point and continueTime greater 1 MINUTE")
        @Test
        void test5(){
            AlarmInfo listAlarmInfo = alarmInfos.get(0);
            listAlarmInfo.setFirstOccurrenceTime(new Date(System.currentTimeMillis()-1000000000));
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            alarmRuleDto.setEqualsFlag(-1);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.LESS,flag);
                assertEquals("TASK_INCREMENT_DELAY_ALWAYS",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 3, alarmRuleDto);
        }
    }

    @NotNull
    private static AlarmRuleDto getAlarmRuleDto() {
        AlarmRuleDto alarmRuleDto = new AlarmRuleDto();
        alarmRuleDto.setKey(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        alarmRuleDto.setPoint(24);
        alarmRuleDto.setEqualsFlag(1);
        alarmRuleDto.setMs(40000);
        return alarmRuleDto;
    }
}
