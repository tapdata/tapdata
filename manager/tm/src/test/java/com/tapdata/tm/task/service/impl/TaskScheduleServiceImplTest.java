package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.Unit4Util;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskScheduleServiceImplTest {
    TaskScheduleServiceImpl taskScheduleService;

    StateMachineService stateMachineService;
    AgentGroupService agentGroupService;
    Logger logger;
    UserDetail user;

    @BeforeEach
    void init() throws IllegalAccessException, NoSuchFieldException {
        user = mock(UserDetail.class);
        taskScheduleService = mock(TaskScheduleServiceImpl.class);
        stateMachineService = mock(StateMachineService.class);
        ReflectionTestUtils.setField(taskScheduleService, "stateMachineService", stateMachineService);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(taskScheduleService, "agentGroupService", agentGroupService);

        logger = mock(Logger.class);
        Unit4Util.mockSlf4jLog(taskScheduleService, logger);
    }

    @Nested
    class ScheduleFailedTest {
        TaskDto taskDto;
        List<String> processNodeListWithGroup;
        @BeforeEach
        void init() {
            taskDto = mock(TaskDto.class);
            when(taskDto.getName()).thenReturn("name");

            processNodeListWithGroup = new ArrayList<>();
            processNodeListWithGroup.add("id");

            doNothing().when(logger).warn(anyString(), anyString());
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());

            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user)).thenReturn(mock(StateMachineResult.class));
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(processNodeListWithGroup);

            doCallRealMethod().when(taskScheduleService).scheduleFailed(taskDto, user);
        }

        @Test
        void testNormal() {
            Assertions.assertThrows(BizException.class, () -> {
                taskScheduleService.scheduleFailed(taskDto, user);
            });
            verify(logger, times(1)).warn(anyString(), anyString());
            verify(taskDto, times(1)).getName();
            verify(taskDto, times(1)).getAccessNodeType();
            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
        }

        @Test
        void testNotIsManually() {
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            Assertions.assertThrows(BizException.class, () -> {
                taskScheduleService.scheduleFailed(taskDto, user);
            });
            verify(logger, times(1)).warn(anyString(), anyString());
            verify(taskDto, times(1)).getName();
            verify(taskDto, times(1)).getAccessNodeType();
            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
        }

        @Test
        void testProcessNodeListWithGroupIsEmpty() {
            processNodeListWithGroup.remove(0);
            Assertions.assertThrows(BizException.class, () -> {
                taskScheduleService.scheduleFailed(taskDto, user);
            });
            verify(logger, times(1)).warn(anyString(), anyString());
            verify(taskDto, times(1)).getName();
            verify(taskDto, times(1)).getAccessNodeType();
            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, user);
        }
    }
}