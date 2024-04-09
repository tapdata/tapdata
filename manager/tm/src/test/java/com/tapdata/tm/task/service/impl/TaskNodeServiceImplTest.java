package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskNodeServiceImplTest {
    TaskNodeServiceImpl taskNodeService;
    AgentGroupService agentGroupService;
    WorkerService workerService;
    MessageQueueService messageQueueService;

    UserDetail userDetail;

    @BeforeEach
    void init() {
        taskNodeService = mock(TaskNodeServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(taskNodeService, "agentGroupService", agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(taskNodeService, "workerService", workerService);
        messageQueueService = mock(MessageQueueService.class);
        ReflectionTestUtils.setField(taskNodeService, "messageQueueService", messageQueueService);

        userDetail = mock(UserDetail.class);
    }

    @Nested
    class SendMessageAfterFindAgentTest {
        TaskDto taskDto;
        TaskDto taskDtoCopy;

        List<Worker> workers;
        Worker worker;
        List<String> nodeList;
        @BeforeEach
        void init() {
            taskDto = mock(TaskDto.class);
            taskDtoCopy = mock(TaskDto.class);

            nodeList = mock(List.class);
            workers = new ArrayList<>();
            worker = mock(Worker.class);
            when(worker.getProcessId()).thenReturn("id");
            workers.add(worker);

            when(agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail)).thenReturn(nodeList);
            when(workerService.findAvailableAgentByAccessNode(userDetail, nodeList)).thenReturn(workers);
            doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
            doCallRealMethod().when(taskNodeService).sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> taskNodeService.sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail));
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, nodeList);
            verify(worker, times(1)).getProcessId();
            verify(messageQueueService, times(1)).sendMessage(any(MessageQueueDto.class));
        }
        @Test
        void testNotAnyWorkers() {
            workers.remove(0);
            Assertions.assertThrows(BizException.class, () -> taskNodeService.sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail));
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, nodeList);
            verify(worker, times(0)).getProcessId();
            verify(messageQueueService, times(0)).sendMessage(any(MessageQueueDto.class));
        }
    }

    @Nested
    class WsTestRunTest {
        @Test
        void testNormal() {
            doNothing().when(taskNodeService).sendMessageAfterFindAgent(any(TaskDto.class), any(TaskDto.class), any(UserDetail.class));
            doCallRealMethod().when(taskNodeService).wsTestRun(any(UserDetail.class), any(TaskDto.class), any(TaskDto.class));

            Map<String, Object> map = taskNodeService.wsTestRun(userDetail, mock(TaskDto.class), mock(TaskDto.class));
            Assertions.assertNotNull(map);
            Assertions.assertEquals(HashMap.class.getName(), map.getClass().getName());
            verify(taskNodeService, times(1)).sendMessageAfterFindAgent(any(TaskDto.class), any(TaskDto.class), any(UserDetail.class));
        }
    }
}