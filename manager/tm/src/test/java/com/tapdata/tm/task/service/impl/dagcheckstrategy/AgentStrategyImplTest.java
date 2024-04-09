package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class AgentStrategyImplTest {
    AgentStrategyImpl agentStrategy;
    WorkerService workerService;
    AgentGroupService agentGroupService;

    UserDetail userDetail;
    TaskDto taskDto;
    @BeforeEach
    void init() {
        agentStrategy = mock(AgentStrategyImpl.class);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(agentStrategy, "workerService", workerService);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(agentStrategy, "agentGroupService", agentGroupService);

        userDetail = mock(UserDetail.class);
        taskDto = mock(TaskDto.class);
    }

    @Nested
    class GetWorkersTest {
        AtomicReference<String> agent;
        List<String> processNodeListWithGroup;
        List<Worker> availableAgent;
        @BeforeEach
        void init() {
            availableAgent = mock(List.class);
            agent = mock(AtomicReference.class);
            doNothing().when(agent).set(anyString());

            processNodeListWithGroup = mock(List.class);
            when(processNodeListWithGroup.get(0)).thenReturn("id");

            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            when(workerService.findAvailableAgent(userDetail)).thenReturn(availableAgent);

            when(agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail)).thenReturn(processNodeListWithGroup);
            when(workerService.findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup)).thenReturn(availableAgent);
            when(availableAgent.isEmpty()).thenReturn(false);

            when(agentStrategy.getWorkers(taskDto, userDetail, agent)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentStrategy.getWorkers(taskDto, userDetail, agent));
            verify(processNodeListWithGroup, times(0)).get(0);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(workerService, times(1)).findAvailableAgent(userDetail);

            verify(agentGroupService, times(0)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(0)).findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup);
            verify(availableAgent, times(0)).isEmpty();
        }

        @Test
        void testNotAutomatic() {
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            Assertions.assertDoesNotThrow(() -> agentStrategy.getWorkers(taskDto, userDetail, agent));
            verify(processNodeListWithGroup, times(1)).get(0);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(workerService, times(0)).findAvailableAgent(userDetail);

            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup);
            verify(availableAgent, times(1)).isEmpty();
        }

        @Test
        void testAvailableAgentIsEmpty() {
            when(availableAgent.isEmpty()).thenReturn(true);
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            Assertions.assertDoesNotThrow(() -> agentStrategy.getWorkers(taskDto, userDetail, agent));
            verify(processNodeListWithGroup, times(0)).get(0);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(workerService, times(0)).findAvailableAgent(userDetail);

            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup);
            verify(availableAgent, times(1)).isEmpty();
        }

        @Test
        void testAvailableAgentIsNull() {
            when(workerService.findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup)).thenReturn(null);
            when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            Assertions.assertDoesNotThrow(() -> agentStrategy.getWorkers(taskDto, userDetail, agent));
            verify(processNodeListWithGroup, times(0)).get(0);
            verify(taskDto, times(1)).getAccessNodeType();
            verify(workerService, times(0)).findAvailableAgent(userDetail);

            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, processNodeListWithGroup);
            verify(availableAgent, times(0)).isEmpty();
        }
    }
}