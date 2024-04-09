package com.tapdata.tm.agent.controller;


import com.tapdata.tm.agent.dto.AgentGroupDto;
import com.tapdata.tm.agent.dto.AgentRemoveFromGroupDto;
import com.tapdata.tm.agent.dto.AgentToGroupDto;
import com.tapdata.tm.agent.dto.AgentWithGroupBaseDto;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.dto.GroupUsedDto;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgentGroupControllerTest {
    AgentGroupController agentGroupController;
    AgentGroupService agentGroupService;
    @BeforeEach
    void init() {
        agentGroupController = mock(AgentGroupController.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(agentGroupController, "agentGroupService", agentGroupService);

        when(agentGroupController.getLoginUser()).thenReturn(mock(UserDetail.class));
        when(agentGroupController.parseFilter(anyString())).thenReturn(mock(Filter.class));
    }

    @Nested
    class GroupAllAgentTest {
        @BeforeEach
        void init() {
            when(agentGroupService.groupAllAgent(any(Filter.class), anyBoolean(), any(UserDetail.class)))
                    .thenReturn(mock(Page.class));
            when(agentGroupController.groupAllAgent(anyString(), anyBoolean())).thenCallRealMethod();
            when(agentGroupController.success(any(Page.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.groupAllAgent("{}", true));
            verify(agentGroupController, times(1)).parseFilter(anyString());
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).groupAllAgent(any(Filter.class), anyBoolean(), any(UserDetail.class));
        }
    }

    @Nested
    class CreateAgentGroupTest {
        GroupDto dto;
        @BeforeEach
        void init() {
            dto = mock(GroupDto.class);
            when(agentGroupService.createGroup(any(GroupDto.class), any(UserDetail.class)))
                    .thenReturn(mock(AgentGroupDto.class));
            when(agentGroupController.createAgentGroup(dto)).thenCallRealMethod();
            when(agentGroupController.success(any(AgentGroupDto.class))).thenReturn(mock(ResponseMessage.class));

        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.createAgentGroup(dto));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).createGroup(any(GroupDto.class), any(UserDetail.class));
        }
    }

    @Nested
    class AddAgentToGroupTest {
        AgentWithGroupBaseDto dto;
        @BeforeEach
        void init() {
            dto = mock(AgentWithGroupBaseDto.class);
            when(agentGroupService.addAgentToGroup(any(AgentWithGroupBaseDto.class), any(UserDetail.class)))
                    .thenReturn(mock(AgentGroupDto.class));
            when(agentGroupController.addAgentToGroup(dto)).thenCallRealMethod();
            when(agentGroupController.success(any(AgentGroupDto.class))).thenReturn(mock(ResponseMessage.class));

        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.addAgentToGroup(dto));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).addAgentToGroup(any(AgentWithGroupBaseDto.class), any(UserDetail.class));
        }
    }

    @Nested
    class BatchTest {
        AgentToGroupDto dto;
        @BeforeEach
        void init() {
            dto = mock(AgentToGroupDto.class);
            when(agentGroupService.batchOperator(any(AgentToGroupDto.class), any(UserDetail.class)))
                    .thenReturn(mock(List.class));
            when(agentGroupController.batch(dto)).thenCallRealMethod();
            when(agentGroupController.success(any(List.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.batch(dto));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).batchOperator(any(AgentToGroupDto.class), any(UserDetail.class));
        }
    }

    @Nested
    class RemoveAgentFromGroupTest {
        AgentRemoveFromGroupDto dto;
        @BeforeEach
        void init() {
            dto = mock(AgentRemoveFromGroupDto.class);
            when(agentGroupService.removeAgentFromGroup(any(AgentRemoveFromGroupDto.class), any(UserDetail.class)))
                    .thenReturn(mock(AgentGroupDto.class));
            when(agentGroupController.removeAgentFromGroup(dto)).thenCallRealMethod();
            when(agentGroupController.success(any(AgentGroupDto.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.removeAgentFromGroup(dto));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).removeAgentFromGroup(any(AgentRemoveFromGroupDto.class), any(UserDetail.class));
        }
    }

    @Nested
    class DeleteGroupTest {
        @BeforeEach
        void init() {
            when(agentGroupService.deleteGroup(anyString(), any(UserDetail.class)))
                    .thenReturn(mock(GroupUsedDto.class));
            when(agentGroupController.deleteGroup("id")).thenCallRealMethod();
            when(agentGroupController.success(any(GroupUsedDto.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.deleteGroup("id"));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).deleteGroup(anyString(), any(UserDetail.class));
        }
    }

    @Nested
    class FindOneTest {
        @BeforeEach
        void init() {
            when(agentGroupService.findAgentGroupInfo(any(Filter.class), any(UserDetail.class)))
                    .thenReturn(mock(List.class));
            when(agentGroupController.findOne("{}")).thenCallRealMethod();
            when(agentGroupController.success(any(List.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.findOne("{}"));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).findAgentGroupInfo(any(Filter.class), any(UserDetail.class));
        }
    }

    @Nested
    class UpdateByWhereTest {
        GroupDto dto;
        @BeforeEach
        void init() {
            dto = mock(GroupDto.class);
            when(agentGroupService.updateBaseInfo(any(GroupDto.class), any(UserDetail.class)))
                    .thenReturn(mock(AgentGroupDto.class));
            when(agentGroupController.updateByWhere(dto)).thenCallRealMethod();
            when(agentGroupController.success(any(AgentGroupDto.class))).thenReturn(mock(ResponseMessage.class));
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupController.updateByWhere(dto));
            verify(agentGroupController, times(1)).getLoginUser();
            verify(agentGroupService, times(1)).updateBaseInfo(any(GroupDto.class), any(UserDetail.class));
        }
    }
}