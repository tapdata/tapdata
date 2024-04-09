package com.tapdata.tm.task.service.utils;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskServiceUtilTest {
    AgentGroupService agentGroupService;

    UserDetail user;

    @BeforeEach
    void init() {
        agentGroupService = mock(AgentGroupService.class);
        user = mock(UserDetail.class);
    }

    @Nested
    class CopyAccessNodeInfoTest {
        TaskDto source;
        TaskDto target;
        @BeforeEach
        void init() {
            source = mock(TaskDto.class);
            target = mock(TaskDto.class);

            when(target.getAccessNodeType()).thenReturn("type");
            when(source.getAccessNodeType()).thenReturn("type");
            when(source.getAccessNodeProcessId()).thenReturn("id");

            doNothing().when(target).setAccessNodeType(anyString());
            doNothing().when(target).setAccessNodeProcessId(anyString());
            doNothing().when(target).setAccessNodeProcessIdList(anyList());

            when(agentGroupService.getProcessNodeListWithGroup(target, user)).thenReturn(mock(List.class));
        }

        @Test
        void testNormal() {
            try (MockedStatic<TaskServiceUtil> tsu = mockStatic(TaskServiceUtil.class)) {
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(source, target, user, agentGroupService)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> TaskServiceUtil.copyAccessNodeInfo(source, target, user, agentGroupService));
                verify(target, times(1)).getAccessNodeType();
                verify(source, times(0)).getAccessNodeType();
                verify(source, times(0)).getAccessNodeProcessId();
                verify(target, times(0)).setAccessNodeType(anyString());
                verify(target, times(0)).setAccessNodeProcessId(anyString());
                verify(target, times(0)).setAccessNodeProcessIdList(anyList());
                verify(agentGroupService, times(0)).getProcessNodeListWithGroup(target, user);
            }
        }
        @Test
        void testSourceIsNull() {
            try (MockedStatic<TaskServiceUtil> tsu = mockStatic(TaskServiceUtil.class)) {
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(null, target, user, agentGroupService)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> TaskServiceUtil.copyAccessNodeInfo(null, target, user, agentGroupService));
                verify(target, times(0)).getAccessNodeType();
                verify(source, times(0)).getAccessNodeType();
                verify(source, times(0)).getAccessNodeProcessId();
                verify(target, times(0)).setAccessNodeType(anyString());
                verify(target, times(0)).setAccessNodeProcessId(anyString());
                verify(target, times(0)).setAccessNodeProcessIdList(anyList());
                verify(agentGroupService, times(0)).getProcessNodeListWithGroup(target, user);
            }
        }
        @Test
        void testTargetIsNull() {
            try (MockedStatic<TaskServiceUtil> tsu = mockStatic(TaskServiceUtil.class)) {
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(source, null, user, agentGroupService)).thenCallRealMethod();
                Assertions.assertDoesNotThrow(() -> TaskServiceUtil.copyAccessNodeInfo(source, null, user, agentGroupService));
                verify(source, times(0)).getAccessNodeType();
                verify(target, times(0)).getAccessNodeType();
                verify(source, times(0)).getAccessNodeProcessId();
                verify(target, times(0)).setAccessNodeType(anyString());
                verify(target, times(0)).setAccessNodeProcessId(anyString());
                verify(target, times(0)).setAccessNodeProcessIdList(anyList());
                verify(agentGroupService, times(0)).getProcessNodeListWithGroup(target, user);
            }
        }
        @Test
        void testAccessNodeTypeIsEmpty() {
            try (MockedStatic<TaskServiceUtil> tsu = mockStatic(TaskServiceUtil.class)) {
                tsu.when(() -> TaskServiceUtil.copyAccessNodeInfo(source, target, user, agentGroupService)).thenCallRealMethod();
                when(target.getAccessNodeType()).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> TaskServiceUtil.copyAccessNodeInfo(source, target, user, agentGroupService));
                verify(target, times(1)).getAccessNodeType();
                verify(source, times(1)).getAccessNodeType();
                verify(source, times(1)).getAccessNodeProcessId();
                verify(target, times(1)).setAccessNodeType(anyString());
                verify(target, times(1)).setAccessNodeProcessId(anyString());
                verify(target, times(1)).setAccessNodeProcessIdList(anyList());
                verify(agentGroupService, times(1)).getProcessNodeListWithGroup(target, user);
            }

        }
    }
}