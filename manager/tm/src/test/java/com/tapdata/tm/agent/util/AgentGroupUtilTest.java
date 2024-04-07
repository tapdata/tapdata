package com.tapdata.tm.agent.util;

import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgentGroupUtilTest {
    AgentGroupUtil agentGroupUtil;
    @BeforeEach
    void init() {
        agentGroupUtil = mock(AgentGroupUtil.class);
    }

    @Nested
    class InitFilterTest {
        @Test
        void testFilterIsNull() {
            when(agentGroupUtil.initFilter(null)).thenCallRealMethod();
            Assertions.assertNotNull(agentGroupUtil.initFilter(null));
        }
        @Test
        void testFilterNotNull() {
            Filter filter = mock(Filter.class);
            when(agentGroupUtil.initFilter(filter)).thenCallRealMethod();
            Assertions.assertNotNull(agentGroupUtil.initFilter(filter));
        }
    }

    @Nested
    class SortAgentGroupTest {
        AgentGroupEntity a;
        AgentGroupEntity b;
        @BeforeEach
        void init() {
            a = mock(AgentGroupEntity.class);
            b = mock(AgentGroupEntity.class);

            when(agentGroupUtil.sortAgentGroup(a, b)).thenCallRealMethod();
        }

        @Test
        void testAllListIsNull() {
            when(a.getAgentIds()).thenReturn(null);
            when(b.getAgentIds()).thenReturn(null);
            int count = agentGroupUtil.sortAgentGroup(a, b);
            Assertions.assertEquals(0, count);
        }

        @Test
        void testAListIsNull() {
            when(a.getAgentIds()).thenReturn(null);
            when(b.getAgentIds()).thenReturn(mock(List.class));
            int count = agentGroupUtil.sortAgentGroup(a, b);
            Assertions.assertEquals(1, count);
        }

        @Test
        void testBListIsNull() {
            when(b.getAgentIds()).thenReturn(null);
            when(a.getAgentIds()).thenReturn(mock(List.class));
            int count = agentGroupUtil.sortAgentGroup(a, b);
            Assertions.assertEquals(-1, count);
        }

        @Test
        void testAListSizeLessThanAbListSize() {
            List<String> mockA = mock(List.class);
            when(mockA.size()).thenReturn(1);
            List<String> mockB = mock(List.class);
            when(mockB.size()).thenReturn(2);
            when(b.getAgentIds()).thenReturn(mockB);
            when(a.getAgentIds()).thenReturn(mockA);
            int count = agentGroupUtil.sortAgentGroup(a, b);
            Assertions.assertEquals(1, count);
        }

        @Test
        void testAListSizeMoreThanBListSize() {
            List<String> mockA = mock(List.class);
            when(mockA.size()).thenReturn(2);
            List<String> mockB = mock(List.class);
            when(mockB.size()).thenReturn(1);
            when(b.getAgentIds()).thenReturn(mockB);
            when(a.getAgentIds()).thenReturn(mockA);
            int count = agentGroupUtil.sortAgentGroup(a, b);
            Assertions.assertEquals(-1, count);
        }
    }

    @Nested
    class MappingAccessNodeInfoTest {
        AgentGroupEntity group;
        Map<String, AccessNodeInfo> infoMap;
        java.util.List<String> agentIds;
        @BeforeEach
        void init() {
            agentIds = new ArrayList<>();
            agentIds.add("id");
            group = mock(AgentGroupEntity.class);
            infoMap = mock(Map.class);
            when(infoMap.get(anyString())).thenReturn(mock(AccessNodeInfo.class));

            when(group.getAgentIds()).thenReturn(agentIds);

            when(group.getGroupId()).thenReturn("id");
            when(group.getName()).thenReturn("name");

            when(agentGroupUtil.mappingAccessNodeInfo(group, infoMap)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            AccessNodeInfo accessNodeInfo = agentGroupUtil.mappingAccessNodeInfo(group, infoMap);
            Assertions.assertNotNull(accessNodeInfo);
            verify(group, times(1)).getAgentIds();
            verify(group, times(1)).getGroupId();
            verify(group, times(1)).getName();
            verify(infoMap, times(1)).get(anyString());
        }
        @Test
        void testAgentIdsListIsNull() {
            when(group.getAgentIds()).thenReturn(null);
            AccessNodeInfo accessNodeInfo = agentGroupUtil.mappingAccessNodeInfo(group, infoMap);
            Assertions.assertNotNull(accessNodeInfo);
            verify(group, times(1)).getAgentIds();
            verify(group, times(1)).getGroupId();
            verify(group, times(1)).getName();
            verify(infoMap, times(0)).get(anyString());
        }
    }

    @Nested
    class VerifyUpdateGroupInfoTest {
        GroupDto dto;
        @BeforeEach
        void init() {
            dto = mock(GroupDto.class);
            when(dto.getGroupId()).thenReturn("id");
            when(dto.getName()).thenReturn("name");

            doCallRealMethod().when(agentGroupUtil).verifyUpdateGroupInfo(dto);
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
        @Test
        void testDtoIsNull() {
            doCallRealMethod().when(agentGroupUtil).verifyUpdateGroupInfo(null);
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(null));
        }
        @Test
        void testGetGroupIdIsNull() {
            when(dto.getGroupId()).thenReturn(null);
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
        @Test
        void testGetGroupIdIsEmpty() {
            when(dto.getGroupId()).thenReturn("");
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
        @Test
        void testGetNameNull() {
            when(dto.getName()).thenReturn(null);
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
        @Test
        void testGetNameIsEmpty() {
            when(dto.getName()).thenReturn("");
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
        @Test
        void testGetNameTooLong() {
            StringBuffer name = new StringBuffer();
            for (int i = 0 ; i< 100 ; i++) {
                name.append("x");
            }
            when(dto.getName()).thenReturn(name.toString());
            Assertions.assertThrows(BizException.class, () -> agentGroupUtil.verifyUpdateGroupInfo(dto));
        }
    }
}