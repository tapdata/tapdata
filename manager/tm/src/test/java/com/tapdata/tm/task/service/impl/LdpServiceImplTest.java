package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LdpServiceImplTest {

    LdpServiceImpl ldpService;
    AgentGroupService agentGroupService;
    WorkerService workerService;

    UserDetail user;
    MetadataInstancesService metadataInstancesService;

    @BeforeEach
    void init() {
        ldpService = mock(LdpServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(ldpService, "agentGroupService", agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(ldpService, "workerService", workerService);
        metadataInstancesService = mock(MetadataInstancesServiceImpl.class);
        ReflectionTestUtils.setField(ldpService, "metadataInstancesService", metadataInstancesService);
        user = mock(UserDetail.class);
    }

    @Nested
    class FindAgentTest {
        DataSourceConnectionDto connectionDto;
        List<String> processNodeListWithGroup;
        List<Worker> availableAgent;
        Worker worker;

        @BeforeEach
        void init() {
            availableAgent = new ArrayList<>();
            processNodeListWithGroup = new ArrayList<>();
            processNodeListWithGroup.add("id");

            worker = mock(Worker.class);
            when(worker.getProcessId()).thenReturn("id");
            availableAgent.add(worker);

            connectionDto = mock(DataSourceConnectionDto.class);
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());

            when(agentGroupService.getProcessNodeListWithGroup(connectionDto, user)).thenReturn(processNodeListWithGroup);
            when(workerService.findAvailableAgent(user)).thenReturn(availableAgent);

            when(ldpService.findAgent(connectionDto, user)).thenCallRealMethod();
        }
        String assertVerify(int getProcessIdTimes, int getProcessNodeListWithGroupTimes, int getAccessNodeTypeTimes) {
            String agent = ldpService.findAgent(connectionDto, user);
            verify(agentGroupService, times(getProcessNodeListWithGroupTimes)).getProcessNodeListWithGroup(connectionDto, user);
            verify(connectionDto, times(getAccessNodeTypeTimes)).getAccessNodeType();
            verify(workerService, times(1)).findAvailableAgent(user);
            verify(worker, times(getProcessIdTimes)).getProcessId();
            return agent;
        }

        @Test
        void testNormal() {
            String nodeId = assertVerify(1, 1, 1);
            Assertions.assertEquals("id", nodeId);
        }
        @Test
        void testAvailableAgentIsNull() {
            when(workerService.findAvailableAgent(user)).thenReturn(null);
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull( nodeId);
        }
        @Test
        void testAvailableAgentIsEmpty() {
            when(workerService.findAvailableAgent(user)).thenReturn(new ArrayList<>());
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull( nodeId);
        }
        @Test
        void testProcessIdsEmpty() {
            Worker w = null;
            ArrayList<Worker> ws = new ArrayList<>();
            ws.add(w);
            when(workerService.findAvailableAgent(user)).thenReturn(ws);
            String nodeId = assertVerify(0, 0, 0);
            Assertions.assertNull(nodeId);
        }
        @Test
        void testIsNotManually() {
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            String nodeId = assertVerify(1, 1, 1);
            Assertions.assertEquals("id", nodeId);
        }
        @Test
        void testProcessNodeListWithGroupIsEmpty() {
            when(agentGroupService.getProcessNodeListWithGroup(connectionDto, user)).thenReturn(new ArrayList<>());
            String nodeId = assertVerify(1, 1, 1);
            Assertions.assertEquals("id", nodeId);
        }
        @Test
        void testProcessNodeListWithGroupNotContainsId() {
            when(worker.getProcessId()).thenReturn("ids");
            String nodeId = assertVerify(1, 1, 1);
            Assertions.assertNull(nodeId);
        }
    }
    @Nested
    class CleanLdpMeta{
        @Test
        void test_main(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            metadataInstancesDto1.setAncestorsName("test");
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(1)).deleteAll(any(),any());
        }

        @Test
        void test_tagIdIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setAncestorsName("test");
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

        @Test
        void test_SourceIdIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            metadataInstancesDto1.setAncestorsName("test");
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

        @Test
        void test_AncestorsNameIsNull(){
            List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setListtags(Arrays.asList(new Tag("id","test")));
            SourceDto sourceDto = new SourceDto();
            sourceDto.setId(new ObjectId());
            metadataInstancesDto1.setSource(sourceDto);
            metadataInstancesDtos.add(metadataInstancesDto1);
            doCallRealMethod().when(ldpService).cleanLdpMeta(any(),any());
            ldpService.cleanLdpMeta(metadataInstancesDtos,mock(UserDetail.class));
            verify(metadataInstancesService,times(0)).deleteAll(any(),any());
        }

    }
}