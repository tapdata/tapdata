package com.tapdata.tm.cluster.service;


import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.worker.dto.TcmInfo;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class ClusterStateServiceTest {
    ClusterStateService clusterStateService;

    WorkerService workerService;
    ClusterOperationService clusterOperationService;
    MessageService messageService;
    SettingsService settingsService;
    MongoTemplate mongoTemplate;
    AgentGroupService agentGroupService;

    UserDetail userDetail;
    @BeforeEach
    void init() {
        clusterStateService = mock(ClusterStateService.class);

        workerService = mock(WorkerService.class);
        clusterOperationService = mock(ClusterOperationService.class);
        messageService = mock(MessageService.class);
        settingsService = mock(SettingsService.class);
        mongoTemplate = mock(MongoTemplate.class);
        agentGroupService = mock(AgentGroupService.class);
        ClusterStateDto clusterStateDto = new ClusterStateDto();
        SystemInfo systemInfo = new SystemInfo("hostName", UUID.randomUUID().toString(), "ip", Arrays.asList("ips"), System.currentTimeMillis(), "accessCode",
                "userName", "id", 4, "Linux", 1L, "/workDir/log", "/workDir/", "/installDir");
        clusterStateDto.setAgentName("agentName");
        clusterStateDto.setSystemInfo(systemInfo);
        List<ClusterStateDto> clusterStateDtos = new ArrayList<>();
        clusterStateDtos.add(clusterStateDto);
        when(clusterStateService.findAll(Query.query(Criteria.where("systemInfo.process_id").in("id")))).thenReturn(clusterStateDtos);
        ReflectionTestUtils.setField(clusterStateService, "workerService",workerService);
        ReflectionTestUtils.setField(clusterStateService, "clusterOperationService",clusterOperationService);
        ReflectionTestUtils.setField(clusterStateService, "messageService",messageService);
        ReflectionTestUtils.setField(clusterStateService, "settingsService",settingsService);
        ReflectionTestUtils.setField(clusterStateService, "mongoTemplate",mongoTemplate);
        ReflectionTestUtils.setField(clusterStateService, "agentGroupService",agentGroupService);

        userDetail = mock(UserDetail.class);

    }

    @Nested
    class FindAccessNodeInfoTest {
        List<Worker> workerList;
        Worker worker;
        TcmInfo tcmInfo;
        @BeforeEach
        void init() {
            worker = mock(Worker.class);
            workerList = new ArrayList<>();
            workerList.add(worker);
            tcmInfo = mock(TcmInfo.class);
            when(tcmInfo.getAgentName()).thenReturn("agent");

            when(settingsService.isCloud()).thenReturn(false);
            when(workerService.findAllAgent(userDetail)).thenReturn(workerList);
            when(worker.getHostname()).thenReturn("name");
            when(worker.getTcmInfo()).thenReturn(tcmInfo);
            when(worker.getStopping()).thenReturn(true);
            when(worker.getPingTime()).thenReturn(1000L);
            when(worker.getProcessId()).thenReturn("id");
            when(agentGroupService.filterGroupList(anyList(), any(UserDetail.class))).thenReturn(mock(List.class));

            when(clusterStateService.findAccessNodeInfo(userDetail)).thenCallRealMethod();
        }

        void assertVerify(int getHostnameTimes, int getTcmInfoTimes,
                          int getAgentNameTimes,
                          int getStoppingTimes, int getPingTimeTimes, int getProcessIdTimes,
                          int filterGroupListTimes) {
            try (MockedStatic<SettingUtil> su = mockStatic(SettingUtil.class)) {
                su.when(() -> SettingUtil.getValue(CategoryEnum.WORKER.getValue(), KeyEnum.WORKER_HEART_TIMEOUT.getValue())).thenReturn("30");
                Assertions.assertDoesNotThrow(() -> clusterStateService.findAccessNodeInfo(userDetail));
            }
           verify(settingsService, times(1)).isCloud();
           verify(workerService, times(1)).findAllAgent(userDetail);
           verify(worker, times(getHostnameTimes)).getHostname();
           verify(worker, times(getTcmInfoTimes)).getTcmInfo();
           verify(tcmInfo, times(getAgentNameTimes)).getAgentName();
           verify(worker, times(getStoppingTimes)).getStopping();
           verify(worker, times(getPingTimeTimes)).getPingTime();
           verify(worker, times(getProcessIdTimes)).getProcessId();
           verify(agentGroupService, times(filterGroupListTimes)).filterGroupList(anyList(), any(UserDetail.class));
        }

        @Test
        void testEmptyWorkers() {
            workerList.remove(0);
            assertVerify(0, 0, 0, 0, 0, 0, 0);
        }

        @Test
        void testIsCloudButNullTcmInfo() {
            when(settingsService.isCloud()).thenReturn(true);
            when(worker.getTcmInfo()).thenReturn(null);
            assertVerify(1, 1,
                    0,
                    2, 0, 5,
                    1);
        }
        @Test
        void testIsCloudAntTcmInfoNotNull() {
            when(settingsService.isCloud()).thenReturn(true);
            assertVerify(1, 1,
                    1,
                    2, 0, 5,
                    1);
        }

        @Test
        void testStoppingIsNull() {
            when(worker.getStopping()).thenReturn(null);
            assertVerify(1, 0,
                    0,
                    1, 2, 5,
                    1);
        }
        @Test
        void testStoppingIsFalse() {
            when(worker.getStopping()).thenReturn(false);
            assertVerify(1, 0,
                    0,
                    2, 2, 5,
                    1);
        }

        @Test
        void testPingTimeIsNull() {
            when(worker.getStopping()).thenReturn(null);
            when(worker.getPingTime()).thenReturn(null);
            assertVerify(1, 0,
                    0,
                    1, 1, 5,
                    1);
        }
        @Test
        void testPingTimeLessThan30Sec() {
            when(worker.getStopping()).thenReturn(null);
            when(worker.getPingTime()).thenReturn(2L);
            assertVerify(1, 0,
                    0,
                    1, 2, 5,
                    1);
        }
        @Test
        void testPingTimeMoreThan30Sec() {
            when(worker.getStopping()).thenReturn(null);
            when(worker.getPingTime()).thenReturn(Long.MAX_VALUE);
            assertVerify(1, 0,
                    0,
                    1, 2, 5,
                    1);
        }
    }
    @Nested
    class deleteClusterTest{
        @Test
        public void testDeleteCluster() {
            Field field = new Field();
            field.put("systemInfo", 1);
            WorkerDto worker = new WorkerDto();
            worker.setLicenseBind(true);
            when(workerService.findByProcessId(anyString(), any(UserDetail.class), anyString())).thenReturn(worker);
            ObjectId id = new ObjectId();
            UserDetail user = mock(UserDetail.class);
            when(clusterStateService.deleteById(id)).thenReturn(true);
            doCallRealMethod().when(clusterStateService).deleteCluster(id, user);
            boolean result = clusterStateService.deleteCluster(id, user);
            assertTrue(result);

            when(clusterStateService.findById(id, field)).thenReturn(null);
            result = clusterStateService.deleteCluster(id, user);
            assertTrue(result);

            ClusterStateDto clusterStateDto = new ClusterStateDto();
            SystemInfo systemInfo = mock(SystemInfo.class);
            clusterStateDto.setSystemInfo(systemInfo);
            when(systemInfo.getProcess_id()).thenReturn("123");
            when(clusterStateService.findById(id, field)).thenReturn(clusterStateDto);
            result = clusterStateService.deleteCluster(id, user);
            assertFalse(result);

            when(workerService.unbindByProcessId("123")).thenReturn(true);
            result = clusterStateService.deleteCluster(id, user);
            assertTrue(result);
        }

        @Test
        public void testDeleteClusterLicenseBindIsNull() {
            Field field = new Field();
            field.put("systemInfo", 1);
            WorkerDto worker = new WorkerDto();
            when(workerService.findByProcessId(anyString(), any(UserDetail.class), anyString())).thenReturn(worker);
            ObjectId id = new ObjectId();
            UserDetail user = mock(UserDetail.class);
            when(clusterStateService.deleteById(id)).thenReturn(true);
            doCallRealMethod().when(clusterStateService).deleteCluster(id, user);
            ClusterStateDto clusterStateDto = new ClusterStateDto();
            SystemInfo systemInfo = mock(SystemInfo.class);
            clusterStateDto.setSystemInfo(systemInfo);
            when(systemInfo.getProcess_id()).thenReturn("123");
            when(clusterStateService.findById(id, field)).thenReturn(clusterStateDto);
            boolean result = clusterStateService.deleteCluster(id, user);
            assertTrue(result);
            worker.setLicenseBind(true);
            result = clusterStateService.deleteCluster(id, user);
            assertFalse(result);
            worker.setLicenseBind(false);
            result = clusterStateService.deleteCluster(id, user);
            assertTrue(result);

        }
    }
}