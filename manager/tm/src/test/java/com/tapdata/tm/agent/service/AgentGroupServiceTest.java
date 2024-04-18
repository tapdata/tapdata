package com.tapdata.tm.agent.service;


import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.Unit4Util;
import com.tapdata.tm.agent.dto.AgentGroupDto;
import com.tapdata.tm.agent.dto.AgentRemoveFromGroupDto;
import com.tapdata.tm.agent.dto.AgentToGroupDto;
import com.tapdata.tm.agent.dto.AgentWithGroupBaseDto;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.agent.repository.AgentGroupRepository;
import com.tapdata.tm.agent.util.AgentGroupTag;
import com.tapdata.tm.agent.util.AgentGroupUtil;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AgentGroupServiceTest {
    AgentGroupService agentGroupService;
    WorkerServiceImpl workerServiceImpl;
    DataSourceService dataSourceService;
    TaskServiceImpl taskService;
    SettingsServiceImpl settingsService;
    AgentGroupUtil agentGroupUtil;
    Logger log;
    AgentGroupRepository repository;
    UserDetail userDetail;

    @BeforeEach
    void init() {
        log = mock(Logger.class);
        agentGroupService = mock(AgentGroupService.class);
        Unit4Util.mockSlf4jLog(agentGroupService, log);
        workerServiceImpl = mock(WorkerServiceImpl.class);
        ReflectionTestUtils.setField(agentGroupService, "workerServiceImpl", workerServiceImpl);

        dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(agentGroupService, "dataSourceService", dataSourceService);

        taskService = mock(TaskServiceImpl.class);
        ReflectionTestUtils.setField(agentGroupService, "taskService", taskService);

        settingsService = mock(SettingsServiceImpl.class);
        ReflectionTestUtils.setField(agentGroupService, "settingsService", settingsService);

        agentGroupUtil = mock(AgentGroupUtil.class);
        ReflectionTestUtils.setField(agentGroupService, "agentGroupUtil", agentGroupUtil);

        repository = mock(AgentGroupRepository.class);
        ReflectionTestUtils.setField(agentGroupService, "repository", repository);

        userDetail = mock(UserDetail.class);
    }

    @Test
    void testConstructor() {
        Assertions.assertDoesNotThrow(() -> new AgentGroupService(repository));
        Assertions.assertThrows(Exception.class, () -> new AgentGroupService(null));
    }

    @Nested
    class BeforeSaveTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(agentGroupService).beforeSave(any(GroupDto.class), any(UserDetail.class));
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.beforeSave(mock(GroupDto.class), userDetail));
        }
    }

    @Nested
    class GroupAllAgentTest {
        Filter filter;
        Boolean containWorker;

        Page<GroupDto> groupDtoPage;
        List<GroupDto> items;

        List<WorkerDto> all;
        WorkerDto workerDto;

        @BeforeEach
        void init() {
            filter = mock(Filter.class);
            containWorker = true;

            groupDtoPage = mock(Page.class);

            when(agentGroupUtil.initFilter(filter)).thenReturn(filter);

            when(agentGroupService.find(filter, userDetail)).thenReturn(groupDtoPage);
            items = new ArrayList<>();
            items.add(mock(GroupDto.class));
            when(groupDtoPage.getItems()).thenReturn(items);
            when(groupDtoPage.getTotal()).thenReturn(0L);

            all = new ArrayList<>();
            workerDto = mock(WorkerDto.class);
            all.add(workerDto);
            all.add(null);
            when(agentGroupService.getAllAgentId(anyList(), anyBoolean(), any(UserDetail.class))).thenReturn(all);
            when(workerDto.getProcessId()).thenReturn("id");
            when(agentGroupService.generateDto(any(GroupDto.class), anyBoolean(), anyMap())).thenReturn(mock(AgentGroupDto.class));

            when(agentGroupService.groupAllAgent(filter, containWorker, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.groupAllAgent(filter, containWorker, userDetail));
            verify(agentGroupUtil, times(1)).initFilter(filter);
            verify(agentGroupService, times(1)).find(filter, userDetail);
            verify(groupDtoPage, times(1)).getItems();
            verify(groupDtoPage, times(1)).getTotal();
            verify(agentGroupService, times(1)).getAllAgentId(anyList(), anyBoolean(), any(UserDetail.class));
            verify(workerDto, times(2)).getProcessId();
        }

        @Test
        void testItemsIsEmpty() {
            items.remove(0);
            Assertions.assertDoesNotThrow(() -> agentGroupService.groupAllAgent(filter, containWorker, userDetail));
            verify(agentGroupUtil, times(1)).initFilter(filter);
            verify(agentGroupService, times(1)).find(filter, userDetail);
            verify(groupDtoPage, times(1)).getItems();
            verify(groupDtoPage, times(1)).getTotal();
            verify(agentGroupService, times(0)).getAllAgentId(anyList(), anyBoolean(), any(UserDetail.class));
            verify(workerDto, times(0)).getProcessId();
        }
        @Test
        void testEqualsIsFalse() {
            containWorker = false;
            when(agentGroupService.groupAllAgent(filter, containWorker, userDetail)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> agentGroupService.groupAllAgent(filter, containWorker, userDetail));
            verify(agentGroupUtil, times(1)).initFilter(filter);
            verify(agentGroupService, times(1)).find(filter, userDetail);
            verify(groupDtoPage, times(1)).getItems();
            verify(groupDtoPage, times(1)).getTotal();
            verify(agentGroupService, times(1)).getAllAgentId(anyList(), anyBoolean(), any(UserDetail.class));
            verify(workerDto, times(0)).getProcessId();
        }
        @Test
        void testProcessIdIsNull() {
            when(workerDto.getProcessId()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> agentGroupService.groupAllAgent(filter, containWorker, userDetail));
            verify(agentGroupUtil, times(1)).initFilter(filter);
            verify(agentGroupService, times(1)).find(filter, userDetail);
            verify(groupDtoPage, times(1)).getItems();
            verify(groupDtoPage, times(1)).getTotal();
            verify(agentGroupService, times(1)).getAllAgentId(anyList(), anyBoolean(), any(UserDetail.class));
            verify(workerDto, times(1)).getProcessId();
        }
    }

    @Nested
    class GetAllAgentIdTest {
        List<GroupDto> items;
        GroupDto a;
        List<WorkerDto> all;
        @BeforeEach
        void init() {
            items = new ArrayList<>();
            a = mock(GroupDto.class);
            items.add(a);
            items.add(null);

            when(a.getAgentIds()).thenReturn(new ArrayList<>());

            all = mock(List.class);
            when(agentGroupService.findAllAgent(anyList(), any(UserDetail.class))).thenReturn(all);

            when(agentGroupService.getAllAgentId(items, false, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNotEquals() {
            List<WorkerDto> allAgentId = agentGroupService.getAllAgentId(items, false, userDetail);
            Assertions.assertNotNull(allAgentId);
            Assertions.assertEquals(0, allAgentId.size());
            verify(a, times(1)).getAgentIds();
            verify(agentGroupService, times(0)).findAllAgent(anyList(), any(UserDetail.class));
        }

        @Test
        void testItemsHasNull() {
            when(a.getAgentIds()).thenReturn(Lists.newArrayList("id"));
            List<WorkerDto> allAgentId = agentGroupService.getAllAgentId(items, false, userDetail);
            Assertions.assertNotNull(allAgentId);
            Assertions.assertEquals(0, allAgentId.size());
            verify(a, times(2)).getAgentIds();
            verify(agentGroupService, times(0)).findAllAgent(anyList(), any(UserDetail.class));
        }
        @Test
        void testAllAgentIdIsEmpty() {

            List<WorkerDto> allAgentId = agentGroupService.getAllAgentId(items, false, userDetail);
            Assertions.assertNotNull(allAgentId);
            Assertions.assertEquals(0, allAgentId.size());
            verify(a, times(1)).getAgentIds();
            verify(agentGroupService, times(0)).findAllAgent(anyList(), any(UserDetail.class));
        }

        @Test
        void testNotFindAllAgent() {
            when(agentGroupService.getAllAgentId(items, true, userDetail)).thenCallRealMethod();
            when(agentGroupService.findAllAgent(anyList(), any(UserDetail.class))).thenReturn(null);
            List<WorkerDto> allAgentId = agentGroupService.getAllAgentId(items, true, userDetail);
            Assertions.assertNotNull(allAgentId);
            Assertions.assertEquals(0, allAgentId.size());
            verify(a, times(1)).getAgentIds();
            verify(agentGroupService, times(0)).findAllAgent(anyList(), any(UserDetail.class));
        }
    }

    @Nested
    class GenerateDtoTest {
        GroupDto item;
        Map<String, WorkerDto> map;
        List<String> agentIds;

        @BeforeEach
        void init() {
            agentIds = new ArrayList<>();
            agentIds.add("id");

            item = mock(GroupDto.class);
            when(item.getAgentIds()).thenReturn(agentIds);
            when(item.getGroupId()).thenReturn("id");
            when(item.getName()).thenReturn("name");

            map = mock(Map.class);
            when(map.get("id")).thenReturn(mock(WorkerDto.class));
            when(agentGroupService.generateDto(item, true, map)).thenCallRealMethod();
            when(agentGroupService.generateDto(item, false, map)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertNotNull(agentGroupService.generateDto(item, true, map));
            verify(item, times(1)).getAgentIds();
            verify(item, times(1)).getGroupId();
            verify(item, times(1)).getName();
        }

        @Test
        void testAgentIdsIsEmpty() {
            Assertions.assertNotNull(agentGroupService.generateDto(item, true, map));
            verify(item, times(1)).getAgentIds();
            verify(item, times(1)).getGroupId();
            verify(item, times(1)).getName();
        }

        @Test
        void testAgentIdsIsNull() {
            when(item.getAgentIds()).thenReturn(null);
            Assertions.assertNotNull(agentGroupService.generateDto(item, true, map));
            verify(item, times(1)).getAgentIds();
            verify(item, times(1)).getGroupId();
            verify(item, times(1)).getName();
        }

        @Test
        void testNotEquals(){
            Assertions.assertNotNull(agentGroupService.generateDto(item, false, map));
            verify(item, times(1)).getAgentIds();
            verify(item, times(1)).getGroupId();
            verify(item, times(1)).getName();
        }
    }

    @Nested
    class CreateGroupTest {
        @Test
        void testNormal() {
            GroupDto groupDto = mock(GroupDto.class);
            when(groupDto.getName()).thenReturn("name");
            Query query = mock(Query.class);
            when(agentGroupService.verifyCountGroupByName("name", userDetail)).thenReturn(query);
            when(agentGroupService.upsert(any(Query.class), any(AgentGroupDto.class), any(UserDetail.class))).thenReturn(1L);
            doNothing().when(log).info("A agent group has be created - {}", "name");

            when(agentGroupService.createGroup(groupDto, userDetail)).thenCallRealMethod();

            Assertions.assertNotNull(agentGroupService.createGroup(groupDto, userDetail));
            verify(agentGroupService, times(1)).verifyCountGroupByName("name", userDetail);
            verify(agentGroupService, times(1)).upsert(any(Query.class), any(AgentGroupDto.class), any(UserDetail.class));
            verify(log, times(1)).info("A agent group has be created - {}", "name");
        }
    }

    @Nested
    class VerifyCountGroupByNameTest {
        @Test
        void testNormal() {
            when(agentGroupService.count(any(Query.class), any(UserDetail.class))).thenReturn(0L);
            when(agentGroupService.verifyCountGroupByName(anyString(), any(UserDetail.class))).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> agentGroupService.verifyCountGroupByName("name", userDetail));
            verify(agentGroupService, times(1)).count(any(Query.class), any(UserDetail.class));
        }

        @Test
        void testCountLessThanZero() {
            when(agentGroupService.count(any(Query.class), any(UserDetail.class))).thenReturn(1L);
            when(agentGroupService.verifyCountGroupByName(anyString(), any(UserDetail.class))).thenCallRealMethod();
            Assertions.assertThrows(BizException.class, () -> agentGroupService.verifyCountGroupByName("name", userDetail));
            verify(agentGroupService, times(1)).count(any(Query.class), any(UserDetail.class));
        }
    }

    @Nested
    class BatchOperatorTest {
        AgentToGroupDto agentDto;
        List<String> agentId;
        List<String> groupId;
        @BeforeEach
        void init() {
            agentDto = mock(AgentToGroupDto.class);
            agentId = mock(List.class);
            groupId = mock(List.class);
            when(agentDto.getAgentId()).thenReturn(agentId);
            when(agentDto.getGroupId()).thenReturn(groupId);
            when(agentGroupService.batchRemoveAll(userDetail)).thenReturn(mock(List.class));
            when(agentGroupService.batchRemoveAllGroup(groupId, userDetail)).thenReturn(mock(List.class));
            when(agentGroupService.batchRemoveAllAgent(agentId, userDetail)).thenReturn(mock(List.class));
            when(agentGroupService.batchUpdate(agentId, groupId, userDetail)).thenReturn(mock(List.class));
            when(agentGroupService.batchOperator(agentDto, userDetail)).thenCallRealMethod();
        }

        void assertVerify(boolean agentIdIsEmpty, boolean groupIdIsEmpty,
                          int agentIdIsEmptyTimes,
                          int batchRemoveAll, int batchRemoveAllGroup, int batchRemoveAllAgent, int batchUpdate) {
            try(MockedStatic<CollectionUtils> cu = mockStatic(CollectionUtils.class)) {
                cu.when(()-> CollectionUtils.isEmpty(agentId)).thenReturn(agentIdIsEmpty);
                cu.when(()-> CollectionUtils.isEmpty(groupId)).thenReturn(groupIdIsEmpty);
                Assertions.assertDoesNotThrow(() -> agentGroupService.batchOperator(agentDto, userDetail));
                cu.verify(()-> CollectionUtils.isEmpty(agentId), times(agentIdIsEmptyTimes));
                cu.verify(()-> CollectionUtils.isEmpty(groupId), times(1));
            }
            verify(agentDto, times(1)).getAgentId();
            verify(agentDto, times(1)).getGroupId();
            verify(agentGroupService, times(batchRemoveAll)).batchRemoveAll(userDetail);
            verify(agentGroupService, times(batchRemoveAllGroup)).batchRemoveAllGroup(groupId, userDetail);
            verify(agentGroupService, times(batchRemoveAllAgent)).batchRemoveAllAgent(agentId, userDetail);
            verify(agentGroupService, times(batchUpdate)).batchUpdate(agentId, groupId, userDetail);
        }

        @Test
        void testNormal() {
            assertVerify(false, false,
                    2,
                    0, 0, 0, 1);
        }

        @Test
        void testBatchRemoveAll() {
            assertVerify(true, true,
                    1,
                    1, 0, 0, 0);
        }

        @Test
        void testBatchRemoveAllGroup() {
            assertVerify(true, false,
                    2,
                    0, 1, 0, 0);
        }

        @Test
        void testBatchRemoveAllAgent() {
            assertVerify(false, true,
                    2,
                    0, 0, 1, 0);
        }
    }

    @Nested
    class BatchRemoveAllTest {
        @BeforeEach
        void init() {
            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(mock(UpdateResult.class));
            when(agentGroupService.findAgentGroupInfo(any(Query.class), any(UserDetail.class))).thenReturn(mock(List.class));
            when(agentGroupService.batchRemoveAll(userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAll(userDetail));
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(agentGroupService, times(1)).findAgentGroupInfo(any(Query.class), any(UserDetail.class));
        }
    }

    @Nested
    class BatchRemoveAllAgentTest {
        List<String> agentIds;
        List<AgentGroupEntity> all;
        AgentGroupEntity agentGroupEntity;

        Criteria criteria;
        UpdateResult updateResult;
        @BeforeEach
        void init() {
            agentIds = mock(List.class);
            when(agentIds.isEmpty()).thenReturn(false);
            when(agentIds.toArray()).thenReturn(new String[]{});

            all = new ArrayList<>();
            agentGroupEntity = mock(AgentGroupEntity.class);
            when(agentGroupEntity.getGroupId()).thenReturn("id");
            all.add(agentGroupEntity);

            when(agentGroupService.findAll(any(Query.class), any(UserDetail.class))).thenReturn(all);

            criteria = mock(Criteria.class);
            when(criteria.and(AgentGroupTag.TAG_AGENT_IDS)).thenReturn(criteria);
            when(criteria.in(anyList())).thenReturn(criteria);
            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);

            updateResult = mock(UpdateResult.class);
            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(updateResult);
            doNothing().when(log).info(anyString(), anyList(), anyList());
            when(agentGroupService.findAgentGroupInfoMany(any(List.class), any(UserDetail.class))).thenReturn(mock(List.class));

            when(agentGroupService.batchRemoveAllAgent(agentIds, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAllAgent(agentIds, userDetail));
            verify(agentIds, times(1)).isEmpty();

            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(agentGroupEntity, times(2)).getGroupId();
            verify(agentGroupService, times(1)).findCriteria(anyList());

            verify(criteria, times(1)).and(AgentGroupTag.TAG_AGENT_IDS);
            verify(criteria, times(1)).in(anyList());

            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(1)).info(anyString(), anyList(), anyList());
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(any(List.class), any(UserDetail.class));
        }

        @Test
        void testAgentIdsIsEMpty() {
            when(agentIds.isEmpty()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAllAgent(agentIds, userDetail));
            verify(agentIds, times(1)).isEmpty();

            verify(agentGroupService, times(0)).findAll(any(Query.class), any(UserDetail.class));
            verify(agentGroupEntity, times(0)).getGroupId();
            verify(agentGroupService, times(0)).findCriteria(anyList());

            verify(criteria, times(0)).and(AgentGroupTag.TAG_AGENT_IDS);
            verify(criteria, times(0)).in(anyList());

            verify(agentGroupService, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(0)).info(anyString(), anyList(), anyList());
            verify(agentGroupService, times(0)).findAgentGroupInfoMany(any(List.class), any(UserDetail.class));
        }


        @Test
        void testFindAllContainsNull() {
            all.add(null);
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAllAgent(agentIds, userDetail));
            verify(agentIds, times(1)).isEmpty();

            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(agentGroupEntity, times(2)).getGroupId();
            verify(agentGroupService, times(1)).findCriteria(anyList());

            verify(criteria, times(1)).and(AgentGroupTag.TAG_AGENT_IDS);
            verify(criteria, times(1)).in(anyList());

            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(1)).info(anyString(), anyList(), anyList());
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(any(List.class), any(UserDetail.class));
        }
        @Test
        void testGetGroupIdIsNull() {
            when(agentGroupEntity.getGroupId()).thenReturn(null);

            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAllAgent(agentIds, userDetail));
            verify(agentIds, times(1)).isEmpty();

            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(agentGroupEntity, times(1)).getGroupId();
            verify(agentGroupService, times(1)).findCriteria(anyList());

            verify(criteria, times(1)).and(AgentGroupTag.TAG_AGENT_IDS);
            verify(criteria, times(1)).in(anyList());

            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(1)).info(anyString(), anyList(), anyList());
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(any(List.class), any(UserDetail.class));
        }
    }

    @Nested
    class BatchRemoveAllGroupTest {
        List<String> groupIds;
        @BeforeEach
        void init() {
            groupIds = mock(List.class);

            when(agentGroupService.updateAgent(anyList(), anyList(), any(UserDetail.class))).thenReturn(mock(List.class));
            doNothing().when(log).info("Agent batch operator: all agent are removed from groups: {} ", groupIds);
            when(agentGroupService.batchRemoveAllGroup(groupIds, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchRemoveAllGroup(groupIds, userDetail));
            verify(log, times(1)).info(anyString(), anyList());
            verify(agentGroupService, times(1)).updateAgent(anyList(), anyList(), any(UserDetail.class));
        }
    }

    @Nested
    class UpdateAgentTest {
        List<String> groupIds;
        List<String> agentId;
        Criteria criteria;

        List<AgentGroupEntity> all;
        AgentGroupEntity entity;
        @BeforeEach
        void init() {
            agentId = mock(List.class);
            groupIds = mock(List.class);
            when(groupIds.isEmpty()).thenReturn(false);

            all = new ArrayList<>();
            entity = mock(AgentGroupEntity.class);
            when(entity.getGroupId()).thenReturn("id");
            all.add(entity);
            when(groupIds.contains("id")).thenReturn(true);
            when(groupIds.addAll(anyList())).thenReturn(true);
            when(agentGroupService.findAll(any(Query.class), any(UserDetail.class))).thenReturn(all);


            criteria = mock(Criteria.class);
            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);
            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(mock(UpdateResult.class));
            when(agentGroupService.findAgentGroupInfoMany(anyList(), any(UserDetail.class))).thenReturn(mock(List.class));
            when(agentGroupService.updateAgent(groupIds, agentId, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.updateAgent(groupIds, agentId, userDetail));
            verify(groupIds, times(0)).addAll(anyList());
            verify(groupIds, times(1)).contains("id");
            verify(groupIds, times(1)).isEmpty();
            verify(agentGroupService, times(1)).findCriteria(groupIds);
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(groupIds, userDetail);
        }

        @Test
        void testGroupIdsIsEmpty() {
            when(groupIds.isEmpty()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> agentGroupService.updateAgent(groupIds, agentId, userDetail));
            verify(groupIds, times(0)).addAll(anyList());
            verify(groupIds, times(0)).contains("id");
            verify(groupIds, times(1)).isEmpty();
            verify(agentGroupService, times(0)).findCriteria(groupIds);
            verify(agentGroupService, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(agentGroupService, times(0)).findAgentGroupInfoMany(groupIds, userDetail);
        }

        @Test
        void testNeedClean() {
            when(groupIds.contains("id")).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> agentGroupService.updateAgent(groupIds, agentId, userDetail));
            verify(groupIds, times(1)).addAll(anyList());
            verify(groupIds, times(1)).isEmpty();
            verify(agentGroupService, times(1)).findCriteria(groupIds);
            verify(agentGroupService, times(2)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(groupIds, userDetail);
            verify(groupIds, times(1)).contains("id");
        }
    }

    @Nested
    class BatchUpdateTest {
        List<String> groupIds;
        List<String> agentIds;
        @BeforeEach
        void init() {
            agentIds = mock(List.class);
            groupIds = mock(List.class);

            when(agentGroupService.updateAgent(groupIds, agentIds, userDetail)).thenReturn(mock(List.class));
            doNothing().when(log).info("Agent: {} has be added to group: {} ", agentIds, groupIds);;
            when(agentGroupService.batchUpdate(agentIds, groupIds, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.batchUpdate(agentIds, groupIds, userDetail));
            verify(log, times(1)).info("Agent: {} has be added to group: {} ", agentIds, groupIds);
            verify(agentGroupService, times(1)).updateAgent(anyList(), anyList(), any(UserDetail.class));
        }
    }

    @Nested
    class AddAgentToGroupTest {
        AgentWithGroupBaseDto agentDto;
        GroupDto groupDto;
        List<String> agents;
        Criteria criteria;
        UpdateResult updateResult;
        @BeforeEach
        void init() {
            groupDto = mock(GroupDto.class);
            agents = mock(List.class);
            criteria = Criteria.where("hhh").ne(true);
            updateResult = mock(UpdateResult.class);
            agentDto = mock(AgentWithGroupBaseDto.class);


            doNothing().when(agentDto).verify();
            when(agentDto.getGroupId()).thenReturn("g-id");
            when(agentDto.getAgentId()).thenReturn("a-id");

            doNothing().when(agentGroupService).verifyAgent(anyList(), any(UserDetail.class));
            when(agentGroupService.findGroupById("g-id", userDetail)).thenReturn(groupDto);
            when(groupDto.getAgentIds()).thenReturn(agents);

            when(agents.isEmpty()).thenReturn(false);
            when(agents.contains("a-id")).thenReturn(false);

            when(log.isDebugEnabled()).thenReturn(true);

            when(groupDto.toString()).thenReturn("");
            doNothing().when(log).debug("group agent repeatedly, {}", "");
            when(agentGroupService.findAgentGroupInfo("g-id", userDetail)).thenReturn(mock(AgentGroupDto.class));

            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);

            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(updateResult);
            when(updateResult.getModifiedCount()).thenReturn(1L);

            when(groupDto.getName()).thenReturn("name");
            doNothing().when(log).info("Agent: {} has be added to group: {} ", "a-id", "name");


            when(agentGroupService.addAgentToGroup(agentDto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.addAgentToGroup(agentDto, userDetail));
            verify(agentDto, times(1)).verify();
            verify(agentDto, times(1)).getGroupId();
            verify(agentDto, times(1)).getAgentId();
            verify(agentGroupService, times(1)).verifyAgent(anyList(), any(UserDetail.class));
            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(groupDto, times(1)).getAgentIds();
            verify(agents, times(1)).isEmpty();
            verify(agents, times(1)).contains("a-id");

            verify(log, times(0)).isDebugEnabled();
            verify(log, times(0)).debug("group agent repeatedly, {}", "");

            verify(agentGroupService, times(1)).findAgentGroupInfo("g-id", userDetail);
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(updateResult, times(1)).getModifiedCount();
            verify(groupDto, times(1)).getName();
            verify(log, times(1)).info("Agent: {} has be added to group: {} ", "a-id", "name");
        }

        @Test
        void testModifiedCountLessThanZero() {
            when(updateResult.getModifiedCount()).thenReturn(0L);
            Assertions.assertThrows(BizException.class, () -> agentGroupService.addAgentToGroup(agentDto, userDetail));
            verify(agentDto, times(1)).verify();
            verify(agentDto, times(1)).getGroupId();
            verify(agentDto, times(1)).getAgentId();
            verify(agentGroupService, times(1)).verifyAgent(anyList(), any(UserDetail.class));
            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(groupDto, times(1)).getAgentIds();
            verify(agents, times(1)).isEmpty();
            verify(agents, times(1)).contains("a-id");

            verify(log, times(0)).isDebugEnabled();
            verify(log, times(0)).debug("group agent repeatedly, {}", "");

            verify(agentGroupService, times(0)).findAgentGroupInfo("g-id", userDetail);
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(updateResult, times(1)).getModifiedCount();
            verify(groupDto, times(0)).getName();
            verify(log, times(0)).info("Agent: {} has be added to group: {} ", "a-id", "name");
        }

        @Test
        void testAgentsIsEmpty() {
            when(agents.isEmpty()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> agentGroupService.addAgentToGroup(agentDto, userDetail));
            verify(agentDto, times(1)).verify();
            verify(agentDto, times(1)).getGroupId();
            verify(agentDto, times(1)).getAgentId();
            verify(agentGroupService, times(1)).verifyAgent(anyList(), any(UserDetail.class));
            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(groupDto, times(1)).getAgentIds();
            verify(agents, times(1)).isEmpty();
            verify(agents, times(0)).contains("a-id");

            verify(log, times(0)).isDebugEnabled();
            verify(log, times(0)).debug("group agent repeatedly, {}", "");

            verify(agentGroupService, times(1)).findAgentGroupInfo("g-id", userDetail);
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(updateResult, times(1)).getModifiedCount();
            verify(groupDto, times(1)).getName();
            verify(log, times(1)).info("Agent: {} has be added to group: {} ", "a-id", "name");
        }

        @Test
        void testAgentsContains() {
            when(agents.contains("a-id")).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> agentGroupService.addAgentToGroup(agentDto, userDetail));
            verify(agentDto, times(1)).verify();
            verify(agentDto, times(1)).getGroupId();
            verify(agentDto, times(1)).getAgentId();
            verify(agentGroupService, times(1)).verifyAgent(anyList(), any(UserDetail.class));
            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(groupDto, times(1)).getAgentIds();
            verify(agents, times(1)).isEmpty();
            verify(agents, times(1)).contains("a-id");

            verify(log, times(1)).isDebugEnabled();
            verify(log, times(1)).debug("group agent repeatedly, {}", "");

            verify(agentGroupService, times(1)).findAgentGroupInfo("g-id", userDetail);
            verify(agentGroupService, times(0)).findCriteria(anyList());
            verify(agentGroupService, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(updateResult, times(0)).getModifiedCount();
            verify(groupDto, times(0)).getName();
            verify(log, times(0)).info("Agent: {} has be added to group: {} ", "a-id", "name");
        }
        @Test
        void testNotDebug() {
            when(agents.contains("a-id")).thenReturn(true);
            when(log.isDebugEnabled()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> agentGroupService.addAgentToGroup(agentDto, userDetail));
            verify(agentDto, times(1)).verify();
            verify(agentDto, times(1)).getGroupId();
            verify(agentDto, times(1)).getAgentId();
            verify(agentGroupService, times(1)).verifyAgent(anyList(), any(UserDetail.class));
            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(groupDto, times(1)).getAgentIds();
            verify(agents, times(1)).isEmpty();
            verify(agents, times(1)).contains("a-id");

            verify(log, times(1)).isDebugEnabled();
            verify(log, times(0)).debug("group agent repeatedly, {}", "");

            verify(agentGroupService, times(1)).findAgentGroupInfo("g-id", userDetail);
            verify(agentGroupService, times(0)).findCriteria(anyList());
            verify(agentGroupService, times(0)).update(any(Query.class), any(Update.class), any(UserDetail.class));
            verify(updateResult, times(0)).getModifiedCount();
            verify(groupDto, times(0)).getName();
            verify(log, times(0)).info("Agent: {} has be added to group: {} ", "a-id", "name");
        }
    }

    @Nested
    class VerifyAgentTest {
        List<String> agentIds;
        List<WorkerDto> allAgent;
        @BeforeEach
        void init() {
            agentIds = mock(List.class);
            when(agentIds.toString()).thenReturn("[]");
            allAgent = mock(List.class);
            when(allAgent.isEmpty()).thenReturn(false);

            when(agentGroupService.findAllAgent(agentIds, userDetail)).thenReturn(allAgent);
            doCallRealMethod().when(agentGroupService).verifyAgent(agentIds, userDetail);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.verifyAgent(agentIds, userDetail));
            verify(agentGroupService, times(1)).findAllAgent(agentIds, userDetail);
            verify(allAgent, times(1)).isEmpty();
        }
        @Test
        void testEmpty() {
            when(allAgent.isEmpty()).thenReturn(true);
            Assertions.assertThrows(BizException.class, () -> agentGroupService.verifyAgent(agentIds, userDetail));
            verify(agentGroupService, times(1)).findAllAgent(agentIds, userDetail);
            verify(allAgent, times(1)).isEmpty();
        }
    }

    @Nested
    class RemoveAgentFromGroupTest {
        AgentRemoveFromGroupDto removeDto;
        GroupDto groupDto;
        Criteria criteria;
        UpdateResult updateResult;

        AgentGroupDto dto;
        @BeforeEach
        void init() {
            removeDto = mock(AgentRemoveFromGroupDto.class);
            doNothing().when(removeDto).verify();
            when(removeDto.getGroupId()).thenReturn("g-id");
            when(removeDto.getAgentId()).thenReturn("a-id");

            groupDto = mock(GroupDto.class);
            when(groupDto.getGroupId()).thenReturn("gg-id");
            when(groupDto.getName()).thenReturn("gg-name");

            criteria = Criteria.where("is_delete").ne(false);

            updateResult = mock(UpdateResult.class);
            when(updateResult.getModifiedCount()).thenReturn(1L);

            dto = mock(AgentGroupDto.class);

            when(agentGroupService.findGroupById("g-id", userDetail)).thenReturn(groupDto);
            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);
            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(updateResult);
            doNothing().when(log).info("Agent: {} has be removed from group: {} ", "a-id", "gg-name");
            when(agentGroupService.findAgentGroupInfo("g-id", userDetail)).thenReturn(dto);

            when(agentGroupService.removeAgentFromGroup(removeDto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.removeAgentFromGroup(removeDto, userDetail));
            verify(removeDto, times(1)).verify();
            verify(removeDto, times(1)).getGroupId();
            verify(removeDto, times(1)).getAgentId();

            verify(groupDto, times(1)).getGroupId();
            verify(groupDto, times(1)).getName();

            verify(updateResult, times(1)).getModifiedCount();

            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(1)).info("Agent: {} has be removed from group: {} ", "a-id", "gg-name");

            verify(agentGroupService, times(1)).findAgentGroupInfo("g-id", userDetail);
        }

        @Test
        void testModifiedCountLessThanZero() {
            when(updateResult.getModifiedCount()).thenReturn(0L);
            Assertions.assertThrows(BizException.class, () -> agentGroupService.removeAgentFromGroup(removeDto, userDetail));
            verify(removeDto, times(1)).verify();
            verify(removeDto, times(1)).getGroupId();
            verify(removeDto, times(1)).getAgentId();

            verify(groupDto, times(1)).getGroupId();
            verify(groupDto, times(0)).getName();

            verify(updateResult, times(1)).getModifiedCount();

            verify(agentGroupService, times(1)).findGroupById("g-id", userDetail);
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));

            verify(log, times(0)).info("Agent: {} has be removed from group: {} ", "a-id", "gg-name");

            verify(agentGroupService, times(0)).findAgentGroupInfo("g-id", userDetail);
        }
    }

    @Nested
    class DeleteGroupTest {
        GroupDto groupDto;
        List<DataSourceConnectionDto> beUsedConnections;
        List<TaskDto> serviceAllDto;
        @BeforeEach
        void init() {
            beUsedConnections = mock(List.class);
            when(beUsedConnections.isEmpty()).thenReturn(true);

            serviceAllDto = mock(List.class);
            when(serviceAllDto.isEmpty()).thenReturn(true);

            groupDto = mock(GroupDto.class);
            when(groupDto.getName()).thenReturn("name");
            when(groupDto.getAgentIds()).thenReturn(mock(List.class));
            when(groupDto.getId()).thenReturn(mock(ObjectId.class));

            when(agentGroupService.findGroupById("groupId", userDetail)).thenReturn(groupDto);

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(beUsedConnections);
            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(serviceAllDto);
            when(agentGroupService.deleteById(any(ObjectId.class), any(UserDetail.class))).thenReturn(true);
            doNothing().when(log).info("Agent group has be deleted: {} ", "name");
            when(agentGroupService.deleteGroup("groupId", userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertNotNull(agentGroupService.deleteGroup("groupId", userDetail));
            verify(agentGroupService, times(1)).findGroupById("groupId", userDetail);
            verify(dataSourceService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(taskService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(agentGroupService, times(1)).deleteById(any(ObjectId.class), any(UserDetail.class));
            verify(log, times(1)).info("Agent group has be deleted: {} ", "name");
        }

        @Test
        void testBeUsedConnectionsIsEmpty() {
            when(beUsedConnections.isEmpty()).thenReturn(false);
            Assertions.assertNotNull(agentGroupService.deleteGroup("groupId", userDetail));
            verify(agentGroupService, times(1)).findGroupById("groupId", userDetail);
            verify(dataSourceService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(taskService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(agentGroupService, times(0)).deleteById(any(ObjectId.class), any(UserDetail.class));
            verify(log, times(0)).info("Agent group has be deleted: {} ", "name");
        }

        @Test
        void testServiceAllDtoIsEmpty() {
            when(serviceAllDto.isEmpty()).thenReturn(false);
            Assertions.assertNotNull(agentGroupService.deleteGroup("groupId", userDetail));
            verify(agentGroupService, times(1)).findGroupById("groupId", userDetail);
            verify(dataSourceService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(taskService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(agentGroupService, times(0)).deleteById(any(ObjectId.class), any(UserDetail.class));
            verify(log, times(0)).info("Agent group has be deleted: {} ", "name");
        }

        @Test
        void testNotDeleted() {
            when(agentGroupService.deleteById(any(ObjectId.class), any(UserDetail.class))).thenReturn(false);
            Assertions.assertNotNull(agentGroupService.deleteGroup("groupId", userDetail));
            verify(agentGroupService, times(1)).findGroupById("groupId", userDetail);
            verify(dataSourceService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(taskService, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
            verify(agentGroupService, times(1)).deleteById(any(ObjectId.class), any(UserDetail.class));
            verify(log, times(0)).info("Agent group has be deleted: {} ", "name");
        }
    }

    @Nested
    class UpdateBaseInfoTest {
        GroupDto dto;
        Criteria criteria;
        Query query;
        @BeforeEach
        void init() {
            dto = mock(GroupDto.class);
            criteria = mock(Criteria.class);
            query = mock(Query.class);

            when(dto.getGroupId()).thenReturn("id");
            when(dto.getName()).thenReturn("name");
            when(agentGroupService.verifyCountGroupByName("name", userDetail)).thenReturn(query);
            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);
            when(agentGroupService.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(mock(UpdateResult.class));
            when(agentGroupService.findAgentGroupInfo("id", userDetail)).thenReturn(mock(AgentGroupDto.class));
            doNothing().when(agentGroupUtil).verifyUpdateGroupInfo(dto);
            when(agentGroupService.updateBaseInfo(dto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            try (MockedStatic<Query> q = mockStatic(Query.class)) {
                q.when(() -> Query.query(criteria)).thenReturn(query);
                AgentGroupDto d = agentGroupService.updateBaseInfo(this.dto, userDetail);
                q.verify(() -> Query.query(criteria), times(1));
                Assertions.assertNotNull(d);
                verify(dto, times(2)).getGroupId();
                verify(dto, times(1)).getName();
                verify(agentGroupService, times(1)).verifyCountGroupByName("name", userDetail);
                verify(agentGroupService, times(1)).findCriteria(anyList());
                verify(agentGroupService, times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
                verify(agentGroupService, times(1)).findAgentGroupInfo("id", userDetail);
                verify(agentGroupUtil, times(1)).verifyUpdateGroupInfo(dto);
            }
        }
    }

    @Nested
    class FindGroupByIdTest {
        Criteria criteria;
        GroupDto groupDto;
        Query query;
        @BeforeEach
        void init() {
            criteria = mock(Criteria.class);
            groupDto = mock(GroupDto.class);
            query = mock(Query.class);
            when(agentGroupService.findCriteria(anyList())).thenReturn(criteria);
            when(agentGroupService.findGroupById("groupId", userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            when(agentGroupService.findOne(query, userDetail)).thenReturn(groupDto);
            try(MockedStatic<Query> q = mockStatic(Query.class)) {
                q.when(() -> Query.query(criteria)).thenReturn(query);
                Assertions.assertNotNull(agentGroupService.findGroupById("groupId", userDetail));
                q.verify(() -> Query.query(criteria), times(1));
            }
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).findOne(query, userDetail);
        }

        @Test
        void testGroupDtoIsNull() {
            when(agentGroupService.findOne(query, userDetail)).thenReturn(null);
            try(MockedStatic<Query> q = mockStatic(Query.class)) {
                q.when(() -> Query.query(criteria)).thenReturn(query);
                Assertions.assertThrows(BizException.class, () -> agentGroupService.findGroupById("groupId", userDetail));
                q.verify(() -> Query.query(criteria), times(1));
            }
            verify(agentGroupService, times(1)).findCriteria(anyList());
            verify(agentGroupService, times(1)).findOne(query, userDetail);
        }
    }

    @Nested
    class FindAgentGroupInfoTest {
        @Nested
        class FindOneTest {
            List<AgentGroupDto> agentGroupInfoMany;
            @BeforeEach
            void init() {
                agentGroupInfoMany = mock(List.class);
                when(agentGroupService.findAgentGroupInfoMany(anyList(), any(UserDetail.class))).thenReturn(agentGroupInfoMany);
                when(agentGroupInfoMany.isEmpty()).thenReturn(false);
                when(agentGroupInfoMany.get(0)).thenReturn(mock(AgentGroupDto.class));
                when(agentGroupService.findAgentGroupInfo(anyString(), any(UserDetail.class))).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                AgentGroupDto dto = agentGroupService.findAgentGroupInfo("id", userDetail);
                Assertions.assertNotNull(dto);
                verify(agentGroupService, times(1)).findAgentGroupInfoMany(anyList(), any(UserDetail.class));
                verify(agentGroupInfoMany, times(1)).isEmpty();
                verify(agentGroupInfoMany, times(1)).get(0);
            }

            @Test
            void testAgentGroupInfoManyIsEmpty() {
                when(agentGroupInfoMany.isEmpty()).thenReturn(true);
                AgentGroupDto dto = agentGroupService.findAgentGroupInfo("id", userDetail);
                Assertions.assertNull(dto);
                verify(agentGroupService, times(1)).findAgentGroupInfoMany(anyList(), any(UserDetail.class));
                verify(agentGroupInfoMany, times(1)).isEmpty();
                verify(agentGroupInfoMany, times(0)).get(0);
            }
        }

        @Nested
        class FindManyTest {
            List<GroupDto> groupDto;
            GroupDto dto;

            List<String> agentIds;

            List<WorkerDto> all;
            @BeforeEach
            void init() {
                groupDto = new ArrayList<>();
                dto = mock(GroupDto.class);
                groupDto.add(dto);

                agentIds = new ArrayList<>();
                agentIds.add("id");

                when(dto.getAgentIds()).thenReturn(agentIds);

                all = mock(List.class);

                when(agentGroupService.findAllAgent(anySet(), any(UserDetail.class))).thenReturn(all);
                doNothing().when(agentGroupService).collectAgentGroupDto(any(GroupDto.class), anyList(), anyList());
                when(agentGroupService.findAgentGroupInfo(groupDto, userDetail)).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                List<AgentGroupDto> agentGroupInfo = agentGroupService.findAgentGroupInfo(groupDto, userDetail);
                Assertions.assertNotNull(agentGroupInfo);
                verify(dto, times(1)).getAgentIds();
                verify(agentGroupService, times(1)).findAllAgent(anySet(), any(UserDetail.class));
                verify(agentGroupService, times(1)).collectAgentGroupDto(any(GroupDto.class), anyList(), anyList());
            }
            @Test
            void testAgentIdsIsEmpty() {
                agentIds.clear();
                List<AgentGroupDto> ls = new ArrayList<>();
                try(MockedStatic<Lists> l = mockStatic(Lists.class)) {
                    l.when(Lists::newArrayList).thenReturn(ls);
                    doNothing().when(agentGroupService).collectAgentGroupDto(dto, null, ls);
                    List<AgentGroupDto> agentGroupInfo = agentGroupService.findAgentGroupInfo(groupDto, userDetail);
                    Assertions.assertNotNull(agentGroupInfo);
                    verify(dto, times(1)).getAgentIds();
                    verify(agentGroupService, times(0)).findAllAgent(anySet(), any(UserDetail.class));
                    verify(agentGroupService, times(1)).collectAgentGroupDto(dto, null, ls);
                }
            }
        }
    }

    @Nested
    class CollectAgentGroupDtoTest {
        GroupDto group;
        List<WorkerDto> all;
        WorkerDto workerDto;

        List<AgentGroupDto> info;
        AgentGroupDto agentGroupDto;

        List<String> agentIds;
        @BeforeEach
        void init() {
            group = mock(GroupDto.class);
            when(group.getName()).thenReturn("name");
            when(group.getGroupId()).thenReturn("id");


            workerDto = mock(WorkerDto.class);
            when(workerDto.getProcessId()).thenReturn("id");
            agentGroupDto = mock(AgentGroupDto.class);

            all = new ArrayList<>();
            all.add(workerDto);
            info = new ArrayList<>();
            info.add(agentGroupDto);

            agentIds = new ArrayList<>();
            agentIds.add("id");


            when(group.getAgentIds()).thenReturn(agentIds);

            doCallRealMethod().when(agentGroupService).collectAgentGroupDto(group, all, info);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(2)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
        @Test
        void testAgentIdsIsEmpty() {
            agentIds.remove(0);
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(0)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
        @Test
        void testAllIsEmpty() {
            all.remove(0);
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(0)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
        @Test
        void testProcessIdIsNull() {
            when(workerDto.getProcessId()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(1)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
        @Test
        void testAllContainsNull() {
            all.add(null);
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(2)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
        @Test
        void testAgentIdsNotContainsProcessId() {
            agentIds.remove(0);
            agentIds.add("xxx");
            Assertions.assertDoesNotThrow(() -> agentGroupService.collectAgentGroupDto(group, all, info));
            verify(group, times(1)).getAgentIds();
            verify(workerDto, times(2)).getProcessId();
            verify(group, times(1)).getName();
            verify(group, times(1)).getGroupId();
        }
    }

    @Nested
    class FindAgentGroupInfoManyTest {
        List<String> groupIds;
        Criteria criteria;
        Query query;

        @BeforeEach
        void init() {
            groupIds = mock(List.class);
            criteria = mock(Criteria.class);
            query = mock(Query.class);

            when(agentGroupService.findCriteria(groupIds)).thenReturn(criteria);
            when(agentGroupService.findAgentGroupInfo(query, userDetail)).thenReturn(mock(List.class));
            when(agentGroupService.findAgentGroupInfoMany(groupIds, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            try(MockedStatic<Query> q = mockStatic(Query.class)) {
                q.when(() -> Query.query(criteria)).thenReturn(query);
                Assertions.assertDoesNotThrow(() -> agentGroupService.findAgentGroupInfoMany(groupIds, userDetail));
                q.verify(() -> Query.query(criteria), times(1));
            }
            verify(agentGroupService, times(1)).findCriteria(groupIds);
            verify(agentGroupService, times(1)).findAgentGroupInfo(query, userDetail);
            verify(agentGroupService, times(1)).findAgentGroupInfoMany(groupIds, userDetail);
        }
    }
    @Nested
    class FindAgentGroupInfoByFilterOrQueryTest {
        List<AgentGroupDto> result;
        @BeforeEach
        void init() {
            result = mock(List.class);
            when(agentGroupService.findAgentGroupInfo(anyList(), any(UserDetail.class))).thenReturn(result);
        }

        @Nested
        class ByQuery {
            Query query;
            @BeforeEach
            void init() {
                query = mock(Query.class);
                when(agentGroupService.findAllDto(query, userDetail)).thenReturn(mock(List.class));
                when(agentGroupService.findAgentGroupInfo(query, userDetail)).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> agentGroupService.findAgentGroupInfo(query, userDetail));
                verify(agentGroupService, times(1)).findAllDto(query, userDetail);
                verify(agentGroupService, times(1)).findAgentGroupInfo(anyList(), any(UserDetail.class));
            }
        }

        @Nested
        class ByFilter {
            Filter filter;
            @BeforeEach
            void init() {
                filter = mock(Filter.class);
                when(agentGroupUtil.initFilter(filter)).thenReturn(filter);
                when(agentGroupService.findOne(filter, userDetail)).thenReturn(mock(GroupDto.class));
                when(agentGroupService.findAgentGroupInfo(anyList(), any(UserDetail.class))).thenReturn(mock(List.class));
                when(agentGroupService.findAgentGroupInfo(filter, userDetail)).thenCallRealMethod();
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> agentGroupService.findAgentGroupInfo(filter, userDetail));
                verify(agentGroupUtil, times(1)).initFilter(filter);
                verify(agentGroupService, times(1)).findOne(filter, userDetail);
                verify(agentGroupService, times(1)).findAgentGroupInfo(anyList(), any(UserDetail.class));
            }
        }
    }

    @Nested
    class FindAllAgentTest {
        Collection<String> agentIds;
        @BeforeEach
        void init() {
            agentIds = mock(Collection.class);
            when(agentIds.isEmpty()).thenReturn(false);
            when(workerServiceImpl.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(mock(List.class));
            when(agentGroupService.findAllAgent(agentIds, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            List<WorkerDto> allAgent = agentGroupService.findAllAgent(agentIds, userDetail);
            Assertions.assertNotNull(allAgent);
            verify(workerServiceImpl, times(1)).findAllDto(any(Query.class), any(UserDetail.class));
        }

        @Test
        void testAgentIdsIsEmpty() {
            when(agentIds.isEmpty()).thenReturn(true);
            List<WorkerDto> allAgent = agentGroupService.findAllAgent(agentIds, userDetail);
            Assertions.assertNotNull(allAgent);
            verify(workerServiceImpl, times(0)).findAllDto(any(Query.class), any(UserDetail.class));
        }
    }

    @Nested
    class FilterGroupListTest {
        List<AccessNodeInfo> info;
        AccessNodeInfo accessNodeInfo;
        Criteria criteria;
        List<AgentGroupEntity> entities;
        AgentGroupEntity entity;
        List<String> agentIds;

        @BeforeEach
        void init() {
            criteria = mock(Criteria.class);
            entities = new ArrayList<>();
            entity = mock(AgentGroupEntity.class);
            entities.add(entity);
            agentIds = new ArrayList<>();
            agentIds.add("id");
            when(entity.getAgentIds()).thenReturn(agentIds);

            when(agentGroupUtil.sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class))).thenReturn(1);
            when(agentGroupUtil.mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap())).thenReturn(accessNodeInfo);

            info = new ArrayList<>();
            accessNodeInfo = mock(AccessNodeInfo.class);
            when(accessNodeInfo.getProcessId()).thenReturn("id");
            info.add(accessNodeInfo);

            when(settingsService.isCloud()).thenReturn(false);
            when(agentGroupService.findCriteria(null)).thenReturn(criteria);
            when(agentGroupService.findAll(any(Query.class), any(UserDetail.class))).thenReturn(entities);

            when(agentGroupService.filterGroupList(info, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(2)).getProcessId();
            verify(agentGroupService, times(1)).findCriteria(null);
            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(1)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }

        @Test
        void testIsCloud() {
            when(settingsService.isCloud()).thenReturn(true);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(0)).getProcessId();
            verify(agentGroupService, times(0)).findCriteria(null);
            verify(agentGroupService, times(0)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(0)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }

        @Test
        void testIsEmptyInfo() {
            info.remove(0);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(0)).getProcessId();
            verify(agentGroupService, times(0)).findCriteria(null);
            verify(agentGroupService, times(0)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(0)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }
        @Test
        void testInfoHasNullInfo() {
            info.add(null);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(2)).getProcessId();
            verify(agentGroupService, times(1)).findCriteria(null);
            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(1)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }
        @Test
        void testEntitiesHasNullInfo() {
            entities.add(null);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(2)).getProcessId();
            verify(agentGroupService, times(1)).findCriteria(null);
            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(1)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }
        @Test
        void testAgentIdsIsEmpty() {
            agentIds.remove(0);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(2)).getProcessId();
            verify(agentGroupService, times(1)).findCriteria(null);
            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(1)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }
        @Test
        void testProcessIdIsNull() {
            when(accessNodeInfo.getProcessId()).thenReturn(null);
            List<AccessNodeInfo> accessNodeInfos = agentGroupService.filterGroupList(info, userDetail);
            Assertions.assertNotNull(accessNodeInfos);
            verify(settingsService, times(1)).isCloud();
            verify(accessNodeInfo, times(1)).getProcessId();
            verify(agentGroupService, times(1)).findCriteria(null);
            verify(agentGroupService, times(1)).findAll(any(Query.class), any(UserDetail.class));
            verify(entity, times(0)).getAgentIds();
            verify(agentGroupUtil, times(0)).sortAgentGroup(any(AgentGroupEntity.class), any(AgentGroupEntity.class));
            verify(agentGroupUtil, times(1)).mappingAccessNodeInfo(any(AgentGroupEntity.class), anyMap());
        }

    }

    @Nested
    class GetProcessNodeListWithGroupTest {
        List<String> processNodeList;
        @BeforeEach
        void init() {
            processNodeList = mock(List.class);
            when(processNodeList.isEmpty()).thenReturn(false);
        }

        @Nested
        class ByTaskDtoTest {
            TaskDto taskDto;
            @BeforeEach
            void init() {
                taskDto = mock(TaskDto.class);
                when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
                when(taskDto.getAccessNodeProcessId()).thenReturn("id");
                when(taskDto.getAccessNodeProcessIdList()).thenReturn(processNodeList);
                doNothing().when(taskDto).setAccessNodeProcessIdList(processNodeList);
                when(agentGroupService.getGroupProcessNodeList("type", "id", userDetail)).thenReturn(processNodeList);
                when(agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail)).thenCallRealMethod();
            }
            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail));
                verify(taskDto, times(1)).getAccessNodeType();
                verify(taskDto, times(1)).getAccessNodeProcessId();
                verify(agentGroupService, times(1)).getGroupProcessNodeList(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), "id", userDetail);
                verify(taskDto, times(0)).getAccessNodeProcessIdList();
                verify(taskDto, times(1)).setAccessNodeProcessIdList(anyList());
            }

            @Test
            void testNotByGroup() {
                when(taskDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
                when(processNodeList.isEmpty()).thenReturn(true);
                Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail));
                verify(taskDto, times(1)).getAccessNodeType();
                verify(taskDto, times(0)).getAccessNodeProcessId();
                verify(agentGroupService, times(0)).getGroupProcessNodeList(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), "id", userDetail);
                verify(taskDto, times(1)).getAccessNodeProcessIdList();
                verify(taskDto, times(0)).setAccessNodeProcessIdList(processNodeList);
            }
        }
        @Nested
        class ByDataSourceConnectionDtoTest {
            DataSourceConnectionDto connectionDto;
            @BeforeEach
            void init() {
                connectionDto = mock(DataSourceConnectionDto.class);
                when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
                when(connectionDto.getAccessNodeProcessIdList()).thenReturn(processNodeList);
                when(agentGroupService.getDataSourceConnectionProcessNodeList(connectionDto, userDetail)).thenReturn(processNodeList);
                when(agentGroupService.getProcessNodeListWithGroup(connectionDto, userDetail)).thenCallRealMethod();
            }
            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListWithGroup(connectionDto, userDetail));
                verify(connectionDto, times(0)).getAccessNodeProcessIdList();
                verify(processNodeList, times(0)).isEmpty();
                verify(agentGroupService, times(1)).getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
            }

            @Test
            void testNotByGroup() {
                when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
                when(processNodeList.isEmpty()).thenReturn(true);
                Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListWithGroup(connectionDto, userDetail));
                verify(connectionDto, times(1)).getAccessNodeProcessIdList();
                verify(processNodeList, times(0)).isEmpty();
                verify(agentGroupService, times(0)).getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
            }
        }
    }

    @Nested
    class GetTrueProcessNodeListWithGroupTest {
        DataSourceConnectionDto connectionDto;
        List<String> processNodeList;
        @BeforeEach
        void init() {
            connectionDto = mock(DataSourceConnectionDto.class);
            processNodeList = mock(List.class);
            when(processNodeList.isEmpty()).thenReturn(false);
            when(connectionDto.getTrueAccessNodeProcessIdList()).thenReturn(processNodeList);
            when(agentGroupService.getDataSourceConnectionProcessNodeList(connectionDto, userDetail)).thenReturn(processNodeList);
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            when(agentGroupService.getTrueProcessNodeListWithGroup(connectionDto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.getTrueProcessNodeListWithGroup(connectionDto, userDetail));
            verify(agentGroupService, times(1)).getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
            verify(connectionDto, times(0)).getTrueAccessNodeProcessIdList();
        }

        @Test
        void testNotGroup() {
            when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
            when(processNodeList.isEmpty()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> agentGroupService.getTrueProcessNodeListWithGroup(connectionDto, userDetail));
            verify(agentGroupService, times(0)).getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
            verify(connectionDto, times(1)).getTrueAccessNodeProcessIdList();
        }
    }

    @Nested
    class GetDataSourceConnectionProcessNodeListTest {
        DataSourceConnectionDto connectionDto;
        List<String> processNodeList;
        @BeforeEach
        void init() {
            processNodeList = mock(List.class);
            connectionDto = mock(DataSourceConnectionDto.class);
            when(connectionDto.getAccessNodeType()).thenReturn("type");
            when(connectionDto.getAccessNodeProcessId()).thenReturn("id");
            when(agentGroupService.getGroupProcessNodeList("type", "id", userDetail)).thenReturn(processNodeList);
            doNothing().when(connectionDto).setAccessNodeProcessIdList(processNodeList);
            when(agentGroupService.getDataSourceConnectionProcessNodeList(connectionDto, userDetail)).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> agentGroupService.getDataSourceConnectionProcessNodeList(connectionDto, userDetail));
        }
    }

    @Nested
    class GetProcessNodeListTest {
        @BeforeEach
        void init() {
            when(log.isDebugEnabled()).thenReturn(false);
            doNothing().when(log).debug(anyString(), anyString(), anyString());
            when(agentGroupService.getProcessNodeListByGroupId(anyList(), any(UserDetail.class))).thenReturn(mock(List.class));
            when(agentGroupService.getGroupProcessNodeList(anyString(), anyString(), any(UserDetail.class))).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            List<String> nodeList = agentGroupService.getGroupProcessNodeList("", "", userDetail);
            Assertions.assertNotNull(nodeList);
            verify(log, times(0)).debug(anyString(), anyString(), anyString());
            verify(agentGroupService, times(1)).getProcessNodeListByGroupId(anyList(), any(UserDetail.class));
        }

        @Test
        void testIsGroupManually() {
            List<String> nodeList = agentGroupService.getGroupProcessNodeList(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), "", userDetail);
            Assertions.assertNotNull(nodeList);
            verify(log, times(0)).debug(anyString(), anyString(), anyString());
            verify(agentGroupService, times(1)).getProcessNodeListByGroupId(anyList(), any(UserDetail.class));
        }
        @Test
        void testIsNotBlank() {
            List<String> nodeList = agentGroupService.getGroupProcessNodeList(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), "id", userDetail);
            Assertions.assertNotNull(nodeList);
            verify(log, times(0)).debug(anyString(), anyString(), anyString());
            verify(agentGroupService, times(1)).getProcessNodeListByGroupId(anyList(), any(UserDetail.class));
        }
        @Test
        void testIsDebugEnabled() {
            when(log.isDebugEnabled()).thenReturn(true);
            List<String> nodeList = agentGroupService.getGroupProcessNodeList(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), "id", userDetail);
            Assertions.assertNotNull(nodeList);
            verify(log, times(1)).debug(anyString(), anyString(), anyString());
            verify(agentGroupService, times(1)).getProcessNodeListByGroupId(anyList(), any(UserDetail.class));
        }
    }

    @Nested
    class GetProcessNodeListByGroupIdFilterByAccessNodeTypeTest {
        List<String> accessNodeGroupProcessId;
        @BeforeEach
        void init() {
            when(agentGroupService.getProcessNodeListByGroupId(accessNodeGroupProcessId, userDetail)).thenReturn(accessNodeGroupProcessId);
        }

        @Test
        void testNormal() {
            when(agentGroupService.getProcessNodeListByGroupId(accessNodeGroupProcessId, AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), userDetail)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListByGroupId(accessNodeGroupProcessId, AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name(), userDetail));
            verify(agentGroupService, times(1)).getProcessNodeListByGroupId(accessNodeGroupProcessId, userDetail);
        }

        @Test
        void testNotIsGroupManually() {
            when(agentGroupService.getProcessNodeListByGroupId(accessNodeGroupProcessId, "", userDetail)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> agentGroupService.getProcessNodeListByGroupId(accessNodeGroupProcessId, "", userDetail));
            verify(agentGroupService, times(0)).getProcessNodeListByGroupId(accessNodeGroupProcessId, userDetail);
        }
    }

    @Nested
    class GetProcessNodeListByGroupIdTest {
        List<String> groupIds;
        Criteria criteria;
        Query query;
        List<AgentGroupEntity> all;
        AgentGroupEntity dto;
        @BeforeEach
        void init() {
            groupIds = mock(List.class);
            when(groupIds.isEmpty()).thenReturn(false);

            when(groupIds.toString()).thenReturn("[]");
            criteria = mock(Criteria.class);
            query = mock(Query.class);
            all = Lists.newArrayList();

            dto = mock(AgentGroupEntity.class);
            when(dto.getName()).thenReturn("name");
            when(dto.getAgentIds()).thenReturn(Lists.newArrayList("id"));
            all.add(dto);

            when(agentGroupService.getProcessNodeListByGroupId(groupIds, userDetail)).thenCallRealMethod();
            when(agentGroupService.findCriteria(groupIds)).thenReturn(criteria);
            when(agentGroupService.findAll(query, userDetail)).thenReturn(all);
        }

        void assertVerify(int exceptedSize, int queryTimes, int findCriteriaTimes, int findAllTimes, int getAgentIdsTimes) {
            try(MockedStatic<Query> q = mockStatic(Query.class)) {
                q.when(() -> Query.query(criteria)).thenReturn(query);
                List<String> groupId = agentGroupService.getProcessNodeListByGroupId(groupIds, userDetail);
                Assertions.assertNotNull(groupId);
                Assertions.assertEquals(exceptedSize, groupId.size());
                q.verify(() -> Query.query(criteria), times(queryTimes));
            }
            verify(groupIds, times(1)).isEmpty();
            verify(agentGroupService, times(findCriteriaTimes)).findCriteria(groupIds);
            verify(agentGroupService, times(findAllTimes)).findAll(query, userDetail);
            verify(dto, times(getAgentIdsTimes)).getAgentIds();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1, 1, 2));
        }

        @Test
        void testGroupIdsIsEmpty() {
            when(groupIds.isEmpty()).thenReturn(true);
            Assertions.assertThrows(BizException.class, () -> assertVerify(0, 0, 0, 0, 0));
        }

        @Test
        void testAllIsEmpty() {
            all.remove(0);
            Assertions.assertThrows(BizException.class, () -> assertVerify(0, 1, 1, 1, 0));
        }

        @Test
        void testAgentsIsEmpty() {
            when(dto.getAgentIds()).thenReturn(Lists.newArrayList());
            Assertions.assertThrows(BizException.class, () -> assertVerify(0, 1, 1, 1, 1));
        }

        @Test
        void testAgentIsNull() {
            all.add(null);
            Assertions.assertDoesNotThrow(() -> assertVerify(1, 1, 1, 1, 2));
        }
    }

    @Nested
    class FindCriteriaTest {
        @BeforeEach
        void init() {
            when(agentGroupService.findCriteria(anyList())).thenCallRealMethod();
            when(agentGroupService.findCriteria(null)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            Assertions.assertNotNull(agentGroupService.findCriteria(Lists.newArrayList("id1", "id2")));
        }
        @Test
        void testNullGroupIds() {
            Assertions.assertNotNull(agentGroupService.findCriteria(null));
        }
        @Test
        void testEmptyGroupIds() {
            Assertions.assertNotNull(agentGroupService.findCriteria(Lists.newArrayList()));
        }
        @Test
        void testGroupIdsSizeIsOne() {
            Assertions.assertNotNull(agentGroupService.findCriteria(Lists.newArrayList("id1")));
        }
        @Test
        void testGroupIdsSizeMoreThanOne() {
            Assertions.assertNotNull(agentGroupService.findCriteria(Lists.newArrayList("id1", "id2")));
        }
    }

    @Nested
    class UploadAgentInfoTest {
        TaskDto dto;
        GroupDto one;
        @BeforeEach
        void init() {
            one = mock(GroupDto.class);
            dto = mock(TaskDto.class);
            when(dto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            when(dto.getAccessNodeProcessId()).thenReturn("id");
            when(agentGroupService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(one);
            when(one.getGroupId()).thenReturn("id");
            when(one.getName()).thenReturn("name");
            doNothing().when(dto).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            doNothing().when(dto).setAccessNodeProcessId(null);
            doNothing().when(dto).setAccessNodeProcessIdList(anyList());
            doNothing().when(dto).setAgentGroupInfo(anyMap());
            when(agentGroupService.findCriteria(anyList())).thenCallRealMethod();
            doCallRealMethod().when(agentGroupService).uploadAgentInfo(dto, userDetail);
        }
        @Test
        void testNormal() {
            agentGroupService.uploadAgentInfo(dto, userDetail);
            verify(dto, times(1)).getAccessNodeType();
            verify(dto, times(1)).getAccessNodeProcessId();
            verify(agentGroupService, times(1)).findOne(any(Query.class), any(UserDetail.class));
            verify(one, times(1)).getGroupId();
            verify(one, times(1)).getName();
            verify(dto, times(0)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            verify(dto, times(0)).setAccessNodeProcessId(null);
            verify(dto, times(1)).setAgentGroupInfo(anyMap());
            verify(dto, times(1)).setAccessNodeProcessIdList(anyList());
        }
        @Test
        void testFindNullDto() {
            when(agentGroupService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(null);
            agentGroupService.uploadAgentInfo(dto, userDetail);
            verify(dto, times(1)).getAccessNodeType();
            verify(dto, times(1)).getAccessNodeProcessId();
            verify(agentGroupService, times(1)).findOne(any(Query.class), any(UserDetail.class));
            verify(one, times(0)).getGroupId();
            verify(one, times(0)).getName();
            verify(dto, times(0)).setAgentGroupInfo(anyMap());
            verify(dto, times(0)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            verify(dto, times(0)).setAccessNodeProcessId(null);
            verify(dto, times(1)).setAccessNodeProcessIdList(anyList());
        }
        @Test
        void testNotGroupManually() {
            when(dto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            agentGroupService.uploadAgentInfo(dto, userDetail);
            verify(dto, times(1)).getAccessNodeType();
            verify(dto, times(0)).getAccessNodeProcessId();
            verify(agentGroupService, times(0)).findOne(any(Query.class), any(UserDetail.class));
            verify(one, times(0)).getGroupId();
            verify(one, times(0)).getName();
            verify(dto, times(0)).setAgentGroupInfo(anyMap());
            verify(dto, times(1)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            verify(dto, times(1)).setAccessNodeProcessId(null);
            verify(dto, times(1)).setAccessNodeProcessIdList(anyList());
        }
    }
    @Nested
    class ImportAgentInfoTest {
        
        @Nested
        class ImportTaskDtoTest {
            List<TaskDto> dto;
            TaskDto dto1;
            Map<String, Object> map;
            TaskDto dto2;
            @BeforeEach
            void init() {
                dto = new ArrayList<>();
                dto1 = mock(TaskDto.class);
                dto2 = mock(TaskDto.class);
                dto.add(dto1);
                dto.add(dto2);
                dto.add(null);
                map = mock(Map.class);
                when(map.isEmpty()).thenReturn(false);
                when(dto1.getAgentGroupInfo()).thenReturn(map);
                when(dto2.getAgentGroupInfo()).thenReturn(null);
                when(map.get(AgentGroupTag.TAG_GROUP_ID)).thenReturn(new ObjectId().toHexString());
                when(map.get((AgentGroupTag.TAG_NAME))).thenReturn("name");
                when(agentGroupService.upsert(any(Query.class), any(GroupDto.class), any(UserDetail.class))).thenReturn(0L);
                doNothing().when(agentGroupService).importAgentInfo(any(TaskDto.class));
                doCallRealMethod().when(agentGroupService).importAgentInfo(dto, userDetail);
            }
            @Test
            void testNormal() {
                agentGroupService.importAgentInfo(dto, userDetail);
                verify(dto1, times(3)).getAgentGroupInfo();
                verify(map, times(1)).isEmpty();
                verify(map, times(1)).get(AgentGroupTag.TAG_GROUP_ID);
                verify(map, times(1)).get((AgentGroupTag.TAG_NAME));
                verify(agentGroupService, times(2)).importAgentInfo(any(TaskDto.class));
                verify(agentGroupService, times(1)).upsert(any(Query.class), any(GroupDto.class), any(UserDetail.class));
            }
            @Test
            void testIsEmpty() {
                when(map.isEmpty()).thenReturn(true);
                agentGroupService.importAgentInfo(dto, userDetail);
                verify(dto1, times(2)).getAgentGroupInfo();
                verify(map, times(1)).isEmpty();
                verify(map, times(0)).get(AgentGroupTag.TAG_GROUP_ID);
                verify(map, times(0)).get((AgentGroupTag.TAG_NAME));
                verify(agentGroupService, times(2)).importAgentInfo(any(TaskDto.class));
                verify(agentGroupService, times(0)).upsert(any(Query.class), any(GroupDto.class), any(UserDetail.class));
            }
        }

        @Nested
        class DataSourceConnectionDtoTest {
            DataSourceConnectionDto connectionDto;
            @BeforeEach
            void init() {
                connectionDto = mock(DataSourceConnectionDto.class);
                when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                doNothing().when(connectionDto).setAccessNodeProcessId(null);
                doNothing().when(connectionDto).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                doNothing().when(connectionDto).setAccessNodeProcessIdList(anyList());
                doCallRealMethod().when(agentGroupService).importAgentInfo(connectionDto);
            }
            @Test
            void testNormal() {
                agentGroupService.importAgentInfo(connectionDto);
                verify(connectionDto, times(1)).getAccessNodeType();
                verify(connectionDto, times(1)).setAccessNodeProcessId(null);
                verify(connectionDto, times(1)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                verify(connectionDto, times(1)).setAccessNodeProcessIdList(anyList());
            }
            @Test
            void testIsGroup() {
                when(connectionDto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
                agentGroupService.importAgentInfo(connectionDto);
                verify(connectionDto, times(1)).getAccessNodeType();
                verify(connectionDto, times(0)).setAccessNodeProcessId(null);
                verify(connectionDto, times(0)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                verify(connectionDto, times(1)).setAccessNodeProcessIdList(anyList());
            }
        }
        @Nested
        class TaskDtoDtoTest {
            TaskDto dto;
            @BeforeEach
            void init() {
                dto = mock(TaskDto.class);
                when(dto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                doNothing().when(dto).setAccessNodeProcessId(null);
                doNothing().when(dto).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                doNothing().when(dto).setAccessNodeProcessIdList(anyList());
                doCallRealMethod().when(agentGroupService).importAgentInfo(dto);
            }
            @Test
            void testNormal() {
                agentGroupService.importAgentInfo(dto);
                verify(dto, times(1)).getAccessNodeType();
                verify(dto, times(1)).setAccessNodeProcessId(null);
                verify(dto, times(1)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                verify(dto, times(1)).setAccessNodeProcessIdList(anyList());
            }
            @Test
            void testIsGroup() {
                when(dto.getAccessNodeType()).thenReturn(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
                agentGroupService.importAgentInfo(dto);
                verify(dto, times(1)).getAccessNodeType();
                verify(dto, times(0)).setAccessNodeProcessId(null);
                verify(dto, times(0)).setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
                verify(dto, times(1)).setAccessNodeProcessIdList(anyList());
            }
        }
    }
}