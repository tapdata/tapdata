package com.tapdata.tm.cluster.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.ComponentStoppedRequest;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterComponentStopServiceTest {

    private ClusterComponentStopService service;
    private MongoTemplate mongoTemplate;
    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private StateMachineService stateMachineService;
    private TransformSchemaService transformSchema;
    private UserService userService;
    private UserDetail caller;

    @BeforeEach
    void init() {
        service = new ClusterComponentStopService();
        mongoTemplate = mock(MongoTemplate.class);
        taskService = mock(TaskService.class);
        taskScheduleService = mock(TaskScheduleService.class);
        stateMachineService = mock(StateMachineService.class);
        transformSchema = mock(TransformSchemaService.class);
        userService = mock(UserService.class);
        caller = mock(UserDetail.class);

        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "taskScheduleService", taskScheduleService);
        ReflectionTestUtils.setField(service, "stateMachineService", stateMachineService);
        ReflectionTestUtils.setField(service, "transformSchema", transformSchema);
        ReflectionTestUtils.setField(service, "userService", userService);

        UpdateResult ok = mock(UpdateResult.class);
        when(ok.getModifiedCount()).thenReturn(1L);
        when(mongoTemplate.updateMulti(any(Query.class), any(), anyString())).thenReturn(ok);

        // Default: ClusterState exists, all components are running. Tests that need different
        // pre-state (e.g. two components already stopped) override this stub.
        when(mongoTemplate.findOne(any(Query.class), eq(ClusterStateEntity.class), anyString()))
                .thenReturn(clusterStateWithStatuses("running", "running", "running"));
    }

    private ClusterStateEntity clusterStateWithStatuses(String engine, String api, String management) {
        ClusterStateEntity entity = new ClusterStateEntity();
        if (engine != null) {
            Component c = new Component();
            c.setStatus(engine);
            entity.setEngine(c);
        }
        if (api != null) {
            Component c = new Component();
            c.setStatus(api);
            entity.setApiServer(c);
        }
        if (management != null) {
            Component c = new Component();
            c.setStatus(management);
            entity.setManagement(c);
        }
        return entity;
    }

    @SuppressWarnings("unchecked")
    private Document captureSetClusterStateUpdate() {
        ArgumentCaptor<Update> updateCap = ArgumentCaptor.forClass(Update.class);
        ArgumentCaptor<String> collectionCap = ArgumentCaptor.forClass(String.class);
        verify(mongoTemplate, times(2)).updateMulti(any(Query.class), updateCap.capture(), collectionCap.capture());
        for (int i = 0; i < updateCap.getAllValues().size(); i++) {
            if ("ClusterState".equals(collectionCap.getAllValues().get(i))) {
                Update u = updateCap.getAllValues().get(i);
                return (Document) u.getUpdateObject().get("$set");
            }
        }
        throw new AssertionError("no ClusterState update captured");
    }

    @SuppressWarnings("unchecked")
    private Document captureSingleClusterStateUpdate() {
        ArgumentCaptor<Update> updateCap = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, times(1)).updateMulti(any(Query.class), updateCap.capture(), eq("ClusterState"));
        return (Document) updateCap.getValue().getUpdateObject().get("$set");
    }

    private TaskDto runningTask(String userId) {
        TaskDto t = new TaskDto();
        t.setId(new ObjectId());
        t.setUserId(userId);
        t.setStatus(TaskDto.STATUS_RUNNING);
        return t;
    }

    private void stubUsers(String... userIds) {
        Map<String, UserDetail> map = new HashMap<>();
        for (String id : userIds) {
            map.put(id, mock(UserDetail.class));
        }
        when(userService.getUserMapByIdList(anyList())).thenReturn(map);
    }

    private StateMachineResult okSm() {
        StateMachineResult sm = mock(StateMachineResult.class);
        when(sm.isFail()).thenReturn(false);
        return sm;
    }

    @Test
    void engineStop_marksWorkerStopping_andReschedulesAllOwnedTasks() {
        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_ENGINE);
        req.setProcessId("engine-A");

        List<TaskDto> tasks = Arrays.asList(runningTask("u1"), runningTask("u2"), runningTask("u3"));
        when(taskService.findAll(any(Query.class))).thenReturn(tasks);
        stubUsers("u1", "u2", "u3");
        StateMachineResult sm = okSm();
        when(stateMachineService.executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.OVERTIME), any(UserDetail.class)))
                .thenReturn(sm);

        Map<String, Object> result = service.componentStopped(req, caller);

        assertEquals(true, result.get("workerUpdated"));
        assertEquals(true, result.get("clusterStateUpdated"));
        assertEquals(3, result.get("taskRescheduled"));
        verify(stateMachineService, times(3)).executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.OVERTIME), any(UserDetail.class));
        verify(taskScheduleService, times(3)).scheduling(any(TaskDto.class), any(UserDetail.class), eq(true));
        verify(mongoTemplate, times(2)).updateMulti(any(Query.class), any(), anyString());
    }

    @Test
    void engineStop_singleTaskFailureDoesNotAbortRest() {
        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_ENGINE);
        req.setProcessId("engine-A");

        TaskDto t1 = runningTask("u1");
        TaskDto t2 = runningTask("u1");
        TaskDto t3 = runningTask("u1");
        when(taskService.findAll(any(Query.class))).thenReturn(Arrays.asList(t1, t2, t3));
        stubUsers("u1");
        StateMachineResult sm = okSm();
        when(stateMachineService.executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.OVERTIME), any(UserDetail.class)))
                .thenReturn(sm);
        doThrow(new RuntimeException("boom")).when(taskScheduleService)
                .scheduling(same(t2), any(UserDetail.class), eq(true));

        Map<String, Object> result = service.componentStopped(req, caller);

        assertEquals(2, result.get("taskRescheduled"));
        verify(taskScheduleService, times(1)).scheduling(same(t1), any(UserDetail.class), eq(true));
        verify(taskScheduleService, times(1)).scheduling(same(t2), any(UserDetail.class), eq(true));
        verify(taskScheduleService, times(1)).scheduling(same(t3), any(UserDetail.class), eq(true));
    }

    @Test
    void engineStop_noRunningTasks_onlyMarksWorker() {
        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_ENGINE);
        req.setProcessId("engine-A");

        when(taskService.findAll(any(Query.class))).thenReturn(Collections.emptyList());

        Map<String, Object> result = service.componentStopped(req, caller);

        assertEquals(0, result.get("taskRescheduled"));
        verify(stateMachineService, never()).executeAboutTask(any(TaskDto.class), any(), any(UserDetail.class));
        verify(taskScheduleService, never()).scheduling(any(TaskDto.class), any(UserDetail.class), any());
    }

    @Test
    void apiserverStop_doesNotFailoverTasks_butFlipsClusterState() {
        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_APISERVER);
        req.setProcessId("api-A");

        Map<String, Object> result = service.componentStopped(req, caller);

        assertEquals(true, result.get("workerUpdated"));
        assertEquals(true, result.get("clusterStateUpdated"));
        assertEquals(0, result.get("taskRescheduled"));
        verify(taskService, never()).findAll(any(Query.class));
        verify(taskScheduleService, never()).scheduling(any(TaskDto.class), any(UserDetail.class), any());
    }

    @Test
    void frontendStop_onlyFlipsClusterState_noWorkerNoTasks() {
        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_FRONTEND);

        Map<String, Object> result = service.componentStopped(req, caller);

        assertEquals(false, result.get("workerUpdated"));
        assertEquals(true, result.get("clusterStateUpdated"));
        assertEquals(0, result.get("taskRescheduled"));
        verify(taskService, never()).findAll(any(Query.class));
        verify(mongoTemplate, times(1)).updateMulti(any(Query.class), any(), anyString());
    }

    @Test
    void engineStop_singleComponent_doesNotFlipTopStatusOrExpireTtl() {
        // Pre-state: apiServer and management still running. Only engine will become stopped.
        when(mongoTemplate.findOne(any(Query.class), eq(ClusterStateEntity.class), anyString()))
                .thenReturn(clusterStateWithStatuses("running", "running", "running"));

        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_ENGINE);
        req.setProcessId("engine-A");
        when(taskService.findAll(any(Query.class))).thenReturn(Collections.emptyList());

        Map<String, Object> result = service.componentStopped(req, caller);
        assertEquals(true, result.get("clusterStateUpdated"));

        Document set = captureSetClusterStateUpdate();
        assertEquals("stopped", set.get("engine.status"));
        assertNotNull(set.get("last_updated"));
        // Critical: top-level status / ttl must NOT be touched by a single-component stop.
        assertFalse(set.containsKey("status"), "single-component stop must not write top-level status");
        assertFalse(set.containsKey("ttl"), "single-component stop must not expire ttl");
    }

    @Test
    void frontendStop_completesClusterStop_flipsTopStatusAndExpiresTtl() {
        // Pre-state: engine and apiServer already stopped. Frontend stop closes the loop.
        when(mongoTemplate.findOne(any(Query.class), eq(ClusterStateEntity.class), anyString()))
                .thenReturn(clusterStateWithStatuses("stopped", "stopped", "running"));

        ComponentStoppedRequest req = new ComponentStoppedRequest();
        req.setUuid("uuid-1");
        req.setComponent(ComponentStoppedRequest.COMPONENT_FRONTEND);

        long before = System.currentTimeMillis();
        Map<String, Object> result = service.componentStopped(req, caller);
        assertEquals(true, result.get("clusterStateUpdated"));

        Document set = captureSingleClusterStateUpdate();
        assertEquals("stopped", set.get("management.status"));
        assertEquals("stopped", set.get("status"));
        Object ttl = set.get("ttl");
        assertNotNull(ttl, "ttl must be set when all three components are stopped");
        assertTrue(ttl instanceof Date, "ttl must be a Date");
        assertTrue(((Date) ttl).getTime() < before, "ttl must be in the past so stopCluster sees it");
    }

    @Test
    void invalidRequest_throws() {
        ComponentStoppedRequest noUuid = new ComponentStoppedRequest();
        noUuid.setComponent(ComponentStoppedRequest.COMPONENT_FRONTEND);
        assertThrows(IllegalArgumentException.class, () -> service.componentStopped(noUuid, caller));

        ComponentStoppedRequest unknownComp = new ComponentStoppedRequest();
        unknownComp.setUuid("u");
        unknownComp.setComponent("mystery");
        assertThrows(IllegalArgumentException.class, () -> service.componentStopped(unknownComp, caller));

        ComponentStoppedRequest engineNoPid = new ComponentStoppedRequest();
        engineNoPid.setUuid("u");
        engineNoPid.setComponent(ComponentStoppedRequest.COMPONENT_ENGINE);
        assertThrows(IllegalArgumentException.class, () -> service.componentStopped(engineNoPid, caller));
    }
}
