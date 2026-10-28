package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
import com.tapdata.tm.ds.dto.ConnectionTaskAgentRequest;
import com.tapdata.tm.ds.dto.ConnectionTaskAgentResult;
import com.tapdata.tm.ds.dto.BatchUpdateAgentSettingsRequest;
import com.tapdata.tm.task.entity.TaskEntity;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.ds.exception.ConnectionAgentConflictException;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConnectionAgentConflictServiceTest {
    ConnectionAgentConflictService service;
    TaskService tasks;
    InspectService inspections;
    UserDetail user;
    DataSourceConnectionDto source, target;

    @BeforeEach
    void setup() {
        service = spy(new ConnectionAgentConflictService());
        tasks = mock(TaskService.class);
        inspections = mock(InspectService.class);
        user = mock(UserDetail.class);
        when(user.getCustomerId()).thenReturn("tenant");
        source = connection("source", "agent-a");
        target = connection("target", "agent-a");
        DataSourceService sources = mock(DataSourceService.class);
        when(sources.findAll(any(Query.class))).thenReturn(List.of(source, target));
        TaskScheduleServiceImpl scheduler = new TaskScheduleServiceImpl();
        scheduler.setDataSourceService(sources);
        service.setTaskService(tasks);
        service.setTaskScheduleService(scheduler);
        service.setInspectService(inspections);
        service.setDataSourceService(sources);
        doReturn(true).when(service).canViewConnection(anyString(), eq(user));
        doReturn(false).when(service).canEditInspection(any(TaskDto.class), eq(user));
        doReturn(true).when(service).canView(any(TaskDto.class), anyString(), eq(user));
        doReturn(true).when(service).canEdit(any(TaskDto.class), eq(user));
    }

    @Test
    void predictsAutomaticResolutionWithoutMutatingTaskAndExplainsPermission() {
        TaskDto task = task("manual", "stop", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        ConnectionAgentConflictReport.Item item = service.findConflicts(both("agent-b"), user, 0, 20).getItems().get(0);
        assertTrue(item.isAutoResolvable());
        assertTrue(item.isCanEdit());
        assertNull(item.getBlockedReason());
        assertEquals("agent-a", task.getAccessNodeProcessId());
        doReturn(false).when(service).canEdit(task, user);
        item = service.findConflicts(both("agent-b"), user, 0, 20).getItems().get(0);
        assertFalse(item.isAutoResolvable());
        assertEquals("no_edit_permission", item.getBlockedReason());
    }

    @Test
    void distinguishesConnectionConflictAndActiveTaskFromAutomaticResolution() {
        TaskDto task = task("manual", "stop", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        ConnectionAgentConflictReport.Item item = service.findConflicts(
                Map.of(source.getId().toHexString(), changed(source, "agent-b")), user, 0, 20).getItems().get(0);
        assertFalse(item.isAutoResolvable());
        assertEquals("connection_conflict", item.getBlockedReason());
        assertEquals(2, item.getConnections().size());
        doReturn(false).when(service).canViewConnection(eq(target.getId().toHexString()), eq(user));
        item = service.findConflicts(Map.of(source.getId().toHexString(), changed(source, "agent-b")), user, 0, 20).getItems().get(0);
        assertEquals(1, item.getConnections().size());
        task.setStatus("running");
        item = service.findConflicts(both("agent-b"), user, 0, 20).getItems().get(0);
        assertFalse(item.isEditable());
        assertFalse(item.isAutoResolvable());
        assertEquals("task_active", item.getBlockedReason());
    }

    private ConnectionTaskAgentRequest edits(TaskDto task, String agent) {
        ConnectionTaskAgentRequest request = new ConnectionTaskAgentRequest();
        ConnectionTaskAgentRequest.TaskEdit edit = new ConnectionTaskAgentRequest.TaskEdit();
        edit.setId(task.getId().toHexString());
        edit.setRevision(task.getLastUpdAt() == null ? null : task.getLastUpdAt().getTime());
        request.setTaskEdits(List.of(edit));
        BatchUpdateAgentSettingsRequest.AgentSettings settings = new BatchUpdateAgentSettingsRequest.AgentSettings();
        settings.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        settings.setAccessNodeProcessId(agent);
        request.setTaskSettings(settings);
        return request;
    }

    @Test
    void savesStoppedTaskUsingProposedConnectionPolicyBeforeConnectionSave() {
        TaskDto task = task("stopped", "stop", "agent-a");
        task.setLastUpdAt(new Date(1234));
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        MongoTemplate mongo = mock(MongoTemplate.class);
        service.setMongoTemplate(mongo);
        when(mongo.updateFirst(any(Query.class), any(Update.class), eq(TaskEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        ConnectionTaskAgentResult result = service.updateTaskAgents(both("agent-b"), edits(task, "agent-b"), user);
        assertEquals(List.of(task.getId().toHexString()), result.getUpdatedTaskIds());
        assertTrue(result.getFailures().isEmpty());
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongo).updateFirst(query.capture(), update.capture(), eq(TaskEntity.class));
        assertTrue(query.getValue().getQueryObject().containsKey("status"));
        assertTrue(query.getValue().getQueryObject().containsKey("lastUpdAt"));
        assertEquals("agent-b", ((org.bson.Document) update.getValue().getUpdateObject().get("$set")).get("accessNodeProcessId"));
        assertEquals("agent-a", task.getAccessNodeProcessId());
    }

    @Test
    void rejectsRunningPermissionDeniedStaleAndConflictingEditsWithoutWriting() {
        MongoTemplate mongo = mock(MongoTemplate.class);
        service.setMongoTemplate(mongo);
        TaskDto task = task("task", "running", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        assertEquals(1, service.updateTaskAgents(both("agent-b"), edits(task, "agent-b"), user).getFailures().size());
        task.setStatus("stop");
        doReturn(false).when(service).canEdit(task, user);
        assertEquals(1, service.updateTaskAgents(both("agent-b"), edits(task, "agent-b"), user).getFailures().size());
        doReturn(true).when(service).canEdit(task, user);
        ConnectionTaskAgentRequest request = edits(task, "agent-b");
        request.getTaskEdits().get(0).setRevision(123L);
        assertEquals(1, service.updateTaskAgents(both("agent-b"), request, user).getFailures().size());
        assertEquals(1, service.updateTaskAgents(both("agent-b"), edits(task, "agent-a"), user).getFailures().size());
        verifyNoInteractions(mongo);
    }

    @Test
    void partialBatchReturnsSuccessfulAndFailedTaskIdsSeparately() {
        TaskDto first = task("first", "stop", "agent-a");
        TaskDto second = task("second", "running", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(first), List.of(second));
        MongoTemplate mongo = mock(MongoTemplate.class);
        service.setMongoTemplate(mongo);
        when(mongo.updateFirst(any(Query.class), any(Update.class), eq(TaskEntity.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        ConnectionTaskAgentRequest request = edits(first, "agent-b");
        request.setTaskEdits(List.of(request.getTaskEdits().get(0), edits(second, "agent-b").getTaskEdits().get(0)));
        ConnectionTaskAgentResult result = service.updateTaskAgents(both("agent-b"), request, user);
        assertEquals(List.of(first.getId().toHexString()), result.getUpdatedTaskIds());
        assertEquals(second.getId().toHexString(), result.getFailures().get(0).getId());
        verify(mongo, times(1)).updateFirst(any(Query.class), any(Update.class), eq(TaskEntity.class));
    }

    @Test
    void reportsConcurrentUpdateInsteadOfClaimingSuccess() {
        TaskDto task = task("task", "stop", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        MongoTemplate mongo = mock(MongoTemplate.class);
        service.setMongoTemplate(mongo);
        when(mongo.updateFirst(any(Query.class), any(Update.class), eq(TaskEntity.class)))
                .thenReturn(UpdateResult.acknowledged(0, 0L, null));
        ConnectionTaskAgentResult result = service.updateTaskAgents(both("agent-b"), edits(task, "agent-b"), user);
        assertTrue(result.getUpdatedTaskIds().isEmpty());
        assertEquals(1, result.getFailures().size());
    }

    @Test
    void listsOnlyConflictingTasksAndKeepsStoppedTasksAndOriginalPolicies() {
        TaskDto stopped = task("stopped-task", "stopped", "agent-a");
        TaskDto compatible = task("compatible", "running", "agent-b");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(stopped, compatible));
        Map<String, DataSourceConnectionDto> changes = both("agent-b");
        ConnectionAgentConflictReport report = service.findConflicts(changes, user, 0, 20);
        assertTrue(report.isHasConflicts());
        assertEquals(1, report.getTotal());
        assertEquals("stopped-task", report.getItems().get(0).getName());
        assertEquals("Task.AgentConflict", report.getItems().get(0).getCode());
        assertEquals(2, report.getItems().get(0).getConnections().size());
        assertEquals("agent-a", stopped.getAccessNodeProcessId());
        assertEquals("agent-a", source.getAccessNodeProcessId());
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(tasks).findAll(query.capture());
        assertTrue(query.getValue().getQueryObject().toJson().contains("tenant"));
        assertEquals(200, query.getValue().getLimit());
    }

    @Test
    void evaluatesAllProposedConnectionsTogether() {
        TaskDto automatic = task("automatic", "running", null);
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(automatic));
        assertTrue(service.findConflicts(Map.of(source.getId().toHexString(), changed(source, "agent-b")), user, 0, 20).isHasConflicts());
        assertFalse(service.findConflicts(both("agent-b"), user, 0, 20).isHasConflicts());
    }

    @Test
    void hiddenConflictsBlockSavingWithoutExposingTaskDetails() {
        TaskDto secret = task("secret-name", "running", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(secret));
        doReturn(false).when(service).canView(any(TaskDto.class), anyString(), eq(user));
        ConnectionAgentConflictException exception = assertThrows(ConnectionAgentConflictException.class,
                () -> service.assertNoConflicts(both("agent-b"), user));
        assertTrue(exception.getReport().isHasConflicts());
        assertEquals(1, exception.getReport().getHiddenCount());
        assertEquals(0, exception.getReport().getTotal());
        assertTrue(exception.getReport().getItems().isEmpty());
        assertFalse(exception.getReport().toString().contains(secret.getId().toHexString()));
        assertFalse(exception.getReport().toString().contains("secret-name"));
    }

    @Test
    void paginatesWithoutStoppingTheConflictScanAtTheFirstPage() {
        List<TaskDto> firstBatch = new ArrayList<>();
        for (int i = 0; i < 200; i++) firstBatch.add(task("task-" + i, "stopped", "agent-a"));
        TaskDto last = task("last", "running", "agent-a");
        when(tasks.findAll(any(Query.class))).thenReturn(firstBatch, List.of(last));
        ConnectionAgentConflictReport report = service.findConflicts(both("agent-b"), user, 200, 20);
        assertEquals(201, report.getTotal());
        assertEquals(1, report.getItems().size());
        assertEquals("last", report.getItems().get(0).getName());
        verify(tasks, times(2)).findAll(any(Query.class));
    }

    @Test
    void detectsIndependentInspectionConnectionConflicts() {
        InspectDto inspect = new InspectDto();
        inspect.setId(new ObjectId());
        inspect.setName("inspection");
        inspect.setStatus("stopped");
        com.tapdata.tm.inspect.bean.Task pair = new com.tapdata.tm.inspect.bean.Task();
        Source left = new Source(); left.setConnectionId(source.getId().toHexString());
        Source right = new Source(); right.setConnectionId(target.getId().toHexString());
        pair.setSource(left); pair.setTarget(right);
        inspect.setTasks(List.of(pair));
        when(inspections.findAll(any(Query.class))).thenReturn(List.of(inspect));
        ConnectionAgentConflictReport report = service.findConflicts(Map.of(source.getId().toHexString(), changed(source, "agent-b")), user, 0, 20);
        assertEquals(1, report.getTotal());
        assertEquals("inspect", report.getItems().get(0).getSyncType());
        assertEquals("Task.SourceAgentConflict", report.getItems().get(0).getCode());
    }

    @Test
    void noChangeAndSharedCacheDoNotProduceConflicts() {
        assertFalse(service.findConflicts(Map.of(), user, 0, 20).isHasConflicts());
        verifyNoInteractions(tasks, inspections);
        TaskDto cache = task("cache", "running", "agent-a");
        cache.setShareCache(true);
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(cache));
        assertFalse(service.findConflicts(both("agent-b"), user, 0, 20).isHasConflicts());
    }

    @Test
    void rejectsInvalidPagination() {
        assertThrows(RuntimeException.class, () -> service.findConflicts(both("agent-b"), user, -1, 20));
        assertThrows(RuntimeException.class, () -> service.findConflicts(both("agent-b"), user, 0, 101));
    }

    private Map<String, DataSourceConnectionDto> both(String agent) {
        return Map.of(source.getId().toHexString(), changed(source, agent), target.getId().toHexString(), changed(target, agent));
    }
    private DataSourceConnectionDto connection(String name, String agent) {
        DataSourceConnectionDto value = new DataSourceConnectionDto();
        value.setId(new ObjectId()); value.setName(name);
        value.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        value.setAccessNodeProcessId(agent);
        return value;
    }
    private DataSourceConnectionDto changed(DataSourceConnectionDto old, String agent) {
        DataSourceConnectionDto value = connection(old.getName(), agent); value.setId(old.getId()); return value;
    }
    private TaskDto task(String name, String status, String agent) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId()); task.setName(name); task.setStatus(status); task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAccessNodeType(agent == null ? AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name() : AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        task.setAccessNodeProcessId(agent);
        TableNode left = new TableNode(); left.setConnectionId(source.getId().toHexString());
        TableNode right = new TableNode(); right.setConnectionId(target.getId().toHexString());
        io.github.openlg.graphlib.Graph<Node, com.tapdata.tm.commons.dag.Edge> graph = new io.github.openlg.graphlib.Graph<>();
        graph.setNode("left", left); graph.setNode("right", right); task.setDag(new DAG(graph));
        return task;
    }
}
