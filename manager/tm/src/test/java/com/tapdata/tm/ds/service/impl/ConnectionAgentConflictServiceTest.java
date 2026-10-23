package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.ConnectionAgentConflictReport;
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
        doReturn(true).when(service).canView(any(TaskDto.class), anyString(), eq(user));
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
