package com.tapdata.tm.dql.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.vo.DqlTaskImpactRequestVo;
import com.tapdata.tm.dql.vo.DqlTaskImpactVo;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DqlTaskImpactServiceTest {

    @Mock
    private TaskService taskService;
    @Mock
    private DqlEventRepository eventRepository;
    @Mock
    private UserDetail userDetail;

    @Test
    void returnsCurrentTaskImpactAndKeepsRequestOrder() {
        String firstTaskId = "64f000000000000000000001";
        String secondTaskId = "64f000000000000000000002";
        TaskEntity firstTask = task(firstTaskId, 4L);
        TaskEntity secondTask = task(secondTaskId, 8L);
        when(taskService.findAll(any(Query.class), eq(userDetail))).thenReturn(List.of(firstTask, secondTask));
        when(eventRepository.countByTaskIdAndVersion(Map.of(firstTaskId, 4L, secondTaskId, 8L)))
                .thenReturn(Map.of(firstTaskId, 3L));

        DqlTaskImpactRequestVo request = new DqlTaskImpactRequestVo();
        request.setTaskIds(List.of(secondTaskId, firstTaskId, secondTaskId));

        List<DqlTaskImpactVo> result = new DqlTaskImpactService(taskService, eventRepository)
                .check(request, userDetail);

        assertEquals(List.of(secondTaskId, firstTaskId), result.stream().map(DqlTaskImpactVo::getTaskId).toList());
        assertTrue(result.get(0).isExists());
        assertEquals(0L, result.get(0).getCount());
        assertTrue(result.get(1).isExists());
        assertEquals(3L, result.get(1).getCount());

        ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
        verify(taskService).findAll(queryCaptor.capture(), eq(userDetail));
        String query = queryCaptor.getValue().getQueryObject().toJson();
        assertTrue(query.contains("64f000000000000000000001"));
        assertTrue(query.contains("64f000000000000000000002"));
        assertTrue(query.contains("is_deleted"));
    }

    @Test
    void returnsMissingAndInvalidTasksAsNotExisting() {
        String existingTaskId = "64f000000000000000000001";
        when(taskService.findAll(any(Query.class), eq(userDetail))).thenReturn(List.of(task(existingTaskId, 1L)));
        when(eventRepository.countByTaskIdAndVersion(Map.of(existingTaskId, 1L))).thenReturn(Map.of());

        DqlTaskImpactRequestVo request = new DqlTaskImpactRequestVo();
        request.setTaskIds(List.of("not-an-object-id", "64f000000000000000000099", existingTaskId));

        List<DqlTaskImpactVo> result = new DqlTaskImpactService(taskService, eventRepository)
                .check(request, userDetail);

        assertEquals(3, result.size());
        assertFalse(result.get(0).isExists());
        assertEquals(0L, result.get(0).getCount());
        assertFalse(result.get(1).isExists());
        assertTrue(result.get(2).isExists());
        assertEquals(0L, result.get(2).getCount());
    }

    @Test
    void emptyRequestDoesNotQueryTasksOrEvents() {
        DqlTaskImpactRequestVo request = new DqlTaskImpactRequestVo();
        request.setTaskIds(List.of());

        List<DqlTaskImpactVo> result = new DqlTaskImpactService(taskService, eventRepository)
                .check(request, userDetail);

        assertTrue(result.isEmpty());
    }

    private TaskEntity task(String id, long version) {
        TaskEntity task = new TaskEntity();
        task.setId(new ObjectId(id));
        task.setVersion(version);
        return task;
    }
}
