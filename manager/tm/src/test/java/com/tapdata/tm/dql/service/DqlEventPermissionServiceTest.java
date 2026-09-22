package com.tapdata.tm.dql.service;

import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventPermissionServiceTest {
    private static final String TASK_ID = "64f000000000000000000001";
    private static final String OTHER_TASK_ID = "64f000000000000000000002";

    @Test
    void resolvesOnlyIdsFromTheUserScopedTaskQuery() {
        TaskService taskService = mock(TaskService.class);
        UserDetail user = user();
        TaskEntity visible = task(TASK_ID);
        TaskEntity other = task(OTHER_TASK_ID);
        when(taskService.findAll(any(Query.class), eq(user))).thenReturn(List.of(visible, other));
        DqlEventPermissionService permissionService = permissionService(taskService);

        Set<String> taskIds = permissionService.resolveVisibleTaskIds(new DqlEventQueryVo(), user);

        assertEquals(Set.of(TASK_ID, OTHER_TASK_ID), taskIds);
        verify(taskService).findAll(any(Query.class), eq(user));
    }

    @Test
    void taskQualifiedQueryChecksVisibilityWithoutEnumeratingTasks() {
        TaskService taskService = mock(TaskService.class);
        UserDetail user = user();
        DqlEventPermissionService permissionService = new DqlEventPermissionService(taskService) {
            @Override
            public void checkMenuVisible(UserDetail ignored) {
            }

            @Override
            public void checkTaskVisible(String taskId, UserDetail ignored) {
                assertEquals(TASK_ID, taskId);
            }
        };
        DqlEventQueryVo query = new DqlEventQueryVo();
        query.setTaskId(TASK_ID);

        assertEquals(Set.of(TASK_ID), permissionService.resolveVisibleTaskIds(query, user));
        verify(taskService, never()).findAll(any(Query.class), any(UserDetail.class));
    }

    @Test
    void returnsEmptyScopeWhenNoTaskIsVisible() {
        TaskService taskService = mock(TaskService.class);
        UserDetail user = user();
        when(taskService.findAll(any(Query.class), eq(user))).thenReturn(Collections.emptyList());
        DqlEventPermissionService permissionService = permissionService(taskService);

        assertEquals(Collections.emptySet(), permissionService.resolveVisibleTaskIds(null, user));
    }

    private DqlEventPermissionService permissionService(TaskService taskService) {
        return new DqlEventPermissionService(taskService) {
            @Override
            public void checkMenuVisible(UserDetail ignored) {
            }
        };
    }

    private TaskEntity task(String id) {
        TaskEntity task = new TaskEntity();
        task.setId(new ObjectId(id));
        return task;
    }

    private UserDetail user() {
        return new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
    }
}
