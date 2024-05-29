package com.tapdata.tm.inspect.controller;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.inspect.service.InspectTaskService;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import static com.sun.tools.doclint.Entity.times;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class InspectControllerTest {
    InspectController controller;
    InspectService inspectService;
    InspectTaskService inspectTaskService;
    TaskService taskService;
    @BeforeEach
    void init() {
        controller = mock(InspectController.class);
        inspectService = mock(InspectService.class);
        inspectTaskService = mock(InspectTaskService.class);
        taskService = mock(TaskService.class);
        doCallRealMethod().when(controller).setInspectService(inspectService);
        doCallRealMethod().when(controller).setInspectTaskService(inspectTaskService);
        doCallRealMethod().when(controller).setTaskService(taskService);
        controller.setInspectService(inspectService);
        controller.setInspectTaskService(inspectTaskService);
        controller.setTaskService(taskService);
    }
    @Nested
    class findTaskDtoTest {
        @BeforeEach
        void init() {
            when(controller.findTaskDto(anyString())).thenCallRealMethod();
            when(controller.findTaskDto(null)).thenCallRealMethod();
            when(taskService.findOne(any(Query.class))).thenReturn(new TaskDto());
        }
        @Test
        void testNormal() {
            Assertions.assertNotNull(controller.findTaskDto(new ObjectId().toHexString()));
            verify(taskService).findOne(any(Query.class));
        }
        @Test
        void testNull() {
            Assertions.assertNotNull(controller.findTaskDto("id"));
            verify(taskService, times(0)).findOne(any(Query.class));
        }
    }
}