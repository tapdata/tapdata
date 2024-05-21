package com.tapdata.tm.task.service;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.utils.Lists;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;

import static com.sun.tools.doclint.Entity.times;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class TaskErrorEventServiceTest {
    TaskErrorEventService taskErrorEventService;
    TaskRepository taskRepository;
    TaskService taskService;
    UserDetail user;

    @BeforeEach
    void init() {
        user = mock(UserDetail.class);
        taskErrorEventService = new TaskErrorEventService();
        taskRepository = mock(TaskRepository.class);
        taskService = mock(TaskService.class);

        taskErrorEventService.setTaskRepository(taskRepository);
        taskErrorEventService.setTaskService(taskService);
    }

    @Nested
    class GetErrorEventByTaskIdTest {
        TaskDto taskDto;
        @BeforeEach
        void init() {
            taskDto = mock(TaskDto.class);
            when(taskDto.getErrorEvents()).thenReturn(new ArrayList<>());
        }

        @Test
        void testNormal() {
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            List<ErrorEvent> errorEventByTaskId = taskErrorEventService.getErrorEventByTaskId(new ObjectId().toHexString(), user);
            Assertions.assertNotNull(errorEventByTaskId);
        }

        @Test
        void testTaskDtoIsNull() {
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(null);
            List<ErrorEvent> errorEventByTaskId = taskErrorEventService.getErrorEventByTaskId(new ObjectId().toHexString(), user);
            Assertions.assertNotNull(errorEventByTaskId);
        }
    }

    @Nested
    class SignSkipErrorEventsTest {
        BulkOperations bulkOperations;
        @BeforeEach
        void init() {
            bulkOperations = mock(BulkOperations.class);
            when(taskService.update(any(Query.class), any(Update.class))).thenReturn(mock(UpdateResult.class));
            when(taskRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
            when(bulkOperations.updateMulti(any(Query.class), any(Update.class))).thenReturn(bulkOperations);
            when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
        }
        @Test
        void testNormal() {
            taskErrorEventService.signSkipErrorEvents(new ObjectId().toHexString(), Lists.newArrayList(new ObjectId().toHexString()));
            verify(taskService, times(0)).update(any(Query.class), any(Update.class));
            verify(taskRepository, times(1)).bulkOperations(BulkOperations.BulkMode.UNORDERED);
            verify(bulkOperations, times(2)).updateMulti(any(Query.class), any(Update.class));
            verify(bulkOperations, times(1)).execute();
        }
        @Test
        void testEmpty() {
            taskErrorEventService.signSkipErrorEvents(new ObjectId().toHexString(), Lists.newArrayList());
            verify(taskService, times(1)).update(any(Query.class), any(Update.class));
            verify(taskRepository, times(0)).bulkOperations(BulkOperations.BulkMode.UNORDERED);
            verify(bulkOperations, times(0)).updateMulti(any(Query.class), any(Update.class));
            verify(bulkOperations, times(0)).execute();
        }
    }
}