package com.tapdata.tm.task.service;

import com.tapdata.tm.task.repository.TaskRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class TaskServiceTest {
    private TaskService taskService;
    @Test
    void testFindRunningTasksByAgentIdWithoutId(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->taskService.findRunningTasksByAgentId(processId));
    }
    @Test
    void testFindRunningTasksByAgentIdWithId(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        String processId = "111";
        Query query = Query.query(Criteria.where("agentId").is(processId).and("status").is("running"));
        when(taskService.findAll(query)).thenReturn(new ArrayList<>());
        int actual = taskService.findRunningTasksByAgentId(processId);
        assertEquals(0,actual);
    }
}
