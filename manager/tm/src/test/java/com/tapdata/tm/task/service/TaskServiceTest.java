package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.repository.TaskRepository;
import org.apache.poi.ss.formula.functions.T;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
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

    /**
     * 测试传入的Task任务为null
     */
    @Test
    void testCheckIsCronOrPlanTaskWithNullTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        assertThrows(IllegalArgumentException.class,()->{taskService.checkIsCronOrPlanTask(null);});
    }

    /**
     * 测试传入的Task的crontabExpressionFlag属性为 null 的情况
     */
    @Test
    void testCheckIsCronOrPlanTaskWithNullCronTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        TaskDto taskDto=new TaskDto();
        boolean result = taskService.checkIsCronOrPlanTask(taskDto);
        assertEquals(false,result);
    }

    /**
     * 测试传入的Task的planStartDateFlag属性为true的情况
     */
    @Test
    void testCheckIsCronOrPlanTaskWithTruePlanTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        TaskDto taskDto=new TaskDto();
        taskDto.setPlanStartDateFlag(true);
        boolean result = taskService.checkIsCronOrPlanTask(taskDto);
        assertEquals(true,result);
    }

    /**
     * 测试传入的Task的planStartDateFlag属性为false的情况
     */
    @Test
    void testCheckIsCronOrPlanTaskWithFalsePlanTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        TaskDto taskDto = new TaskDto();
        taskDto.setPlanStartDateFlag(false);
        boolean result = taskService.checkIsCronOrPlanTask(taskDto);
        assertEquals(false,result);
    }

    /**
     * 测试传入的Task的crontabExpressionFlag属性为true的情况
     */
    @Test
    void testCheckIsCronOrPlanTaskWithTrueCronTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        TaskDto taskDto=new TaskDto();
        taskDto.setCrontabExpressionFlag(true);
        boolean result = taskService.checkIsCronOrPlanTask(taskDto);
        assertEquals(true,result);
    }

    /**
     * 测试传入的Task的crontabExpressionFlag属性为false的情况
     */
    @Test
    void testCheckIsCronOrPlanTaskWithFalseCronTask(){
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        TaskDto taskDto=new TaskDto();
        taskDto.setCrontabExpressionFlag(false);
        boolean result = taskService.checkIsCronOrPlanTask(taskDto);
        assertEquals(false,result);
    }

}
