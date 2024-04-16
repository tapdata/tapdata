package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.shareCdcTableMetrics.service.ShareCdcTableMetricsService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskConsoleServiceImplTest {
    @Nested
    class GetRelationTasksTest{
        TaskService taskService;
        ShareCdcTableMetricsService shareCdcTableMetricsService;

        TaskConsoleServiceImpl taskConsoleService = new TaskConsoleServiceImpl();
        @BeforeEach
        void before(){
            taskService = mock(TaskService.class);
            shareCdcTableMetricsService = mock(ShareCdcTableMetricsService.class);
            ReflectionTestUtils.setField(taskConsoleService,"taskService",taskService);
            ReflectionTestUtils.setField(taskConsoleService,"shareCdcTableMetricsService",shareCdcTableMetricsService);
        }
        @Test
        void test(){
            RelationTaskRequest inputRequest = new RelationTaskRequest();
            inputRequest.setTaskId(new ObjectId().toHexString());
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setConnectionId("test");
            when(dag.getSources()).thenReturn(Arrays.asList(databaseNode));
            when(taskService.findById(any())).thenReturn(taskDto);
            List<TaskDto> taskDtoList = new ArrayList<>();
            TaskDto t = new TaskDto();
            t.setId(new ObjectId());
            t.setName("test1");
            t.setStatus("test");
            t.setType("test");
            t.setSyncType("test");
            t.setStartTime(null);
            TaskDto t1 = new TaskDto();
            t1.setId(new ObjectId());
            t1.setName("test1");
            t1.setStatus("test");
            t1.setType("test");
            t1.setSyncType("test");
            t1.setStartTime(new Date());
            taskDtoList.add(t);
            taskDtoList.add(t1);
            when(taskService.findAll(any(Query.class))).thenReturn(taskDtoList);
            List<RelationTaskInfoVo> result = taskConsoleService.getRelationTasks(inputRequest);
            Assertions.assertNull(result.get(1).getStartTime());
        }

    }
}
