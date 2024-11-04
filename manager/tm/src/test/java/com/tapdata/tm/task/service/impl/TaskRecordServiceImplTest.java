package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.task.bean.TaskRecordDto;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.vo.TaskRecordListVo;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class TaskRecordServiceImplTest {
    TaskRecordServiceImpl taskRecordService;
    MongoTemplate mongoTemplate;
    UserService userService;
    @BeforeEach
    void init(){
        mongoTemplate = mock(MongoTemplate.class);
        taskRecordService = new TaskRecordServiceImpl();
        ReflectionTestUtils.setField(taskRecordService,"mongoTemplate",mongoTemplate);
        userService = mock(UserService.class);
        ReflectionTestUtils.setField(taskRecordService,"userService",userService);
    }

    @Nested
    class CleanTaskRecordTest{

        @Test
        void test_main(){
            taskRecordService.cleanTaskRecord("test");
            Query query = new Query(Criteria.where("taskId").is("test"));
            verify(mongoTemplate,times(1)).remove(eq(query), eq(TaskRecord.class));
        }
    }

    @Nested
    class QueryRecordsTest{

        @Test
        void test_main(){
            TaskRecordDto dto = new TaskRecordDto();
            dto.setTaskId("test");
            dto.setPage(1);
            dto.setSize(10);
            when(mongoTemplate.count(eq(new Query(Criteria.where("taskId").is("test"))),eq(TaskRecord.class))).thenReturn(2L);
            List<TaskRecord> taskRecords = new ArrayList<>();
            when(mongoTemplate.find(any(),eq(TaskRecord.class))).thenReturn(taskRecords);
            Page<TaskRecordListVo> result =  taskRecordService.queryRecords(dto);
            Assertions.assertEquals(2L,result.getTotal());
        }

    }
}
