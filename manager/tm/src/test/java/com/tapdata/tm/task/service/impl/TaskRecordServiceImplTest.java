package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.task.entity.TaskRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;

public class TaskRecordServiceImplTest {
    @Nested
    class CleanTaskRecordTest{
        TaskRecordServiceImpl taskRecordService;
        MongoTemplate mongoTemplate;
        @BeforeEach
        void init(){
            mongoTemplate = mock(MongoTemplate.class);
            taskRecordService = new TaskRecordServiceImpl();
            ReflectionTestUtils.setField(taskRecordService,"mongoTemplate",mongoTemplate);
        }

        @Test
        void test_main(){
            taskRecordService.cleanTaskRecord("test");
            Query query = new Query(Criteria.where("taskId").is("test"));
            verify(mongoTemplate,times(1)).remove(eq(query), eq(TaskRecord.class));
        }
    }
}
