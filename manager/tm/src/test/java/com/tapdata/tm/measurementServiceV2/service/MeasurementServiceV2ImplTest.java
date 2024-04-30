package com.tapdata.tm.measurementServiceV2.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.service.MeasurementServiceV2Impl;
import com.tapdata.tm.task.service.TaskService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MeasurementServiceV2ImplTest {
    @Nested
    class querySyncByTableNameTest{
        MeasurementServiceV2Impl measurementServiceV2 = mock(MeasurementServiceV2Impl.class);

        @Test
        void testTableNameIsNotNull(){
            TableSyncStaticDto tableSyncStaticDto = new TableSyncStaticDto("test",1,20,"test");
            Criteria criteria = new Criteria();
            doCallRealMethod().when(measurementServiceV2).querySyncByTableName(any(),any());
            measurementServiceV2.querySyncByTableName(tableSyncStaticDto,criteria);
            Assertions.assertTrue(criteria.getCriteriaObject().containsKey("tags.table"));
        }

        @Test
        void testTableNameIsNull(){
            TableSyncStaticDto tableSyncStaticDto = new TableSyncStaticDto("test",1,20,null);
            Criteria criteria = new Criteria();
            doCallRealMethod().when(measurementServiceV2).querySyncByTableName(any(),any());
            measurementServiceV2.querySyncByTableName(tableSyncStaticDto,criteria);
            Assertions.assertFalse(criteria.getCriteriaObject().containsKey("tags.table"));
        }

    }
    @Nested
    class QuerySyncStatic{
        MeasurementServiceV2Impl measurementServiceV2 = mock(MeasurementServiceV2Impl.class);
        MongoTemplate mongoTemplate;
        TaskService taskService;
        @BeforeEach
        void before(){
            mongoTemplate = mock(MongoTemplate.class);
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(measurementServiceV2,"mongoOperations",mongoTemplate);
            ReflectionTestUtils.setField(measurementServiceV2,"taskService",taskService);
        }
        @Test
        void test(){
            TableSyncStaticDto tableSyncStaticDto = new TableSyncStaticDto("test",1,20,"test");
            when(mongoTemplate.count(any(Query.class),anyString())).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                Assertions.assertTrue(query.getQueryObject().containsKey("tags.table"));
                return 0L;
            });
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));
        }

    }
}
