package com.tapdata.tm.measurementServiceV2.service;

import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.service.MeasurementServiceV2Impl;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.core.query.Criteria;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MeasurementServiceV2ImplTest {
    @Nested
    class querySyncByTableNameTest{
        MeasurementServiceV2Impl measurementServiceV2 = mock(MeasurementServiceV2Impl.class);

        @Test
        void testTableNameIsNotNull(){
            TableSyncStaticDto tableSyncStaticDto = new TableSyncStaticDto("test",1,20,"test", null);
            Criteria criteria = new Criteria();
            doCallRealMethod().when(measurementServiceV2).querySyncByTableName(any(),any());
            measurementServiceV2.querySyncByTableName(tableSyncStaticDto,criteria);
            Assertions.assertTrue(criteria.getCriteriaObject().containsKey("tags.table"));
        }

        @Test
        void testTableNameIsNull(){
            TableSyncStaticDto tableSyncStaticDto = new TableSyncStaticDto("test",1,20,null, null);
            Criteria criteria = new Criteria();
            doCallRealMethod().when(measurementServiceV2).querySyncByTableName(any(),any());
            measurementServiceV2.querySyncByTableName(tableSyncStaticDto,criteria);
            Assertions.assertFalse(criteria.getCriteriaObject().containsKey("tags.table"));
        }

    }
}
