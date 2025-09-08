package com.tapdata.tm.worker.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.vo.WorkerOrServerStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerServiceImplTest {


    @Nested
    class updateWorkerStatusTest {

        @Test
        void testNoral() {
            WorkerOrServerStatus status = new WorkerOrServerStatus();
            status.setStatus("ok");
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUsername()).thenReturn("username");
            WorkerServiceImpl workerService = mock(WorkerServiceImpl.class);
            when(workerService.update(any(Query.class), any(Update.class))).thenReturn(null);
            doCallRealMethod().when(workerService).updateWorkerStatus(status, userDetail);
            Assertions.assertDoesNotThrow(() -> workerService.updateWorkerStatus(status, userDetail));
        }
    }
}