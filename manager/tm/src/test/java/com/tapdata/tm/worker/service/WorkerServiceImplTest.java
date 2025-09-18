package com.tapdata.tm.worker.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.vo.WorkerOrServerStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashMap;

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
            status.setWorkerStatus(new HashMap<>());
            status.getWorkerStatus().put("id", "ok");
            status.setCpuMemStatus(new HashMap<>());
            status.getCpuMemStatus().put("id", new HashMap<>());
            status.setWorkerBaseInfo(new HashMap<>());
            status.getWorkerBaseInfo().put("id", new HashMap<>());
            status.getWorkerBaseInfo().get("id").put("name", "name");
            status.getWorkerBaseInfo().get("id").put("oid", "oid");
            status.getWorkerBaseInfo().get("id").put("id", 1);
            status.getWorkerBaseInfo().get("id").put("worker_start_time", 1L);
            status.getWorkerBaseInfo().get("id").put("sort", 1);
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUsername()).thenReturn("username");
            WorkerServiceImpl workerService = mock(WorkerServiceImpl.class);
            when(workerService.update(any(Query.class), any(Update.class))).thenReturn(null);
            doCallRealMethod().when(workerService).updateWorkerStatus(status, userDetail);
            Assertions.assertDoesNotThrow(() -> workerService.updateWorkerStatus(status, userDetail));
        }
    }
}