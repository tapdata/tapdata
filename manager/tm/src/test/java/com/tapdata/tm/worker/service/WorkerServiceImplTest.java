package com.tapdata.tm.worker.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
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
            status.getCpuMemStatus().put("id", new MetricInfo());
            status.setWorkerBaseInfo(new HashMap<>());
            status.getWorkerBaseInfo().put("id", new ApiServerWorkerInfo());
            status.getWorkerBaseInfo().get("id").setName("name");
            status.getWorkerBaseInfo().get("id").setOid("oid");
            status.getWorkerBaseInfo().get("id").setId(1);
            status.getWorkerBaseInfo().get("id").setWorkerStartTime(1L);
            status.getWorkerBaseInfo().get("id").setSort(1);
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUsername()).thenReturn("username");
            WorkerServiceImpl workerService = mock(WorkerServiceImpl.class);
            when(workerService.update(any(Query.class), any(Update.class))).thenReturn(null);
            doCallRealMethod().when(workerService).updateWorkerStatus(status, userDetail);
            Assertions.assertDoesNotThrow(() -> workerService.updateWorkerStatus(status, userDetail));
        }
    }
}