package com.tapdata.tm.apiServer.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

class WorkerCallServiceTest {

    class MockWorkerCallService implements WorkerCallService{}

    @Test
    void findData() {
        MockWorkerCallService service = new MockWorkerCallService();
        List<WorkerCallEntity> data = service.findData(null);
        assertNotNull(data);
        assertEquals(0, data.size());
    }

}