package com.tapdata.tm.worker.controller;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class WorkerControllerTest {
    WorkerController workerController;
    WorkerService workerService;
    @Nested
    class TestQueryAllBindWorker{
        @Test
        void testQueryAllBindWorkerSimple(){
            workerService = mock(WorkerService.class);
            workerController = spy(new WorkerController(workerService,mock(UserLogService.class),mock(SettingsService.class)));
            List<Worker> workers = mock(ArrayList.class);
            when(workerService.queryAllBindWorker()).thenReturn(workers);
            ResponseMessage<List<Worker>> actual = workerController.queryAllBindWorker();
            assertEquals(workers,actual.getData());
        }
    }
    @Nested
    class TestUnbindByProcessId{
        @Test
        void testUnbindByProcessId(){
            workerService = mock(WorkerService.class);
            workerController = spy(new WorkerController(workerService,mock(UserLogService.class),mock(SettingsService.class)));
            String processId ="111";
            when(workerService.unbindByProcessId(processId)).thenReturn(true);
            ResponseMessage<Boolean> actual = workerController.unbindByProcessId(processId);
            assertEquals(true,actual.getData());
        }
    }
}
