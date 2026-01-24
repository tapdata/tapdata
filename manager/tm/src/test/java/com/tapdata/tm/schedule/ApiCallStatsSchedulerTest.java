package com.tapdata.tm.schedule;

import com.tapdata.tm.apiCalls.service.SupplementApiCallServer;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawScheduleExecutor;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.ReflectionUtils;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-08-30 18:28
 **/
@DisplayName("Class ApiCallStatsScheduler Test")
class ApiCallStatsSchedulerTest {

    private ApiCallStatsScheduler apiCallStatsScheduler;

    private WorkerCallServiceImpl workerCallServiceImpl;
    WorkerService workerService;
    SupplementApiCallServer supplementApiCallServer;
    ApiMetricsRawScheduleExecutor service;

    @BeforeEach
    void setUp() {
        apiCallStatsScheduler = mock(ApiCallStatsScheduler.class);
        service = mock(ApiMetricsRawScheduleExecutor.class);
        supplementApiCallServer = mock(SupplementApiCallServer.class);
        doNothing().when(supplementApiCallServer).supplementOnce();
        workerCallServiceImpl = mock(WorkerCallServiceImpl.class);
        workerService = mock(WorkerService.class);
    }

    @Nested
    class scheduleWorkerCallTest {
        @Test
        @DisplayName("test schedule worker call")
        void test1() {
            doNothing().when(workerCallServiceImpl).metric();
            when(workerService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            doNothing().when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(apiCallStatsScheduler::scheduleForApiCall);
            verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
        }

        @Test
        @DisplayName("test schedule worker call")
        void testNull() {
            doNothing().when(workerCallServiceImpl).metric();
            when(workerService.findAll(any(Query.class))).thenReturn(null);
            doNothing().when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(apiCallStatsScheduler::scheduleForApiCall);
            verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
        }

        @Test
        void testException() {
            doAnswer(a -> {
                throw new RuntimeException("test");
            }).when(workerCallServiceImpl).metric();
            when(workerService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            doAnswer(a -> {
                throw new RuntimeException("test");
            }).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(apiCallStatsScheduler::scheduleForApiCall);
            verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
        }

        @Test
        void testException1() {
            doAnswer(a -> {
                throw new RuntimeException("test");
            }).when(workerCallServiceImpl).metric();
            when(workerService.findAll(any(Query.class))).thenAnswer(a -> {
                throw new RuntimeException("test");
            });
            doAnswer(a -> {
                throw new RuntimeException("test");
            }).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(apiCallStatsScheduler::scheduleForApiCall);
            verify(workerCallServiceImpl, times(0)).collectApiCallCountGroupByWorker(anyString());
        }

        @Test
        void testException2() {
            ArrayList<WorkerDto> objects = new ArrayList<>();
            WorkerDto dto = new WorkerDto();
            dto.setProcessId("1");
            objects.add(dto);
            doThrow(new RuntimeException("test")).when(workerCallServiceImpl).metric();
            when(workerService.findAll(any(Query.class))).thenReturn(objects);
            doThrow(new RuntimeException("test")).when(workerCallServiceImpl).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(apiCallStatsScheduler::scheduleForApiCall);
            verify(workerCallServiceImpl, times(objects.size())).collectApiCallCountGroupByWorker(anyString());
        }
    }
}