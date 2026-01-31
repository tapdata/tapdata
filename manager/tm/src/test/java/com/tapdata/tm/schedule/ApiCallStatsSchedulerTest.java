package com.tapdata.tm.schedule;

import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawScheduleExecutor;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2024-08-30 18:28
 **/
@DisplayName("Class ApiCallStatsScheduler Test")
class ApiCallStatsSchedulerTest {
    WorkerService workerService;
    ApiMetricsRawScheduleExecutor service;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawScheduleExecutor.class);
        workerService = mock(WorkerService.class);
    }
}