package com.tapdata.tm.report.schedule;

import com.tapdata.tm.report.dto.RunDaysBatch;
import com.tapdata.tm.report.service.UserDataReportService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RunDaysScheduleTest {
    @Nested
    class scheduleTest{
        private RunDaysSchedule runDaysSchedule = new RunDaysSchedule();
        @Test
        void testNormal(){
            UserDataReportService userDataReportService = mock(UserDataReportService.class);
            ReflectionTestUtils.setField(runDaysSchedule,"userDataReportService",userDataReportService);
            Thread testThread = new Thread(()->{
                runDaysSchedule.schedule();
                String name = Thread.currentThread().getName();
                assertEquals("RunDaysSchedule-schedule",name);
                verify(userDataReportService,new Times(1)).produceData(any(RunDaysBatch.class));
            });
            testThread.start();
        }
    }
}
