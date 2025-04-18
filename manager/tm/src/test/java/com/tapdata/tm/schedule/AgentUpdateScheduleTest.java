package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;



 class AgentUpdateScheduleTest {

    @Mock
    private ClusterOperationService clusterOperationService;
    @Mock
    private WorkerService workerService;
    @Mock
    private SettingsService settingsService;


//    @Test
//     void testAgentUpdateScheduleCron() {
//        long startTime = System.currentTimeMillis();
//        await().atMost(30, TimeUnit.SECONDS).until(() ->
//                verify(settingsService, times(1)).
//                        getByCategoryAndKey("System", "buildProfile"));
//        long endTime = System.currentTimeMillis();
//        long useTime = endTime-startTime;
//        System.out.println(useTime);
//        assertTrue(useTime < 21000);
//
//    }

    @Configuration
    @EnableScheduling
    static class TestConfig {
        @Bean
        public AgentUpdateSchedule getScheduleTask() {
            return new AgentUpdateSchedule();
        }
    }

}
