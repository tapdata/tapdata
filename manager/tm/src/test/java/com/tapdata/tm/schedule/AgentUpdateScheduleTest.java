package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@SpringBootTest( classes = {AgentUpdateScheduleTest.class, AgentUpdateScheduleTest.TestConfig.class} )
public class AgentUpdateScheduleTest {

    @MockBean
    private ClusterOperationService clusterOperationService;
    @MockBean
    private WorkerService workerService;
    @MockBean
    private SettingsService settingsService;


    @Test
    public void testAgentUpdateScheduleCron() {
        try {
            Thread.sleep(10000L);
            verify(clusterOperationService, times(1)).sendOperation();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Configuration
    @EnableScheduling
    public static class TestConfig {
        @Bean
        public AgentUpdateSchedule getScheduleTask() {
            return new AgentUpdateSchedule();
        }
    }

}
