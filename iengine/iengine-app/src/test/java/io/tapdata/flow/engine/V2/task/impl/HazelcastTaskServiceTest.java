package io.tapdata.flow.engine.V2.task.impl;

import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.common.SettingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class HazelcastTaskServiceTest {
    @Nested
    class GetTaskRetryConfigTest{
        private HazelcastTaskService hazelcastTaskService;
        @BeforeEach
        void setUp(){
            HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
            hazelcastTaskService = spy(new HazelcastTaskService(clientMongoOperator));
        }
        @DisplayName("test get task retry config default")
        @Test
        void test1(){
            SettingService settingService = mock(SettingService.class);
            when(settingService.getLong(eq("retry_interval_second"), eq(60L))).thenReturn(60L);
            when(settingService.getLong(eq("max_retry_time_minute"), eq(60L))).thenReturn(60L);
            ReflectionTestUtils.setField(hazelcastTaskService, "settingService", settingService);
            TaskRetryConfig taskRetryConfig = hazelcastTaskService.getTaskRetryConfig();
            assertEquals(60L, taskRetryConfig.getRetryIntervalSecond());
            assertEquals(60L * 60, taskRetryConfig.getMaxRetryTimeSecond());
        }
    }
}
