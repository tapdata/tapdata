package com.tapdata.tm.schedule.config;

import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiServerAlarmConfigImplTest {
    ApiServerAlarmConfigImpl apiServerAlarmConfig;
    AlarmRuleService alarmRuleService;

    @BeforeEach
    void init() {
        apiServerAlarmConfig = new ApiServerAlarmConfigImpl();
        alarmRuleService = mock(AlarmRuleService.class);
        apiServerAlarmConfig.setAlarmRuleService(alarmRuleService);
    }

    @Nested
    class afterPropertiesSetTest {
        @Test
        void testNormal() {
            when(alarmRuleService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            Assertions.assertDoesNotThrow(apiServerAlarmConfig::afterPropertiesSet);
        }
    }

    @Nested
    class removeTest {
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> apiServerAlarmConfig.remove("id", AlarmKeyEnum.API_SERVER_API_DELAY_P95_ALTER));
        }
        @Test
        void testNullId() {
            Assertions.assertDoesNotThrow(() -> apiServerAlarmConfig.remove(null, AlarmKeyEnum.API_SERVER_API_DELAY_P95_ALTER));
        }
    }
    @Nested
    class configTest {
        @Test
        void testNormal() {
            Assertions.assertNull(apiServerAlarmConfig.config("id", AlarmKeyEnum.API_SERVER_API_DELAY_P95_ALTER));
        }
        @Test
        void testNullId() {
            Assertions.assertNull(apiServerAlarmConfig.config(null, AlarmKeyEnum.API_SERVER_API_DELAY_P95_ALTER));
        }
    }
}