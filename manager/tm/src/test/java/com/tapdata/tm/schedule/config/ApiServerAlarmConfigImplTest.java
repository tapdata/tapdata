package com.tapdata.tm.schedule.config;

import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;

import static org.mockito.Mockito.mock;

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

    }

    @Nested
    class updateConfigTest {

    }
}