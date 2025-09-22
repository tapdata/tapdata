package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApiServerAlarmConfigImplTest {

    @Test
    void testConfig() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            new ApiServerAlarmConfigImpl().config("1", AlarmKeyEnum.API_SERVER_API_DELAY_P95_ALTER);
        });
    }
}