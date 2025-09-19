package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RealTimeOfApiServerMemoryUsageWarnTest {
    @Test
    void check() {
        Assertions.assertDoesNotThrow(() -> {
            RealTimeOfApiServerMemoryUsageWarn realTimeOfApiResponseSizeAlter = new RealTimeOfApiServerMemoryUsageWarn();
            realTimeOfApiResponseSizeAlter.check(null);
        });
    }

    @Test
    void type() {
        Assertions.assertEquals(AlarmKeyEnum.API_SERVER_MEMORY_USAGE_WARN, new RealTimeOfApiServerMemoryUsageWarn().type());
    }
}