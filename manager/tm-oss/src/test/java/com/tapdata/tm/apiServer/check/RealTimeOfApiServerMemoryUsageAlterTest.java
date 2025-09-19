package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RealTimeOfApiServerMemoryUsageAlterTest {
    @Test
    void check() {
        Assertions.assertDoesNotThrow(() -> {
            RealTimeOfApiServerMemoryUsageAlter realTimeOfApiResponseSizeAlter = new RealTimeOfApiServerMemoryUsageAlter();
            realTimeOfApiResponseSizeAlter.check(null);
        });
    }

    @Test
    void type() {
        Assertions.assertEquals(AlarmKeyEnum.API_SERVER_MEMORY_USAGE_ALTER, new RealTimeOfApiServerMemoryUsageAlter().type());
    }
}