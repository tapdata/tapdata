package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RealTimeOfApiServerCpuUsageAlterTest {
    @Test
    void check() {
        Assertions.assertDoesNotThrow(() -> {
            RealTimeOfApiServerCpuUsageAlter realTimeOfApiResponseSizeAlter = new RealTimeOfApiServerCpuUsageAlter();
            realTimeOfApiResponseSizeAlter.check(null);
        });
    }

    @Test
    void type() {
        Assertions.assertEquals(AlarmKeyEnum.API_SERVER_CPU_USAGE_ALTER, new RealTimeOfApiServerCpuUsageAlter().type());
    }
}