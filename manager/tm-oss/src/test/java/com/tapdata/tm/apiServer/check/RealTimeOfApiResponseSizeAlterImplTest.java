package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RealTimeOfApiResponseSizeAlterImplTest {

    @Test
    void check() {
        Assertions.assertDoesNotThrow(() -> {
            RealTimeOfApiResponseSizeAlterImpl realTimeOfApiResponseSizeAlter = new RealTimeOfApiResponseSizeAlterImpl();
            realTimeOfApiResponseSizeAlter.check("1", null);
        });
    }

    @Test
    void type() {
        Assertions.assertEquals(AlarmKeyEnum.API_SERVER_API_RESPONSE_SIZE_ALTER, new RealTimeOfApiResponseSizeAlterImpl().type());
    }
}