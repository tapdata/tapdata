package com.tapdata.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/4 11:29 Create
 */
class TapdataRecoveryEventTest {
    @Nested
    class ConstructorTest {
        @Test
        void testSyncStageNotNull() {
            Assertions.assertNotNull(new TapdataRecoveryEvent().getSyncStage());
            Assertions.assertNotNull(new TapdataRecoveryEvent("id", TapdataRecoveryEvent.RECOVERY_TYPE_BEGIN).getSyncStage());
        }
    }
}
