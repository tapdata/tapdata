package com.tapdata.tm.worker.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;

class ServerUsageTest {

    @Nested
    class ProcessTypeTest {
        @org.junit.jupiter.api.Test
        void testAs() {
            ServerUsage.ProcessType type = ServerUsage.ProcessType.as(0);
            Assertions.assertEquals(ServerUsage.ProcessType.TM, type);
            type = ServerUsage.ProcessType.as(1);
            Assertions.assertEquals(ServerUsage.ProcessType.ENGINE, type);
            type = ServerUsage.ProcessType.as(2);
            Assertions.assertEquals(ServerUsage.ProcessType.API_SERVER, type);
            type = ServerUsage.ProcessType.as(3);
            Assertions.assertEquals(ServerUsage.ProcessType.API_SERVER_WORKER, type);
            type = ServerUsage.ProcessType.as(4);
            Assertions.assertEquals(ServerUsage.ProcessType.TM, type);
        }
    }

    @Nested
    class InstanceTest {
        @org.junit.jupiter.api.Test
        void testInstance() {
            ServerUsage metric = ServerUsage.instance(1, "1", "1", 0);
            Assertions.assertEquals(1, metric.getLastUpdateTime());
            Assertions.assertEquals("1", metric.getProcessId());
            Assertions.assertEquals("1", metric.getWorkOid());
            Assertions.assertEquals(0, metric.getProcessType());
        }
    }
}