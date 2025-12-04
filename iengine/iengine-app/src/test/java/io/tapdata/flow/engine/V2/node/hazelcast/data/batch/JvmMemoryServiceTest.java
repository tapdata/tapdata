package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.management.MemoryUsage;

class JvmMemoryServiceTest {

    @Nested
    class GetHeapUsageTest {
        @Test
        void testGetHeapUsage() {
            JvmMemoryService service = new JvmMemoryService();
            MemoryUsage heapUsage = service.getHeapUsage();
            Assertions.assertNotNull(heapUsage);
            Assertions.assertTrue(heapUsage.getMax() > 0);
            Assertions.assertTrue(heapUsage.getUsed() >= 0);
        }
    }

    @Nested
    class GetNonHeapUsageTest {
        @Test
        void testGetNonHeapUsage() {
            JvmMemoryService service = new JvmMemoryService();
            MemoryUsage nonHeapUsage = service.getNonHeapUsage();
            Assertions.assertNotNull(nonHeapUsage);
            Assertions.assertTrue(nonHeapUsage.getUsed() >= 0);
        }
    }
}

