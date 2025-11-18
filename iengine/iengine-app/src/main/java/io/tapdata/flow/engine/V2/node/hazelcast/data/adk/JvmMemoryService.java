package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import org.springframework.stereotype.Component;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

@Component
public class JvmMemoryService {

    public MemoryUsage getHeapUsage() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        return memoryMXBean.getHeapMemoryUsage();
    }

    public MemoryUsage getNonHeapUsage() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        return memoryMXBean.getNonHeapMemoryUsage();
    }
}