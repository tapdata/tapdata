package io.tapdata.threadgroup;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Optional;

public final class ThreadCPUMonitor {
    private final ThreadMXBean threadMXBean;

    public ThreadCPUMonitor() {
        this.threadMXBean = ManagementFactory.getThreadMXBean();
        threadMXBean.setThreadCpuTimeEnabled(true);
    }

    /**
     * Get the CPU time of the thread (nanoseconds)
     */
    public long getThreadCpuTime(long threadId) {
        return threadMXBean.getThreadCpuTime(threadId);
    }

    /**
     * Get the user mode CPU time (nanoseconds) of the thread
     */
    public long getThreadUserTime(long threadId) {
        return threadMXBean.getThreadUserTime(threadId);
    }

    /**
     * Calculate CPU usage (requires two samplings)
     */
    public double calculateCpuUsage(long threadId, Long previousCpuTime, long timeIntervalMs) {
        long currentCpuTime = getThreadCpuTime(threadId);
        long cpuTimeDiff = currentCpuTime - Optional.ofNullable(previousCpuTime).orElse(0L);
        // Convert nanoseconds to milliseconds and calculate usage rate
        return (cpuTimeDiff / 1_000_000.0D) / timeIntervalMs * 100;
    }
}