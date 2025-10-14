package io.tapdata.entity;

import java.util.Optional;

public class Usage {
    Double cpuUsage = 0D;
    Long heapMemoryUsage = 0L;

    public void addCpu(Double cpuUsage) {
        this.cpuUsage += Optional.ofNullable(cpuUsage).orElse(0D);
    }

    public Double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(Double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public Long getHeapMemoryUsage() {
        return heapMemoryUsage;
    }

    public void setHeapMemoryUsage(Long heapMemoryUsage) {
        this.heapMemoryUsage = heapMemoryUsage;
    }
}