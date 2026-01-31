package com.tapdata.tm.v2.api.monitor.main.entity;

import lombok.Data;

@Data
public class WorkerInfo {
    String workerOid;
    Long reqCount;
    Long errorCount;

    public WorkerInfo() {
        this.reqCount = 0L;
        this.errorCount = 0L;
    }
}