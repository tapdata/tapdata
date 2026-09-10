package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DqlTaskImpactVo implements Serializable {
    private String taskId;
    private boolean exists;
    private long count;

    public DqlTaskImpactVo() {
    }

    public DqlTaskImpactVo(String taskId, boolean exists, long count) {
        this.taskId = taskId;
        this.exists = exists;
        this.count = count;
    }
}
