package com.tapdata.tm.monitor.entity;

import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2022/3/15
 * @Description:
 */
@Data
public class Statistics {
    private double initialTime;
    private long inputTotal;
    private long insertedTotal;
    private double cdcTime;
    private long updatedTotal;
    private double initialTotal;
    private double replicateLag;
    private long deletedTotal;
    private long outputTotal;
    private double initialWrite;
}
