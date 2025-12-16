package io.tapdata.entity;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/18 17:03 Create
 * @description
 */

public class BatchSizeInfo {
    Integer batchSize;
    Long targetIntervalMs;

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getTargetIntervalMs() {
        return targetIntervalMs;
    }

    public void setTargetIntervalMs(Long targetIntervalMs) {
        this.targetIntervalMs = targetIntervalMs;
    }
}
