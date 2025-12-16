package io.tapdata.aspect;

import io.tapdata.entity.BatchSizeInfo;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/18 17:03 Create
 * @description
 */
public class BatchSizeAspect extends DataFunctionAspect<BatchSizeAspect> {
    BatchSizeInfo batchSizeInfo;
    String nodeId;

    public BatchSizeAspect(BatchSizeInfo batchSizeInfo) {
        this.batchSizeInfo = batchSizeInfo;
    }

    public BatchSizeInfo getBatchSizeInfo() {
        return batchSizeInfo;
    }

    public void setBatchSizeInfo(BatchSizeInfo batchSizeInfo) {
        this.batchSizeInfo = batchSizeInfo;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
