package io.tapdata.dql.classifier;

import java.util.Objects;

/**
 * Capture-point data needed by Storm Guard without coupling it to Engine nodes.
 */
public final class DqlStormGuardContext {
    private final String taskId;
    private final String failedNodeId;
    private final String tableId;
    private final String errorCode;
    private final String errorMessage;
    private final DqlBatchContext batchContext;
    private final boolean singleEventLocatable;

    public DqlStormGuardContext(String taskId,
                                String failedNodeId,
                                String tableId,
                                String errorCode,
                                String errorMessage,
                                DqlBatchContext batchContext,
                                boolean singleEventLocatable) {
        this.taskId = taskId;
        this.failedNodeId = failedNodeId;
        this.tableId = tableId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.batchContext = Objects.requireNonNull(batchContext, "batchContext must not be null");
        this.singleEventLocatable = singleEventLocatable;
    }

    public static DqlStormGuardContext singleRecord(String taskId,
                                                    String failedNodeId,
                                                    String tableId,
                                                    String errorCode,
                                                    String errorMessage) {
        return new DqlStormGuardContext(taskId, failedNodeId, tableId, errorCode,
                errorMessage, DqlBatchContext.singleRecord(), true);
    }

    public DqlStormGuardKey getGuardKey() {
        return DqlStormGuardKey.of(taskId, failedNodeId, tableId, errorCode, errorMessage);
    }

    public DqlBatchContext getBatchContext() {
        return batchContext;
    }

    public boolean isSingleEventLocatable() {
        return singleEventLocatable;
    }

    /**
     * Returns -1 when this context does not represent a multi-record batch.
     */
    public double getBatchRatio() {
        if (!batchContext.isBatchWriteFailed() || batchContext.getBatchSize() <= 1) {
            return -1d;
        }
        double ratio = (double) batchContext.getSameErrorCount() / batchContext.getBatchSize();
        return Math.min(1d, Math.max(0d, ratio));
    }
}
