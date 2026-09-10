package io.tapdata.dql.classifier;

/**
 * Batch information available while classifying an Engine failure.
 */
public final class DqlBatchContext {
    private final boolean batchWriteFailed;
    private final int batchSize;
    private final int splitEventCount;
    private final int sameErrorCount;

    public DqlBatchContext(boolean batchWriteFailed,
                           int batchSize,
                           int splitEventCount,
                           int sameErrorCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        if (splitEventCount < 0 || sameErrorCount < 0) {
            throw new IllegalArgumentException("batch counters must not be negative");
        }
        this.batchWriteFailed = batchWriteFailed;
        this.batchSize = batchSize;
        this.splitEventCount = splitEventCount;
        this.sameErrorCount = sameErrorCount;
    }

    public static DqlBatchContext singleRecord() {
        return new DqlBatchContext(false, 1, 1, 1);
    }

    public static DqlBatchContext batchFailure(int batchSize, int sameErrorCount) {
        return new DqlBatchContext(true, batchSize, 0, sameErrorCount);
    }

    public boolean isBatchWriteFailed() {
        return batchWriteFailed;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getSplitEventCount() {
        return splitEventCount;
    }

    public int getSameErrorCount() {
        return sameErrorCount;
    }
}
