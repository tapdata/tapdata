package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds isolated DuckDB state for one sourceId:tableId pair.
 */
public class PerSourceContext {

    private final String key;
    private final DuckDbOperator operator;
    private volatile String targetTableName;
    private final List<TapdataEvent> batchBuffer = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean tableInitialized;
    private final AtomicInteger accumulatedRecordCount = new AtomicInteger(0);
    private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
    private final Object commitLock = new Object();
    private int batchSize = 1000;

    public PerSourceContext(String key, DuckDbOperator operator) {
        this.key = key;
        this.operator = operator;
    }

    public String getKey() {
        return key;
    }

    public DuckDbOperator getOperator() {
        return operator;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public List<TapdataEvent> getBatchBuffer() {
        return batchBuffer;
    }

    public boolean isTableInitialized() {
        return tableInitialized;
    }

    public void setTableInitialized(boolean tableInitialized) {
        this.tableInitialized = tableInitialized;
    }

    public AtomicInteger getAccumulatedRecordCount() {
        return accumulatedRecordCount;
    }

    public AtomicLong getLastCommitTime() {
        return lastCommitTime;
    }

    public Object getCommitLock() {
        return commitLock;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void addEvent(TapdataEvent event) {
        batchBuffer.add(event);
        accumulatedRecordCount.incrementAndGet();
    }

    public List<TapdataEvent> drainBuffer() {
        synchronized (batchBuffer) {
            List<TapdataEvent> copy = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            accumulatedRecordCount.set(0);
            lastCommitTime.set(System.currentTimeMillis());
            return copy;
        }
    }
}
