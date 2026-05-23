package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.SyncStage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SyncStageTracker {
    private static final Logger logger = LogManager.getLogger(SyncStageTracker.class);
    
    private final Map<String, SyncStage> tableStageMap = new ConcurrentHashMap<>();
    private final AtomicBoolean allTablesIncrementalFlag = new AtomicBoolean(false);
    private volatile boolean transitionCompleted = false;
    private Consumer<Void> onAllTablesCdcCallback;
    
    public void setOnAllTablesCdcCallback(Consumer<Void> callback) {
        this.onAllTablesCdcCallback = callback;
    }
    
    // Backward compatibility - old boolean API
    @Deprecated
    public void updateTableStage(String tableName, boolean incremental) {
        SyncStage stage = incremental ? SyncStage.CDC : SyncStage.INITIAL_SYNC;
        updateTableStage(tableName, stage);
    }
    
    public void updateTableStage(String tableName, SyncStage stage) {
        SyncStage previous = tableStageMap.put(tableName, stage);
        if (previous != stage) {
            logger.debug("Table {} sync stage changed: {} -> {}", tableName, previous, stage);
            checkAllIncremental();
        }
    }
    
    public void updateTableStageFromEvent(String tableName, SyncStage eventStage) {
        if (eventStage != null) {
            updateTableStage(tableName, eventStage);
        }
    }
    
    private void checkAllIncremental() {
        if (!tableStageMap.isEmpty() && 
            tableStageMap.values().stream().allMatch(s -> s == SyncStage.CDC)) {
            if (allTablesIncrementalFlag.compareAndSet(false, true)) {
                logger.info("All tables have transitioned to CDC stage! Total tables: {}", 
                    tableStageMap.size());
                transitionCompleted = true;
                
                // Trigger callback if registered
                if (onAllTablesCdcCallback != null) {
                    try {
                        onAllTablesCdcCallback.accept(null);
                    } catch (Exception e) {
                        logger.error("Error in onAllTablesCdcCallback: {}", e.getMessage(), e);
                    }
                }
            }
        }
    }
    
    public boolean allTablesIncremental() {
        return allTablesIncrementalFlag.get();
    }
    
    public boolean isTransitionCompleted() {
        return transitionCompleted;
    }
    
    public boolean isTableInInitialSync(String tableName) {
        SyncStage stage = tableStageMap.get(tableName);
        return stage == null || stage == SyncStage.INITIAL_SYNC;
    }
    
    public boolean isTableInCdc(String tableName) {
        return tableStageMap.get(tableName) == SyncStage.CDC;
    }
    
    public int getTotalTables() {
        return tableStageMap.size();
    }
    
    public void reset() {
        tableStageMap.clear();
        allTablesIncrementalFlag.set(false);
        transitionCompleted = false;
    }
}
