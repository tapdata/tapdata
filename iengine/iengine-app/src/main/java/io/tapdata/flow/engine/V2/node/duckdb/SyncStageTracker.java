package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.SyncStage;
import io.tapdata.observable.logging.ObsLogger;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Setter
public class SyncStageTracker {
    private ObsLogger logger;
    private final Map<String, SyncStage> tableStageMap = new ConcurrentHashMap<>();
    private Consumer<Boolean> onAllTablesCdcCallback;
    private boolean initCdc = false;

    public void updateTableStage(String tableName, SyncStage stage) {
        SyncStage previous = tableStageMap.put(tableName, stage);
        if (null != previous && previous != stage) {
            logger.debug("Table {} sync stage changed: {} -> {}", tableName, previous, stage);
            checkAllIncremental();
        }
    }

    public void updateTableStageFromEvent(String tableName, SyncStage eventStage) {
        if (eventStage == null) {
            return;
        }
        updateTableStage(tableName, eventStage);
    }

    private void checkAllIncremental() {
        if (!initCdc) {
            return;
        }
        if (onAllTablesCdcCallback == null) {
            return;
        }
        try {
            onAllTablesCdcCallback.accept(true);
        } catch (Exception e) {
            logger.error("Error in onAllTablesCdcCallback: {}", e.getMessage(), e);
        }
    }

    public void initCdc() {
        this.initCdc = true;
    }

    public boolean isTableInInitialSync(String tableName) {
        SyncStage stage = tableStageMap.get(tableName);
        return stage == null || stage == SyncStage.INITIAL_SYNC;
    }

    public void reset() {
        tableStageMap.clear();
    }

    public boolean cdcIng() {
        return initCdc;
    }
}
