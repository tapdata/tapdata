package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SyncStageTrackerTest {
    @Test
    void syncStageTrackerDetectsAllTablesIncremental() {
        SyncStageTracker tracker = new SyncStageTracker();
        tracker.updateTableStage("table_a", false);
        tracker.updateTableStage("table_b", true);
        assertFalse(tracker.allTablesIncremental());
        tracker.updateTableStage("table_a", true);
        assertTrue(tracker.allTablesIncremental());
    }
}
